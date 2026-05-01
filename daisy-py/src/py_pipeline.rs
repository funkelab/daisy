//! Pipeline DSL: compose tasks into DAGs with `+` and `|`.
//!
//! Composition operators and the DAG data structure live in Rust.
//! Each `Pipeline` holds Python `Task` references (`Py<PyAny>`)
//! plus an edge list and source/output index lists. `+` builds
//! cross-product edges from outputs of the left side to sources of
//! the right; `|` is a union without new edges. Composition is
//! fully Rust-side; `materialize()` calls back into Python only
//! to invoke `copy.copy(task)` and `setattr` on the clones, which
//! is the minimum needed to produce a fresh task tree without
//! mutating the originals.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// A DAG of tasks. Tasks are stored as deduplicated `Py<PyAny>`
/// references (Python identity); edges are index pairs into that
/// list, so the entire structure is small and cheap to clone /
/// compose.
#[pyclass(name = "Pipeline", skip_from_py_object)]
pub struct PyPipeline {
    pub tasks: Vec<Py<PyAny>>,
    pub edges: Vec<(usize, usize)>,
    pub sources: Vec<usize>,
    pub outputs: Vec<usize>,
}

impl PyPipeline {
    /// Deep-clone (Py<PyAny> needs the GIL to bump its refcount, so
    /// we can't `#[derive(Clone)]`).
    fn deep_clone(&self, py: Python<'_>) -> Self {
        Self {
            tasks: self.tasks.iter().map(|t| t.clone_ref(py)).collect(),
            edges: self.edges.clone(),
            sources: self.sources.clone(),
            outputs: self.outputs.clone(),
        }
    }
}

#[pymethods]
impl PyPipeline {
    /// Promote a single task to a singleton pipeline (one source,
    /// one output, no edges).
    #[staticmethod]
    fn from_task(task: Py<PyAny>) -> Self {
        Self {
            tasks: vec![task],
            edges: vec![],
            sources: vec![0],
            outputs: vec![0],
        }
    }

    /// `a + b` — sequential composition. Every output of `a`
    /// becomes a block-level upstream of every source of `b`.
    fn __add__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<PyPipeline> {
        let right = coerce_to_pipeline(py, other)?;
        let (tasks, left_idx, right_idx) = merge_tasks(py, &self.tasks, &right.tasks);
        let mut edges = remap(&self.edges, &left_idx);
        edges.extend(remap(&right.edges, &right_idx));
        for &out in &self.outputs {
            for &src in &right.sources {
                edges.push((left_idx[out], right_idx[src]));
            }
        }
        Ok(Self {
            tasks,
            edges,
            sources: self.sources.iter().map(|&i| left_idx[i]).collect(),
            outputs: right.outputs.iter().map(|&i| right_idx[i]).collect(),
        })
    }

    /// `a | b` — parallel composition. Union of the two DAGs with
    /// no new edges; sources and outputs are unioned.
    fn __or__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<PyPipeline> {
        let right = coerce_to_pipeline(py, other)?;
        let (tasks, left_idx, right_idx) = merge_tasks(py, &self.tasks, &right.tasks);
        let mut edges = remap(&self.edges, &left_idx);
        edges.extend(remap(&right.edges, &right_idx));

        let mut sources: Vec<usize> = self.sources.iter().map(|&i| left_idx[i]).collect();
        for &i in &right.sources {
            let mapped = right_idx[i];
            if !sources.contains(&mapped) {
                sources.push(mapped);
            }
        }
        let mut outputs: Vec<usize> = self.outputs.iter().map(|&i| left_idx[i]).collect();
        for &i in &right.outputs {
            let mapped = right_idx[i];
            if !outputs.contains(&mapped) {
                outputs.push(mapped);
            }
        }
        Ok(Self {
            tasks,
            edges,
            sources,
            outputs,
        })
    }

    /// Symmetric implementations so `task + pipeline` / `task | pipeline`
    /// work when Python tries the right operand's reflected method.
    fn __radd__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<PyPipeline> {
        let left = coerce_to_pipeline(py, other)?;
        let self_obj = Py::new(py, self.deep_clone(py))?;
        left.__add__(py, self_obj.bind(py).as_any())
    }

    fn __ror__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<PyPipeline> {
        let left = coerce_to_pipeline(py, other)?;
        let self_obj = Py::new(py, self.deep_clone(py))?;
        left.__or__(py, self_obj.bind(py).as_any())
    }

    /// Clear every member task's done-marker. No cascade — see
    /// `Task.reset()` for the per-task semantics.
    fn reset(&self, py: Python<'_>) -> PyResult<()> {
        for t in &self.tasks {
            t.call_method0(py, "reset")?;
        }
        Ok(())
    }

    /// Return a fresh tree of task clones whose `upstream_tasks`
    /// are set from this pipeline's edges (merged with each task's
    /// own constructor-set `upstream_tasks`, deduplicated by
    /// Python object identity). The output tasks are returned —
    /// passing them to `daisy.run_blockwise` walks the upstream
    /// chain to schedule the full DAG.
    fn materialize(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let copy_module = py.import("copy")?;
        let copy_fn = copy_module.getattr("copy")?;

        // edges: for each downstream index, the list of upstream indices.
        let mut from_edges: Vec<Vec<usize>> = vec![Vec::new(); self.tasks.len()];
        for &(up, down) in &self.edges {
            from_edges[down].push(up);
        }

        // Clone each task once.
        let mut clones: Vec<Py<PyAny>> = Vec::with_capacity(self.tasks.len());
        for t in &self.tasks {
            let cloned = copy_fn.call1((t,))?;
            clones.push(cloned.unbind());
        }

        // For each clone, merge its existing upstream_tasks attribute
        // with edge-derived upstream clones, deduplicate by Python
        // object identity, and assign back.
        for (idx, clone) in clones.iter().enumerate() {
            let existing_obj = self.tasks[idx].getattr(py, "upstream_tasks")?;
            let existing: Vec<Py<PyAny>> = existing_obj.extract(py)?;
            let new_upstream = PyList::empty(py);
            // Track identity by raw pointer.
            let mut seen: Vec<*mut pyo3::ffi::PyObject> = Vec::new();
            for u in &existing {
                let ptr = u.as_ptr();
                if !seen.contains(&ptr) {
                    seen.push(ptr);
                    new_upstream.append(u)?;
                }
            }
            for &up_idx in &from_edges[idx] {
                let up_clone = &clones[up_idx];
                let ptr = up_clone.as_ptr();
                if !seen.contains(&ptr) {
                    seen.push(ptr);
                    new_upstream.append(up_clone)?;
                }
            }
            clone.setattr(py, "upstream_tasks", new_upstream)?;
        }

        let out = PyList::empty(py);
        for &i in &self.outputs {
            out.append(&clones[i])?;
        }
        Ok(out.unbind())
    }

    /// Shortcut: materialize the pipeline and pass to
    /// `daisy.run_blockwise`. `**kwargs` are forwarded.
    #[pyo3(signature = (**kwargs))]
    fn run_blockwise(
        &self,
        py: Python<'_>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let materialized = self.materialize(py)?;
        let runner = py.import("daisy._runner")?;
        let result = runner.call_method("run_blockwise", (materialized,), kwargs)?;
        result.extract()
    }

    #[getter]
    fn tasks(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for t in &self.tasks {
            list.append(t)?;
        }
        Ok(list.unbind())
    }

    #[getter]
    fn sources(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for &i in &self.sources {
            list.append(&self.tasks[i])?;
        }
        Ok(list.unbind())
    }

    #[getter]
    fn outputs(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for &i in &self.outputs {
            list.append(&self.tasks[i])?;
        }
        Ok(list.unbind())
    }

    #[getter]
    fn edges(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for &(u, d) in &self.edges {
            let pair = pyo3::types::PyTuple::new(py, &[&self.tasks[u], &self.tasks[d]])?;
            list.append(pair)?;
        }
        Ok(list.unbind())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let task_ids: Vec<String> = self
            .tasks
            .iter()
            .map(|t| {
                t.getattr(py, "task_id")
                    .and_then(|a| a.extract::<String>(py))
                    .unwrap_or_else(|_| "<?>".into())
            })
            .collect();
        let source_ids: Vec<&String> = self.sources.iter().map(|&i| &task_ids[i]).collect();
        let output_ids: Vec<&String> = self.outputs.iter().map(|&i| &task_ids[i]).collect();
        let edges: Vec<(&String, &String)> = self
            .edges
            .iter()
            .map(|&(u, d)| (&task_ids[u], &task_ids[d]))
            .collect();
        Ok(format!(
            "Pipeline(tasks={task_ids:?}, sources={source_ids:?}, outputs={output_ids:?}, edges={edges:?})"
        ))
    }
}

fn coerce_to_pipeline(py: Python<'_>, x: &Bound<'_, PyAny>) -> PyResult<PyPipeline> {
    if let Ok(p) = x.downcast::<PyPipeline>() {
        return Ok(p.borrow().deep_clone(py));
    }
    // Anything else — assume it's a Task (Python wrapper or _rs.Task).
    // We don't strictly type-check here because users may have their
    // own Task-like classes; the materialize step will fail loudly if
    // upstream_tasks is missing. Reject obvious non-objects up front
    // to give a useful error:
    let type_name: String = x.get_type().name()?.extract()?;
    if matches!(type_name.as_str(), "int" | "float" | "str" | "bool" | "list" | "dict" | "tuple" | "NoneType") {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "cannot compose Pipeline with {type_name}; expected Task or Pipeline"
        )));
    }
    Ok(PyPipeline::from_task(x.clone().unbind()))
}

/// Merge `right` into `left` deduplicating by Python identity. Returns
/// the unified task list and two index-translation tables: `left_idx`
/// maps each old left index to its position in the merged list,
/// `right_idx` does the same for right.
fn merge_tasks(
    py: Python<'_>,
    left: &[Py<PyAny>],
    right: &[Py<PyAny>],
) -> (Vec<Py<PyAny>>, Vec<usize>, Vec<usize>) {
    let mut tasks: Vec<Py<PyAny>> = left.iter().map(|t| t.clone_ref(py)).collect();
    let left_idx: Vec<usize> = (0..left.len()).collect();
    let mut right_idx = Vec::with_capacity(right.len());
    for r in right {
        let r_ptr = r.as_ptr();
        let pos = tasks.iter().position(|t| t.as_ptr() == r_ptr);
        match pos {
            Some(i) => right_idx.push(i),
            None => {
                right_idx.push(tasks.len());
                tasks.push(r.clone_ref(py));
            }
        }
    }
    (tasks, left_idx, right_idx)
}

fn remap(edges: &[(usize, usize)], idx_map: &[usize]) -> Vec<(usize, usize)> {
    edges.iter().map(|&(a, b)| (idx_map[a], idx_map[b])).collect()
}
