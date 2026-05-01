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

use daisy_core::pipeline::Pipeline as CorePipeline;
use daisy_core::task::Task as CoreTask;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;

use crate::py_task::PyTask;

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
    /// Convert to a `daisy_core::Pipeline`. Each task is converted
    /// once and cached by task_id; edges become (upstream_task_id,
    /// downstream_task_id) pairs. The returned core Pipeline is what
    /// `Scheduler::new`, `Server::run_blockwise`, and
    /// `SerialRunner::run` consume.
    pub fn to_core(&self, py: Python<'_>) -> PyResult<CorePipeline> {
        let mut cache: HashMap<String, Arc<CoreTask>> = HashMap::new();
        let mut tasks: Vec<Arc<CoreTask>> = Vec::with_capacity(self.tasks.len());
        let mut task_ids: Vec<String> = Vec::with_capacity(self.tasks.len());
        for t in &self.tasks {
            let bound = t.bind(py).cast::<PyTask>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "pipeline contains a non-Task object",
                )
            })?;
            let arc = PyTask::convert_task(&bound, py, &mut cache)?;
            task_ids.push(arc.task_id.clone());
            tasks.push(arc);
        }
        let edges: Vec<(String, String)> = self
            .edges
            .iter()
            .map(|&(u, d)| (task_ids[u].clone(), task_ids[d].clone()))
            .collect();
        CorePipeline::new(tasks, edges)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))
    }

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
    /// Construct from a flat task list and edge list. Each edge is a
    /// `(upstream_task, downstream_task)` pair where both endpoints
    /// must be in `tasks`. Validates that every edge endpoint is
    /// listed and that task ids are unique. Sources and outputs are
    /// derived from the edge structure (tasks with no incoming /
    /// outgoing edges respectively).
    #[new]
    #[pyo3(signature = (tasks=None, edges=None))]
    fn new(
        py: Python<'_>,
        tasks: Option<Bound<'_, PyList>>,
        edges: Option<Bound<'_, PyList>>,
    ) -> PyResult<Self> {
        let tasks_vec: Vec<Py<PyAny>> = match tasks {
            None => Vec::new(),
            Some(list) => {
                let mut v = Vec::new();
                for item in list.iter() {
                    v.push(item.unbind());
                }
                v
            }
        };
        let task_index: std::collections::HashMap<*mut pyo3::ffi::PyObject, usize> =
            tasks_vec
                .iter()
                .enumerate()
                .map(|(i, t)| (t.as_ptr(), i))
                .collect();
        if task_index.len() != tasks_vec.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Pipeline tasks list contains duplicates (by Python identity)",
            ));
        }
        let edges_vec: Vec<(usize, usize)> = match edges {
            None => Vec::new(),
            Some(list) => {
                let mut v = Vec::new();
                for item in list.iter() {
                    let pair: (Py<PyAny>, Py<PyAny>) = item.extract()?;
                    let up_idx = *task_index.get(&pair.0.as_ptr()).ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "edge upstream task is not in the pipeline's task list",
                        )
                    })?;
                    let down_idx = *task_index.get(&pair.1.as_ptr()).ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "edge downstream task is not in the pipeline's task list",
                        )
                    })?;
                    v.push((up_idx, down_idx));
                }
                v
            }
        };
        let with_incoming: std::collections::HashSet<usize> =
            edges_vec.iter().map(|&(_, d)| d).collect();
        let with_outgoing: std::collections::HashSet<usize> =
            edges_vec.iter().map(|&(u, _)| u).collect();
        let sources: Vec<usize> = (0..tasks_vec.len())
            .filter(|i| !with_incoming.contains(i))
            .collect();
        let outputs: Vec<usize> = (0..tasks_vec.len())
            .filter(|i| !with_outgoing.contains(i))
            .collect();
        let _ = py;
        Ok(Self {
            tasks: tasks_vec,
            edges: edges_vec,
            sources,
            outputs,
        })
    }

    /// Promote a single task to a singleton pipeline (one source,
    /// one output, no edges).
    #[staticmethod]
    pub fn from_task(task: Py<PyAny>) -> Self {
        Self {
            tasks: vec![task],
            edges: vec![],
            sources: vec![0],
            outputs: vec![0],
        }
    }

    /// `a + b` — sequential composition. Every output of `a`
    /// becomes a block-level upstream of every source of `b`.
    pub fn __add__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<PyPipeline> {
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
    pub fn __or__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<PyPipeline> {
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


    /// Shortcut: pass `self` to `daisy.run_blockwise`. `**kwargs` are
    /// forwarded.
    #[pyo3(signature = (**kwargs))]
    fn run_blockwise(
        slf: PyRef<'_, Self>,
        py: Python<'_>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let runner = py.import("daisy._runner")?;
        let self_obj: Py<PyAny> = slf.into_pyobject(py)?.into_any().unbind();
        let result = runner.call_method("run_blockwise", (self_obj,), kwargs)?;
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
