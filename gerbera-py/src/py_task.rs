use gerbera_core::task::{Fit, Task};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;

use crate::py_callbacks::{PyCheckBlock, PyProcessBlock, PySpawnWorker};
use crate::py_roi::PyRoi;

#[pyclass(name = "Task", skip_from_py_object)]
pub struct PyTask {
    pub task_id: String,
    pub total_roi: PyRoi,
    pub read_roi: PyRoi,
    pub write_roi: PyRoi,
    pub read_write_conflict: bool,
    pub fit: String,
    pub num_workers: usize,
    pub max_retries: u32,
    pub check_function: Option<Py<PyAny>>,
    pub process_function: Option<Py<PyAny>>,
    /// Stored as Py references so they survive without Clone.
    pub upstream_tasks: Vec<Py<PyTask>>,
    /// Path to the persistent done-marker directory; `None` disables.
    pub done_marker_path: Option<String>,
    /// Per-worker resource cost; empty disables resource accounting.
    pub requires: HashMap<String, i64>,
}

#[pymethods]
impl PyTask {
    #[new]
    #[pyo3(signature = (
        task_id,
        total_roi,
        read_roi,
        write_roi,
        process_function=None,
        check_function=None,
        read_write_conflict=true,
        fit="valid".to_string(),
        num_workers=1,
        max_retries=2,
        upstream_tasks=None,
        done_marker_path=None,
        requires=None,
    ))]
    fn new(
        task_id: String,
        total_roi: PyRoi,
        read_roi: PyRoi,
        write_roi: PyRoi,
        process_function: Option<Py<PyAny>>,
        check_function: Option<Py<PyAny>>,
        read_write_conflict: bool,
        fit: String,
        num_workers: usize,
        max_retries: u32,
        upstream_tasks: Option<Bound<'_, PyList>>,
        done_marker_path: Option<String>,
        requires: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let ups = if let Some(list) = upstream_tasks {
            let mut v = Vec::new();
            for item in list.iter() {
                let bound: Bound<'_, PyTask> = item.cast()?.clone();
                v.push(bound.unbind());
            }
            v
        } else {
            Vec::new()
        };
        let reqs: HashMap<String, i64> = if let Some(d) = requires {
            let mut m = HashMap::new();
            for (k, v) in d.iter() {
                let key: String = k.extract()?;
                let val: i64 = v.extract()?;
                m.insert(key, val);
            }
            m
        } else {
            HashMap::new()
        };
        Ok(Self {
            task_id,
            total_roi,
            read_roi,
            write_roi,
            read_write_conflict,
            fit,
            num_workers,
            max_retries,
            check_function,
            process_function,
            upstream_tasks: ups,
            done_marker_path,
            requires: reqs,
        })
    }

    #[getter]
    fn task_id(&self) -> &str {
        &self.task_id
    }

    #[getter]
    fn total_roi(&self) -> PyRoi {
        self.total_roi.clone()
    }

    #[getter]
    fn read_roi(&self) -> PyRoi {
        self.read_roi.clone()
    }

    #[getter]
    fn write_roi(&self) -> PyRoi {
        self.write_roi.clone()
    }

    #[getter]
    fn fit(&self) -> &str {
        &self.fit
    }

    #[getter]
    fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

impl PyTask {
    pub fn empty() -> Self {
        use gerbera_core::coordinate::Coordinate;
        use gerbera_core::roi::Roi;
        let zero_roi = PyRoi {
            inner: Roi::new(Coordinate::new(vec![0]), Coordinate::new(vec![1])),
        };
        Self {
            task_id: String::new(),
            total_roi: zero_roi.clone(),
            read_roi: zero_roi.clone(),
            write_roi: zero_roi,
            read_write_conflict: false,
            fit: "valid".into(),
            num_workers: 0,
            max_retries: 0,
            check_function: None,
            process_function: None,
            upstream_tasks: Vec::new(),
            done_marker_path: None,
            requires: HashMap::new(),
        }
    }

    /// Recursively convert to core `Task`, using a cache to handle shared
    /// upstream references.
    pub fn convert_task_tree(
        bound: &Bound<'_, PyTask>,
        py: Python<'_>,
        cache: &mut HashMap<String, Arc<Task>>,
    ) -> PyResult<Arc<Task>> {
        let task_id = bound.borrow().task_id.clone();
        if let Some(cached) = cache.get(&task_id) {
            return Ok(cached.clone());
        }

        // Recursively convert upstream tasks first.
        let upstream_refs: Vec<Py<PyTask>> = {
            let borrow = bound.borrow();
            borrow.upstream_tasks.iter().map(|r| r.clone_ref(py)).collect()
        };
        let mut upstream_arc = Vec::new();
        for up_ref in &upstream_refs {
            let up_bound: &Bound<'_, PyTask> = up_ref.bind(py);
            let up_arc = Self::convert_task_tree(up_bound, py, cache)?;
            upstream_arc.push(up_arc);
        }

        let borrow = bound.borrow();
        let mut builder = Task::builder(&borrow.task_id)
            .total_roi(borrow.total_roi.inner.clone())
            .read_roi(borrow.read_roi.inner.clone())
            .write_roi(borrow.write_roi.inner.clone())
            .read_write_conflict(borrow.read_write_conflict)
            .fit(Fit::from_str(&borrow.fit))
            .num_workers(borrow.num_workers)
            .max_retries(borrow.max_retries);

        if let Some(ref path) = borrow.done_marker_path {
            builder = builder.done_marker_path(path);
        }
        if !borrow.requires.is_empty() {
            builder = builder.requires(borrow.requires.clone());
        }

        for up in upstream_arc {
            builder = builder.upstream(up);
        }

        if let Some(ref check_fn) = borrow.check_function {
            builder = builder.check_function(PyCheckBlock::new(check_fn.clone_ref(py)));
        }
        if let Some(ref process_fn) = borrow.process_function {
            // Detect arity: 0 args → spawn function, 1 arg → block processor.
            // Matches daisy's Task.__init__ logic.
            let inspect = py.import("inspect")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}")))?;
            let argspec = inspect.call_method1("getfullargspec", (process_fn.clone_ref(py),))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}")))?;
            let args: Vec<String> = argspec.getattr("args")
                .and_then(|a| a.extract())
                .unwrap_or_default();
            let nargs = args.iter().filter(|a| a.as_str() != "self").count();

            if nargs == 0 {
                builder = builder.spawn_function(PySpawnWorker::new(process_fn.clone_ref(py)));
            } else {
                builder = builder.process_function(PyProcessBlock::new(process_fn.clone_ref(py)));
            }
        }

        let arc = Arc::new(builder.build());
        cache.insert(task_id, arc.clone());
        Ok(arc)
    }
}
