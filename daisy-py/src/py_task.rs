use daisy_core::done_marker::DoneMarker;
use daisy_core::task::{Fit, Task};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::py_callbacks::{PyCheckBlock, PyProcessBlock, PySpawnWorker};
use crate::py_roi::PyRoi;

/// Three-state done-marker spec: `Auto` resolves at convert time
/// against the global basedir (or `daisy.logging.LOG_BASEDIR` as
/// final fallback); `Disabled` opts out; `Path` is verbatim.
#[derive(Clone, Debug)]
pub enum DoneMarkerSpec {
    Auto,
    Disabled,
    Path(String),
}

impl DoneMarkerSpec {
    /// Resolve to a concrete path string. Precedence:
    ///   1. `Disabled`           → None
    ///   2. `Path(s)`            → Some(s)
    ///   3. `Auto` + global Rust basedir set → `<basedir>/<task_id>`
    ///   4. `Auto` + Python `daisy.logging.LOG_BASEDIR` set → `<log_basedir>/<task_id>`
    ///   5. otherwise            → None
    pub fn resolve(&self, py: Python<'_>, task_id: &str) -> Option<String> {
        match self {
            DoneMarkerSpec::Disabled => None,
            DoneMarkerSpec::Path(s) => Some(s.clone()),
            DoneMarkerSpec::Auto => {
                if let Some(b) = get_basedir() {
                    return Some(b.join(task_id).to_string_lossy().into_owned());
                }
                // Python LOG_BASEDIR fallback. Best-effort: any error
                // (module not importable, attribute missing, value is
                // None) just yields no marker.
                let py_basedir: Option<String> = py
                    .import("daisy.logging")
                    .ok()
                    .and_then(|m| m.getattr("LOG_BASEDIR").ok())
                    .and_then(|v| if v.is_none() { None } else { v.str().ok() })
                    .and_then(|s| s.extract::<String>().ok());
                py_basedir.map(|b| {
                    PathBuf::from(b)
                        .join(task_id)
                        .to_string_lossy()
                        .into_owned()
                })
            }
        }
    }
}

/// Module-level done-marker basedir. `set_done_marker_basedir(path)`
/// sets it; `get_done_marker_basedir()` reads it. Tasks with
/// `done_marker_path=None` (Auto) resolve through this.
static BASEDIR: RwLock<Option<PathBuf>> = RwLock::new(None);

fn get_basedir() -> Option<PathBuf> {
    BASEDIR.read().unwrap().clone()
}

#[pyfunction]
pub fn set_done_marker_basedir(path: Option<String>) -> PyResult<()> {
    let mut g = BASEDIR.write().unwrap();
    *g = path.map(PathBuf::from);
    Ok(())
}

#[pyfunction]
pub fn get_done_marker_basedir(py: Python<'_>) -> PyResult<Py<PyAny>> {
    match get_basedir() {
        Some(p) => {
            let pathlib = py.import("pathlib")?;
            let path_class = pathlib.getattr("Path")?;
            let s = p.to_string_lossy().into_owned();
            Ok(path_class.call1((s,))?.unbind())
        }
        None => Ok(py.None()),
    }
}

/// Coerce a Python None / False / str into a `DoneMarkerSpec`.
/// Anything else is a TypeError.
fn parse_done_marker_spec(v: Option<&Bound<'_, PyAny>>) -> PyResult<DoneMarkerSpec> {
    let Some(v) = v else { return Ok(DoneMarkerSpec::Auto) };
    if v.is_none() {
        return Ok(DoneMarkerSpec::Auto);
    }
    if let Ok(b) = v.extract::<bool>() {
        if !b {
            return Ok(DoneMarkerSpec::Disabled);
        }
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "done_marker_path=True is not a valid value; use None (auto), \
             False (disabled), or a str/Path",
        ));
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(DoneMarkerSpec::Path(s));
    }
    // Accept pathlib.Path by str()-coercing.
    if let Ok(s) = v.str().and_then(|x| x.extract::<String>()) {
        return Ok(DoneMarkerSpec::Path(s));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "done_marker_path must be None, False, or a str/Path",
    ))
}

#[pyclass(name = "Task", skip_from_py_object, subclass)]
pub struct PyTask {
    pub task_id: String,
    pub total_roi: PyRoi,
    pub read_roi: PyRoi,
    pub write_roi: PyRoi,
    pub read_write_conflict: bool,
    pub fit: String,
    pub max_workers: usize,
    pub max_retries: u32,
    pub check_function: Option<Py<PyAny>>,
    pub process_function: Option<Py<PyAny>>,
    /// Three-state done-marker spec (Auto / Disabled / explicit path).
    /// Resolution against the global basedir / `LOG_BASEDIR` happens
    /// in `convert_task_tree`, not at construction, so the same
    /// `Task` instance picks up basedir changes between runs.
    pub done_marker_path: DoneMarkerSpec,
    /// Per-worker resource cost; empty disables resource accounting.
    pub requires: HashMap<String, i64>,
    /// Cap on worker restarts before the task is abandoned. Default 10.
    pub max_worker_restarts: u32,
    /// Per-block processing timeout in seconds. `None` (default) means
    /// no timeout — blocks can sit in `processing` indefinitely as
    /// long as the worker stays connected. When set, the bookkeeper
    /// reclaims any block that's been processing longer than this and
    /// re-queues it for retry (or orphans, depending on retry budget).
    pub timeout_secs: Option<f64>,
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
        max_workers=1,
        max_retries=2,
        timeout=None,
        done_marker_path=None,
        requires=None,
        max_worker_restarts=10,
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
        max_workers: usize,
        max_retries: u32,
        timeout: Option<&Bound<'_, PyAny>>,
        done_marker_path: Option<&Bound<'_, PyAny>>,
        requires: Option<Bound<'_, PyDict>>,
        max_worker_restarts: u32,
    ) -> PyResult<Self> {
        let done_marker_path = parse_done_marker_spec(done_marker_path)?;
        let timeout_secs: Option<f64> = match timeout {
            None => None,
            Some(v) if v.is_none() => None,
            Some(v) => {
                if let Ok(secs) = v.call_method0("total_seconds")
                    .and_then(|x| x.extract::<f64>())
                {
                    Some(secs)
                } else if let Ok(secs) = v.extract::<f64>() {
                    Some(secs)
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "timeout must be a float seconds, a timedelta, or None",
                    ));
                }
            }
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
            max_workers,
            max_retries,
            check_function,
            process_function,
            done_marker_path,
            requires: reqs,
            max_worker_restarts,
            timeout_secs,
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

    #[getter]
    fn process_function(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.process_function.as_ref().map(|f| f.clone_ref(py))
    }

    /// Settable so `_wrap_for_worker_logging` can replace the
    /// process_function with a logging-wrapped version on a clone.
    #[setter(process_function)]
    fn set_process_function(&mut self, value: Option<Py<PyAny>>) {
        self.process_function = value;
    }

    #[getter]
    fn check_function(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.check_function.as_ref().map(|f| f.clone_ref(py))
    }

    #[getter]
    fn read_write_conflict(&self) -> bool {
        self.read_write_conflict
    }

    #[getter]
    fn max_workers(&self) -> usize {
        self.max_workers
    }

    #[getter]
    fn max_worker_restarts(&self) -> u32 {
        self.max_worker_restarts
    }

    #[getter]
    fn timeout_secs(&self) -> Option<f64> {
        self.timeout_secs
    }

    #[getter]
    fn requires(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.requires {
            dict.set_item(k, v)?;
        }
        Ok(dict.unbind())
    }

    /// Shallow copy: same callables (Py<PyAny> ref-counted). Mirrors
    /// `copy.copy(task)` so callers (e.g. `_wrap_for_worker_logging`)
    /// can mutate `process_function` on the clone without affecting
    /// the original.
    fn __copy__(&self, py: Python<'_>) -> Self {
        Self {
            task_id: self.task_id.clone(),
            total_roi: self.total_roi.clone(),
            read_roi: self.read_roi.clone(),
            write_roi: self.write_roi.clone(),
            read_write_conflict: self.read_write_conflict,
            fit: self.fit.clone(),
            max_workers: self.max_workers,
            max_retries: self.max_retries,
            check_function: self.check_function.as_ref().map(|f| f.clone_ref(py)),
            process_function: self.process_function.as_ref().map(|f| f.clone_ref(py)),
            done_marker_path: self.done_marker_path.clone(),
            requires: self.requires.clone(),
            max_worker_restarts: self.max_worker_restarts,
            timeout_secs: self.timeout_secs,
        }
    }

    fn __deepcopy__(&self, py: Python<'_>, _memo: &Bound<'_, PyAny>) -> Self {
        // copy.deepcopy hits the same path — the user's callables
        // shouldn't be deep-cloned anyway (they're Python functions).
        self.__copy__(py)
    }

    /// `a + b` — sequential pipeline composition. `b`'s blocks depend
    /// on `a`'s outputs.
    fn __add__(slf: PyRef<'_, Self>, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<crate::py_pipeline::PyPipeline> {
        let self_obj: Py<PyAny> = slf.into_pyobject(py)?.into_any().unbind();
        let pipeline = crate::py_pipeline::PyPipeline::from_task(self_obj);
        pipeline.__add__(py, other)
    }

    /// `a | b` — parallel pipeline composition. Union of the two
    /// DAGs, no new edges.
    fn __or__(slf: PyRef<'_, Self>, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<crate::py_pipeline::PyPipeline> {
        let self_obj: Py<PyAny> = slf.into_pyobject(py)?.into_any().unbind();
        let pipeline = crate::py_pipeline::PyPipeline::from_task(self_obj);
        pipeline.__or__(py, other)
    }

    /// Delete this task's done-marker so the next run starts fresh.
    /// Returns the path that was cleared, or None if no marker was
    /// configured / nothing existed at the resolved path.
    fn reset(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let Some(p) = self.done_marker_path.resolve(py, &self.task_id) else {
            return Ok(None);
        };
        let path = Path::new(&p);
        if !path.exists() {
            return Ok(None);
        }
        DoneMarker::clear(path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "failed to clear done-marker at {p}: {e}"
            ))
        })?;
        Ok(Some(p))
    }

    /// Property: return the user-facing 3-state value (None / False /
    /// str), matching what was passed at construction.
    #[getter(done_marker_path)]
    fn done_marker_path_get(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.done_marker_path {
            DoneMarkerSpec::Auto => py.None(),
            DoneMarkerSpec::Disabled => false.into_pyobject(py).unwrap().to_owned().unbind().into(),
            DoneMarkerSpec::Path(s) => s.clone().into_pyobject(py).unwrap().into_any().unbind(),
        }
    }
}

impl PyTask {
    pub fn empty() -> Self {
        use daisy_core::coordinate::Coordinate;
        use daisy_core::roi::Roi;
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
            max_workers: 0,
            max_retries: 0,
            check_function: None,
            process_function: None,
            done_marker_path: DoneMarkerSpec::Disabled,
            requires: HashMap::new(),
            max_worker_restarts: 10,
            timeout_secs: None,
        }
    }

    /// Convert a single `PyTask` to a core `Arc<Task>`. Tasks are
    /// pure data with no inter-task knowledge — DAG dependencies live
    /// on the `Pipeline`, not the task. Caching by task_id is still
    /// honoured so a Pipeline that mentions the same task twice (via
    /// composition) collapses to a single Arc.
    pub fn convert_task(
        bound: &Bound<'_, PyTask>,
        py: Python<'_>,
        cache: &mut HashMap<String, Arc<Task>>,
    ) -> PyResult<Arc<Task>> {
        let task_id = bound.borrow().task_id.clone();
        if let Some(cached) = cache.get(&task_id) {
            return Ok(cached.clone());
        }

        let borrow = bound.borrow();
        let mut builder = Task::builder(&borrow.task_id)
            .total_roi(borrow.total_roi.inner.clone())
            .read_roi(borrow.read_roi.inner.clone())
            .write_roi(borrow.write_roi.inner.clone())
            .read_write_conflict(borrow.read_write_conflict)
            .fit(Fit::from_str(&borrow.fit))
            .max_workers(borrow.max_workers)
            .max_retries(borrow.max_retries);

        if let Some(path) = borrow.done_marker_path.resolve(py, &borrow.task_id) {
            builder = builder.done_marker_path(&path);
        }
        if !borrow.requires.is_empty() {
            builder = builder.requires(borrow.requires.clone());
        }
        builder = builder.max_worker_restarts(borrow.max_worker_restarts);
        if let Some(secs) = borrow.timeout_secs {
            if secs > 0.0 && secs.is_finite() {
                builder = builder.timeout(std::time::Duration::from_secs_f64(secs));
            }
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
