use daisy_core::block_tracking::TaskTracking;
use daisy_core::skip_mask::SkipMask;
use daisy_core::task::{Fit, Task};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::py_callbacks::{PyCheckBlock, PyProcessBlock, PySpawnWorker};
use crate::py_roi::PyRoi;

/// Three-state done-marker spec: `Auto` resolves at convert time
/// against the global basedir set via `set_done_marker_basedir()`;
/// `Disabled` opts out; `Path` is verbatim.
///
/// Block tracking is opt-in: with no explicit `done_marker_path` and
/// no basedir set, `Auto` resolves to no marker at all. There is
/// deliberately no fallback to the logging basedir / CWD `daisy_logs`
/// — persistent skip-state anchored to the current working directory
/// meant a rerun of a (possibly changed) script silently skipped
/// every block a previous run had marked done.
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
    ///   3. `Auto` + basedir set via `set_done_marker_basedir` → `<basedir>/<task_id>`
    ///   4. otherwise            → None (tracking is opt-in)
    pub fn resolve(&self, _py: Python<'_>, task_id: &str) -> Option<String> {
        match self {
            DoneMarkerSpec::Disabled => None,
            DoneMarkerSpec::Path(s) => Some(s.clone()),
            DoneMarkerSpec::Auto => get_basedir()
                .map(|b| b.join(task_id).to_string_lossy().into_owned()),
        }
    }
}

/// `<log basedir>/<task_id>/tracking-<timestamp>` for a task that asked for
/// resource tracking without naming a store; `None` (and the scheduler's
/// configuration error) when file logging is off as well.
fn resource_tracking_fallback(py: Python<'_>, task_id: &str) -> PyResult<Option<String>> {
    let logging = py.import("daisy.logging")?;
    let base = logging.call_method0("get_log_basedir")?;
    if base.is_none() {
        return Ok(None);
    }
    let base: String = base.str()?.extract()?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let path = PathBuf::from(base)
        .join(task_id)
        .join(format!("tracking-{stamp}-{}", std::process::id()));
    let path_str = path.to_string_lossy().into_owned();
    let warnings = py.import("warnings")?;
    warnings.call_method1(
        "warn",
        (format!(
            "task {task_id:?}: resource_tracking=True without a tracking path; \
             measurements go to {path_str} (run-scoped, never resumed from). \
             Pass Task(tracking_path=...) or call daisy.set_tracking_basedir(...) \
             to keep a resumable store."
        ),),
    )?;
    Ok(Some(path_str))
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

/// The universal default block timeout. Kept in one place; also exported
/// to python as `daisy._daisy.DEFAULT_BLOCK_TIMEOUT_SECS`.
pub const DEFAULT_BLOCK_TIMEOUT_SECS: f64 = 600.0;

#[pyclass(name = "Task", skip_from_py_object, subclass, module = "daisy._daisy")]
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
    pub resource_tracking: bool,
    /// Per-worker resource cost; empty disables resource accounting.
    pub requires: HashMap<String, i64>,
    /// Cap on worker restarts before the task is abandoned. Default 10.
    pub max_worker_restarts: u32,
    /// Per-block processing timeout in seconds. Defaults to 600 (10
    /// minutes) and can never be disabled — a wedged block function
    /// must not hang a run forever. Historic note: `None` used to mean
    /// no timeout — blocks could sit in `processing` indefinitely as
    /// long as the worker stays connected. When set, the bookkeeper
    /// reclaims any block that's been processing longer than this and
    /// re-queues it for retry (or orphans, depending on retry budget).
    pub timeout_secs: Option<f64>,
    /// Caller-provided blocks to skip this run: C-ordered bytes at the
    /// task's block-grid shape, nonzero = skip. See
    /// `daisy_core::skip_mask`. Validated against the layout here, at
    /// construction, so a wrong shape fails before anything runs.
    pub skip_mask: Option<SkipMask>,
}

/// Accept a numpy-like array (anything with `.shape` and `.tobytes()`),
/// or a `(bytes, shape)` pair, as a skip mask. The Python `Task`
/// wrapper normalises arrays to contiguous uint8 first; this fallback
/// handles direct `_daisy.Task` construction.
fn parse_skip_mask(v: &Bound<'_, PyAny>) -> PyResult<SkipMask> {
    let (data, shape): (Vec<u8>, Vec<usize>) = if v.hasattr("tobytes")? && v.hasattr("shape")? {
        let shape: Vec<usize> = v.getattr("shape")?.extract()?;
        let data: Vec<u8> = v.call_method0("tobytes")?.extract()?;
        (data, shape)
    } else if let Ok(pair) = v.extract::<(Vec<u8>, Vec<usize>)>() {
        pair
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "skip_mask must be an array (anything with .shape and .tobytes(), \
             e.g. a numpy uint8 array) or a (bytes, shape) pair",
        ));
    };
    SkipMask::new(shape, data).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "skip_mask: {e}; pass a contiguous uint8 (or bool) array with one \
             cell per block"
        ))
    })
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
        resource_tracking=false,
        requires=None,
        max_worker_restarts=10,
        skip_mask=None,
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
        max_workers: i64,
        max_retries: u32,
        timeout: Option<&Bound<'_, PyAny>>,
        done_marker_path: Option<&Bound<'_, PyAny>>,
        resource_tracking: bool,
        requires: Option<Bound<'_, PyDict>>,
        max_worker_restarts: u32,
        skip_mask: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        // A task with no workers can never make progress: the server would
        // wait forever for a block that nobody will ever acquire (#81).
        if max_workers < 1 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "max_workers must be at least 1 (got {max_workers}); a task with \
                 no workers would wait forever"
            )));
        }
        let max_workers = max_workers as usize;
        let done_marker_path = parse_done_marker_spec(done_marker_path)?;
        // Every block ALWAYS has a timeout (default 10 minutes): a wedged
        // block function must never be able to hang a run — or its
        // shutdown — forever. Users with genuinely slow blocks raise the
        // value; there is deliberately no way to disable it.
        let timeout_secs: Option<f64> = match timeout {
            None => Some(DEFAULT_BLOCK_TIMEOUT_SECS),
            Some(v) if v.is_none() => Some(DEFAULT_BLOCK_TIMEOUT_SECS),
            Some(v) => {
                let secs = if let Ok(secs) = v.call_method0("total_seconds")
                    .and_then(|x| x.extract::<f64>())
                {
                    secs
                } else if let Ok(secs) = v.extract::<f64>() {
                    secs
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "timeout must be a float seconds, a timedelta, or None \
                         (None means the 600s default)",
                    ));
                };
                if !(secs > 0.0) {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "block timeout must be positive; blocks are expected \
                         to be fast — raise the timeout instead of disabling it",
                    ));
                }
                Some(secs)
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
        let skip_mask = match skip_mask {
            None => None,
            Some(v) if v.is_none() => None,
            Some(v) => {
                let mask = parse_skip_mask(v)?;
                let layout = Task::builder(&task_id)
                    .total_roi(total_roi.inner.clone())
                    .read_roi(read_roi.inner.clone())
                    .write_roi(write_roi.inner.clone())
                    .fit(Fit::from_str(&fit))
                    .build();
                let expected = layout.block_grid_shape();
                if mask.shape() != expected.as_slice() {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "skip_mask has shape {:?} but task {task_id:?} has a block grid \
                         of shape {:?} (one cell per block, cell 0 is the first block; \
                         see Task.block_grid_shape)",
                        mask.shape(),
                        expected
                    )));
                }
                Some(mask)
            }
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
            resource_tracking,
            requires: reqs,
            max_worker_restarts,
            timeout_secs,
            skip_mask,
        })
    }

    /// Number of blocks per axis, as a tuple: the shape a `skip_mask`
    /// must have. Cell ``(0, 0, ...)`` is the first block regardless of
    /// context.
    #[getter]
    fn block_grid_shape(&self) -> Vec<usize> {
        Task::builder(&self.task_id)
            .total_roi(self.total_roi.inner.clone())
            .read_roi(self.read_roi.inner.clone())
            .write_roi(self.write_roi.inner.clone())
            .fit(Fit::from_str(&self.fit))
            .build()
            .block_grid_shape()
    }

    /// The blocks this run skips, as a uint8 numpy array over the block
    /// grid (``block_grid_shape``; nonzero = skip), or None.
    ///
    /// Cell ``(0, 0, ...)`` is the first block, whatever the context: the
    /// cell of a block is ``(block.write_roi.offset -
    /// total_write_roi.offset) // write_roi.shape``. The mask is an input
    /// to this run, not persisted state: masked blocks are reported as
    /// skipped and release their downstream dependencies, nothing is
    /// written to the tracking store for them, and a later run with a
    /// narrower mask (or none) runs them.
    #[getter]
    fn skip_mask(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let Some(m) = self.skip_mask.as_ref() else {
            return Ok(py.None());
        };
        let np = py.import("numpy")?;
        let arr = np.call_method1(
            "frombuffer",
            (PyBytes::new(py, m.data()), "uint8"),
        )?;
        let arr = arr.call_method1("reshape", (m.shape().to_vec(),))?;
        // frombuffer gives a read-only view over the bytes; hand out a
        // copy so callers can edit it and pass it to a new Task.
        Ok(arr.call_method0("copy")?.unbind())
    }

    #[getter]
    fn task_id(&self) -> &str {
        &self.task_id
    }

    /// `funlib.geometry.Roi`, like every ROI daisy hands to Python.
    #[getter]
    fn total_roi(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::py_roi::funlib_roi(py, &self.total_roi.inner)
    }

    #[getter]
    fn read_roi(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::py_roi::funlib_roi(py, &self.read_roi.inner)
    }

    #[getter]
    fn write_roi(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::py_roi::funlib_roi(py, &self.write_roi.inner)
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
            resource_tracking: self.resource_tracking,
            requires: self.requires.clone(),
            max_worker_restarts: self.max_worker_restarts,
            timeout_secs: self.timeout_secs,
            skip_mask: self.skip_mask.clone(),
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
        TaskTracking::clear(path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "failed to clear block tracking at {p}: {e}"
            ))
        })?;
        Ok(Some(p))
    }

    /// Blocks recorded as failed and not done in this task's tracking
    /// store: every block with a failure count above zero whose done flag
    /// is still unset. Empty when no store is configured or none exists.
    fn failed_blocks(&self, py: Python<'_>) -> PyResult<Vec<crate::py_block::PyBlock>> {
        let Some((tracking, task)) = self.open_tracking(py)? else {
            return Ok(Vec::new());
        };
        let graph = daisy_core::dependency_graph::BlockwiseDependencyGraph::from_task(&task);
        let mut out = Vec::new();
        for level in 0..graph.num_levels() {
            for block in graph.level_blocks(level) {
                if tracking.failure_count(&block) > 0 && !tracking.is_done(&block) {
                    out.push(crate::py_block::PyBlock::from_core(block));
                }
            }
        }
        Ok(out)
    }

    /// What the tracking store holds for one block, as a dict: `done`,
    /// `failures`, `stats` (a `BlockStats` or None), `worker_id`,
    /// `sent_at`, `returned_at` (None without a recorded dispatch). None
    /// when no store is configured, none exists, or the block is not
    /// addressable in it (a boundary block under overhang / shrink).
    fn block_record<'py>(
        &self,
        py: Python<'py>,
        block: &crate::py_block::PyBlock,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some((tracking, _task)) = self.open_tracking(py)? else {
            return Ok(None);
        };
        let Some(rec) = tracking.record_of(&block.inner) else {
            return Ok(None);
        };
        let d = Self::record_dict(py, &rec)?;
        d.set_item("spatial_id", block.inner.block_id.spatial_id)?;
        Ok(Some(d))
    }

    /// Every block the tracking store has recorded — done, or with a
    /// failure count — as the dicts `block_record` returns, each with its
    /// `spatial_id`. Opens the store once and walks the whole block grid,
    /// so it is O(blocks); for very large grids read the zarr arrays
    /// directly instead. Empty when no store is configured or none exists.
    fn block_records<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let Some((tracking, task)) = self.open_tracking(py)? else {
            return Ok(Vec::new());
        };
        let graph = daisy_core::dependency_graph::BlockwiseDependencyGraph::from_task(&task);
        let mut out = Vec::new();
        for level in 0..graph.num_levels() {
            for block in graph.level_blocks(level) {
                let Some(rec) = tracking.record_of(&block) else {
                    continue;
                };
                if !rec.done && rec.failures == 0 {
                    continue;
                }
                let d = Self::record_dict(py, &rec)?;
                d.set_item("spatial_id", block.block_id.spatial_id)?;
                out.push(d);
            }
        }
        Ok(out)
    }

    /// The directory this task's tracking store resolves to — an explicit
    /// `tracking_path`, or `<tracking basedir>/<task_id>` when a basedir is
    /// set — or None when the task has no persistent store (tracking off,
    /// or the run-scoped store `resource_tracking=True` falls back to).
    #[getter]
    fn tracking_path(&self, py: Python<'_>) -> Option<String> {
        self.done_marker_path.resolve(py, &self.task_id)
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
            resource_tracking: false,
            requires: HashMap::new(),
            max_worker_restarts: 10,
            timeout_secs: Some(DEFAULT_BLOCK_TIMEOUT_SECS),
            skip_mask: None,
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

        match borrow.done_marker_path.resolve(py, &borrow.task_id) {
            Some(path) => builder = builder.tracking_path(&path),
            None if borrow.resource_tracking => {
                // resource_tracking=True asks for per-block measurements,
                // which need a store. Without an explicit tracking path
                // (or basedir) fall back to a run-scoped directory under
                // the log basedir: measurements are kept, but a later run
                // never resumes from it, so the stale-skip hazard that
                // removed the old always-on fallback cannot recur.
                if let Some(path) = resource_tracking_fallback(py, &borrow.task_id)? {
                    builder = builder.tracking_path(&path);
                }
            }
            None => {}
        }
        builder = builder.resource_tracking(borrow.resource_tracking);
        if let Some(mask) = borrow.skip_mask.as_ref() {
            builder = builder.skip_mask(mask.clone());
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
                // Only the serial path gets here: the distributed runner
                // wraps 1-arg block functions into a 0-arg spawn function
                // before building the core task, so every distributed
                // worker is a subprocess.
                builder = builder.process_function(PyProcessBlock::new(
                    process_fn.clone_ref(py),
                    borrow.resource_tracking,
                ));
            }
        }

        let arc = Arc::new(builder.build());
        cache.insert(task_id, arc.clone());
        Ok(arc)
    }
}

/// Store access shared by the read-only diagnostics above (not exposed
/// to Python).
impl PyTask {
    fn record_dict<'py>(
        py: Python<'py>,
        rec: &daisy_core::block_tracking::BlockRecord,
    ) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("done", rec.done)?;
        d.set_item("failures", rec.failures)?;
        d.set_item(
            "stats",
            rec.stats.map(|inner| crate::py_block::PyBlockStats { inner }),
        )?;
        d.set_item("worker_id", rec.dispatch.and_then(|x| x.worker_id))?;
        d.set_item("sent_at", rec.dispatch.map(|x| x.sent_at))?;
        d.set_item("returned_at", rec.dispatch.map(|x| x.returned_at))?;
        Ok(d)
    }

    /// Open this task's tracking store for reading, if one is configured
    /// and exists. Maps the stat arrays only when the store already has
    /// them, so a read never adds arrays to a store.
    fn open_tracking(&self, py: Python<'_>) -> PyResult<Option<(TaskTracking, Task)>> {
        let Some(p) = self.done_marker_path.resolve(py, &self.task_id) else {
            return Ok(None);
        };
        let path = Path::new(&p);
        if !path.join("zarr.json").exists() {
            return Ok(None);
        }
        let task = Task::builder(&self.task_id)
            .total_roi(self.total_roi.inner.clone())
            .read_roi(self.read_roi.inner.clone())
            .write_roi(self.write_roi.inner.clone())
            .read_write_conflict(self.read_write_conflict)
            .fit(Fit::from_str(&self.fit))
            .build();
        let with_stats = path
            .join(daisy_core::block_tracking::ARRAY_WALL_SECONDS)
            .join("zarr.json")
            .exists();
        let tracking = TaskTracking::open_or_create(
            path,
            &task.total_roi,
            &task.read_roi,
            &task.write_roi,
            &task.fit,
            with_stats,
        )
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "cannot open block tracking at {p}: {e}"
            ))
        })?;
        Ok(Some((tracking, task)))
    }
}
