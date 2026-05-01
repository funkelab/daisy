//! Env-var encoding for passing the server address (host, port,
//! task_id, worker_id) to worker processes via the `DAISY_CONTEXT`
//! environment variable.

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::collections::HashMap;

const ENV_VARIABLE: &str = "DAISY_CONTEXT";

/// Dict-like key→string store with `to_env`/`from_env` helpers.
/// Construct from kwargs (`Context(hostname="...", port=12345)`)
/// or from `Context.from_env()` to read back the env var on the
/// worker side. Keys and values are always coerced to `str`.
#[pyclass(name = "Context")]
pub struct PyContext {
    inner: HashMap<String, String>,
}

#[pymethods]
impl PyContext {
    #[classattr]
    const ENV_VARIABLE: &'static str = ENV_VARIABLE;

    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut inner = HashMap::new();
        if let Some(d) = kwargs {
            for (k, v) in d.iter() {
                let key: String = k.str()?.extract()?;
                let val: String = v.str()?.extract()?;
                inner.insert(key, val);
            }
        }
        Ok(Self { inner })
    }

    fn __setitem__(&mut self, k: &Bound<'_, PyAny>, v: &Bound<'_, PyAny>) -> PyResult<()> {
        let key: String = k.str()?.extract()?;
        let val: String = v.str()?.extract()?;
        self.inner.insert(key, val);
        Ok(())
    }

    fn __getitem__(&self, k: &str) -> PyResult<String> {
        self.inner
            .get(k)
            .cloned()
            .ok_or_else(|| PyErr::new::<PyKeyError, _>(k.to_string()))
    }

    fn __delitem__(&mut self, k: &str) -> PyResult<()> {
        self.inner
            .remove(k)
            .map(|_| ())
            .ok_or_else(|| PyErr::new::<PyKeyError, _>(k.to_string()))
    }

    fn __contains__(&self, k: &str) -> bool {
        self.inner.contains_key(k)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for k in self.inner.keys() {
            list.append(k)?;
        }
        Ok(list.unbind())
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.__iter__(py)
    }

    fn values(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for v in self.inner.values() {
            list.append(v)?;
        }
        Ok(list.unbind())
    }

    fn items(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for (k, v) in &self.inner {
            let pair = PyTuple::new(py, &[k.into_pyobject(py)?, v.into_pyobject(py)?])?;
            list.append(pair)?;
        }
        Ok(list.unbind())
    }

    #[pyo3(signature = (k, default=None))]
    fn get(
        &self,
        py: Python<'_>,
        k: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        match self.inner.get(k) {
            Some(v) => Ok(v.clone().into_pyobject(py)?.into_any().unbind()),
            None => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    fn copy(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }

    /// Encode as `key=value:key=value` for the `DAISY_CONTEXT` env var.
    /// Keys are emitted in insertion order isn't preserved by HashMap;
    /// callers that depend on a specific order should not rely on the
    /// output ordering.
    fn to_env(&self) -> String {
        self.inner
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(":")
    }

    /// Reconstruct from the `DAISY_CONTEXT` env var. Raises `KeyError`
    /// if the variable is unset.
    #[staticmethod]
    fn from_env() -> PyResult<Self> {
        let env = std::env::var(ENV_VARIABLE).map_err(|_| {
            PyErr::new::<PyKeyError, _>(format!("{ENV_VARIABLE} not found"))
        })?;
        let mut inner = HashMap::new();
        for token in env.split(':') {
            if token.is_empty() {
                continue;
            }
            let (k, v) = token.split_once('=').ok_or_else(|| {
                PyErr::new::<PyValueError, _>(format!("invalid context token: {token}"))
            })?;
            inner.insert(k.to_string(), v.to_string());
        }
        Ok(Self { inner })
    }

    fn __repr__(&self) -> String {
        self.to_env()
    }
}
