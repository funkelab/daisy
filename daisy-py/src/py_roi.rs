use daisy_core::{coordinate::Coordinate, roi::Roi};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

/// `funlib.geometry.Roi` for a core Roi: the type every ROI handed to
/// Python is. The class is looked up once; `funlib.geometry` is a hard
/// dependency.
pub fn funlib_roi(py: Python<'_>, roi: &Roi) -> PyResult<Py<PyAny>> {
    static ROI_CLS: std::sync::OnceLock<Py<PyAny>> = std::sync::OnceLock::new();
    let cls = match ROI_CLS.get() {
        Some(c) => c,
        None => {
            let c = py.import("funlib.geometry")?.getattr("Roi")?.unbind();
            let _ = ROI_CLS.set(c);
            ROI_CLS.get().expect("just set")
        }
    };
    let offset: Vec<i64> = roi.offset().as_slice().to_vec();
    let shape: Vec<i64> = roi.shape().as_slice().to_vec();
    Ok(cls.bind(py).call1((offset, shape))?.unbind())
}

fn type_name_of(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .fully_qualified_name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string())
}

/// Coordinate-like values of a foreign object: a native PyCoordinate, or
/// any sequence of ints (funlib.geometry.Coordinate is a tuple subclass).
fn coord_values(obj: &Bound<'_, PyAny>) -> Option<Vec<i64>> {
    obj.extract::<PyCoordinate>()
        .ok()
        .map(|c| c.inner.as_slice().to_vec())
}

/// Every native entry point that takes a Coordinate accepts the native
/// type or any sequence of ints — a tuple, a list, a
/// `funlib.geometry.Coordinate`. Strings are not sequences of ints.
impl<'a, 'py> FromPyObject<'a, 'py> for PyCoordinate {
    type Error = PyErr;

    fn extract(obj: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(c) = obj.downcast::<PyCoordinate>() {
            return Ok(c.get().clone());
        }
        match obj.extract::<Vec<i64>>() {
            Ok(values) => Ok(PyCoordinate {
                inner: Coordinate::new(values),
            }),
            Err(_) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "expected a Coordinate or a sequence of ints, got {}",
                type_name_of(&obj)
            ))),
        }
    }
}

/// Every native entry point that takes a Roi accepts the native type or
/// anything exposing `offset` (or `begin`) and `shape` as int sequences —
/// in particular a `funlib.geometry.Roi`, so 1.x code keeps passing funlib
/// ROIs to `Task`, `Block`, `BlockwiseDependencyGraph`, `intersect`, ...
/// unchanged. The result of a native method is still a native Roi.
impl<'a, 'py> FromPyObject<'a, 'py> for PyRoi {
    type Error = PyErr;

    fn extract(obj: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(r) = obj.downcast::<PyRoi>() {
            return Ok(r.get().clone());
        }
        let offset = obj
            .getattr("offset")
            .or_else(|_| obj.getattr("begin"))
            .ok()
            .as_ref()
            .and_then(coord_values);
        let shape = obj.getattr("shape").ok().as_ref().and_then(coord_values);
        match (offset, shape) {
            (Some(offset), Some(shape)) => Ok(PyRoi {
                inner: Roi::new(Coordinate::new(offset), Coordinate::new(shape)),
            }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "expected a Roi (daisy.Roi, funlib.geometry.Roi, or any object \
                 with integer `offset` and `shape`), got {}",
                type_name_of(&obj)
            ))),
        }
    }
}

fn hash_inner<T: std::hash::Hash>(value: &T) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

#[pyclass(name = "Coordinate", frozen, skip_from_py_object, module = "daisy._daisy")]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyCoordinate {
    pub inner: Coordinate,
}

#[pymethods]
impl PyCoordinate {
    #[new]
    fn new(values: Vec<i64>) -> Self {
        Self {
            inner: Coordinate::new(values),
        }
    }

    #[getter]
    fn dims(&self) -> usize {
        self.inner.dims()
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.dims()
    }

    fn __getitem__(&self, idx: usize) -> PyResult<i64> {
        if idx < self.inner.dims() {
            Ok(self.inner[idx])
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "index out of range",
            ))
        }
    }

    fn to_list(&self) -> Vec<i64> {
        self.inner.as_slice().to_vec()
    }

    /// Equality with duck-typing: another native Coordinate, or any int
    /// sequence (tuple, list, funlib.geometry.Coordinate) compares by
    /// value. Anything else defers via NotImplemented so Python falls
    /// back to standard semantics. NOTE: cross-type equality does NOT
    /// come with cross-type hash equality (a funlib Coordinate hashes as
    /// a tuple) — do not mix native and funlib types as dict/set keys.
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = other.py();
        match coord_values(other) {
            Some(values) => (self.inner.as_slice() == values.as_slice()).into_py_any(py),
            None => Ok(py.NotImplemented()),
        }
    }

    fn __hash__(&self) -> u64 {
        hash_inner(&self.inner)
    }
}

#[pyclass(name = "Roi", frozen, skip_from_py_object, module = "daisy._daisy")]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyRoi {
    pub inner: Roi,
}

#[pymethods]
impl PyRoi {
    #[new]
    fn new(offset: Vec<i64>, shape: Vec<i64>) -> Self {
        Self {
            inner: Roi::new(Coordinate::new(offset), Coordinate::new(shape)),
        }
    }

    #[getter]
    fn begin(&self) -> PyCoordinate {
        PyCoordinate {
            inner: self.inner.begin().clone(),
        }
    }

    #[getter]
    fn offset(&self) -> PyCoordinate {
        PyCoordinate {
            inner: self.inner.offset().clone(),
        }
    }

    #[getter]
    fn shape(&self) -> PyCoordinate {
        PyCoordinate {
            inner: self.inner.shape().clone(),
        }
    }

    #[getter]
    fn end(&self) -> PyCoordinate {
        PyCoordinate {
            inner: self.inner.end(),
        }
    }

    #[getter]
    fn dims(&self) -> usize {
        self.inner.dims()
    }

    fn grow(&self, neg_begin: Vec<i64>, pos_end: Vec<i64>) -> PyRoi {
        PyRoi {
            inner: self.inner.grow(
                &Coordinate::new(neg_begin),
                &Coordinate::new(pos_end),
            ),
        }
    }

    fn intersect(&self, other: PyRoi) -> PyRoi {
        PyRoi {
            inner: self.inner.intersect(&other.inner),
        }
    }

    fn contains(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(roi) = other.extract::<PyRoi>() {
            Ok(self.inner.contains_roi(&roi.inner))
        } else if let Ok(coord) = other.extract::<PyCoordinate>() {
            Ok(self.inner.contains_point(&coord.inner))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "expected a Roi or a Coordinate, got {}",
                type_name_of(other)
            )))
        }
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }

    /// Equality with duck-typing: another native Roi compares directly;
    /// any object exposing `offset` (or `begin`) and `shape` — e.g.
    /// funlib.geometry.Roi — compares by offset+shape values. Anything
    /// else defers via NotImplemented. NOTE: cross-type equality does
    /// NOT come with cross-type hash equality — do not mix native and
    /// funlib ROIs as dict/set keys.
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = other.py();
        if let Ok(roi) = other.extract::<PyRoi>() {
            return (self.inner == roi.inner).into_py_any(py);
        }
        let offset = other
            .getattr("offset")
            .or_else(|_| other.getattr("begin"))
            .ok()
            .as_ref()
            .and_then(coord_values);
        let shape = other.getattr("shape").ok().as_ref().and_then(coord_values);
        match (offset, shape) {
            (Some(offset), Some(shape)) => {
                let eq = self.inner.offset().as_slice() == offset.as_slice()
                    && self.inner.shape().as_slice() == shape.as_slice();
                eq.into_py_any(py)
            }
            _ => Ok(py.NotImplemented()),
        }
    }

    fn __hash__(&self) -> u64 {
        hash_inner(&self.inner)
    }
}
