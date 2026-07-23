use daisy_core::{coordinate::Coordinate, roi::Roi};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

/// Coordinate-like values of a foreign object: a native PyCoordinate, or
/// any sequence of ints (funlib.geometry.Coordinate is a tuple subclass).
fn coord_values(obj: &Bound<'_, PyAny>) -> Option<Vec<i64>> {
    if let Ok(c) = obj.extract::<PyCoordinate>() {
        return Some(c.inner.as_slice().to_vec());
    }
    obj.extract::<Vec<i64>>().ok()
}

fn hash_inner<T: std::hash::Hash>(value: &T) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

/// Extract a native Roi for a method argument, with an error message that
/// names the offending type instead of pyo3's default
/// "'Roi' object is not an instance of 'Roi'" (which is what you get when
/// a funlib.geometry.Roi is passed where a native Roi is required).
fn expect_native_roi(obj: &Bound<'_, PyAny>) -> PyResult<Roi> {
    if let Ok(r) = obj.extract::<PyRoi>() {
        return Ok(r.inner);
    }
    let tname = obj
        .get_type()
        .fully_qualified_name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
        "expected a daisy (native) Roi, got {tname}; convert with \
         daisy.Roi(tuple(other.offset), tuple(other.shape))"
    )))
}

#[pyclass(name = "Coordinate", frozen, from_py_object, module = "daisy._daisy")]
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

#[pyclass(name = "Roi", frozen, from_py_object, module = "daisy._daisy")]
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

    fn intersect(&self, other: &Bound<'_, PyAny>) -> PyResult<PyRoi> {
        let other = expect_native_roi(other)?;
        Ok(PyRoi {
            inner: self.inner.intersect(&other),
        })
    }

    fn contains(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(roi) = other.extract::<PyRoi>() {
            Ok(self.inner.contains_roi(&roi.inner))
        } else if let Ok(coord) = other.extract::<PyCoordinate>() {
            Ok(self.inner.contains_point(&coord.inner))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected Roi or Coordinate",
            ))
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
