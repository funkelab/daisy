use gerbera_core::{coordinate::Coordinate, roi::Roi};
use pyo3::prelude::*;

#[pyclass(name = "Coordinate", frozen, from_py_object, eq, hash)]
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
}

#[pyclass(name = "Roi", frozen, from_py_object, eq, hash)]
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

    fn intersect(&self, other: &Self) -> PyRoi {
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
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "expected Roi or Coordinate",
            ))
        }
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }
}
