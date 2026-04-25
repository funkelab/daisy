use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

/// An n-dimensional coordinate. Wraps a `Vec<i64>` with element-wise
/// arithmetic and convenience methods used throughout the dependency graph
/// and ROI computations.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct Coordinate(Vec<i64>);

impl Coordinate {
    pub fn new(values: Vec<i64>) -> Self {
        Self(values)
    }

    pub fn zeros(dims: usize) -> Self {
        Self(vec![0; dims])
    }

    pub fn dims(&self) -> usize {
        self.0.len()
    }

    pub fn as_slice(&self) -> &[i64] {
        &self.0
    }

    pub fn iter(&self) -> std::slice::Iter<'_, i64> {
        self.0.iter()
    }

    /// Element-wise maximum of two coordinates.
    pub fn max(&self, other: &Coordinate) -> Coordinate {
        assert_eq!(self.dims(), other.dims());
        Coordinate(
            self.0
                .iter()
                .zip(other.0.iter())
                .map(|(a, b)| *a.max(b))
                .collect(),
        )
    }

    /// Element-wise minimum of two coordinates.
    pub fn min(&self, other: &Coordinate) -> Coordinate {
        assert_eq!(self.dims(), other.dims());
        Coordinate(
            self.0
                .iter()
                .zip(other.0.iter())
                .map(|(a, b)| *a.min(b))
                .collect(),
        )
    }

    /// Product of all elements.
    pub fn product(&self) -> i64 {
        self.0.iter().product()
    }
}

impl fmt::Display for Coordinate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{v}")?;
        }
        write!(f, ")")
    }
}

impl std::ops::Index<usize> for Coordinate {
    type Output = i64;
    fn index(&self, index: usize) -> &i64 {
        &self.0[index]
    }
}

impl<'a> IntoIterator for &'a Coordinate {
    type Item = &'a i64;
    type IntoIter = std::slice::Iter<'a, i64>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl IntoIterator for Coordinate {
    type Item = i64;
    type IntoIter = std::vec::IntoIter<i64>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<Vec<i64>> for Coordinate {
    fn from(v: Vec<i64>) -> Self {
        Self(v)
    }
}

impl From<&[i64]> for Coordinate {
    fn from(v: &[i64]) -> Self {
        Self(v.to_vec())
    }
}

// --- Element-wise arithmetic ---

impl Add for &Coordinate {
    type Output = Coordinate;
    fn add(self, rhs: &Coordinate) -> Coordinate {
        assert_eq!(self.dims(), rhs.dims());
        Coordinate(self.0.iter().zip(rhs.0.iter()).map(|(a, b)| a + b).collect())
    }
}

impl Add for Coordinate {
    type Output = Coordinate;
    fn add(self, rhs: Coordinate) -> Coordinate {
        &self + &rhs
    }
}

impl Add<&Coordinate> for Coordinate {
    type Output = Coordinate;
    fn add(self, rhs: &Coordinate) -> Coordinate {
        &self + rhs
    }
}

impl Add<Coordinate> for &Coordinate {
    type Output = Coordinate;
    fn add(self, rhs: Coordinate) -> Coordinate {
        self + &rhs
    }
}

impl Sub for &Coordinate {
    type Output = Coordinate;
    fn sub(self, rhs: &Coordinate) -> Coordinate {
        assert_eq!(self.dims(), rhs.dims());
        Coordinate(self.0.iter().zip(rhs.0.iter()).map(|(a, b)| a - b).collect())
    }
}

impl Sub for Coordinate {
    type Output = Coordinate;
    fn sub(self, rhs: Coordinate) -> Coordinate {
        &self - &rhs
    }
}

impl Sub<&Coordinate> for Coordinate {
    type Output = Coordinate;
    fn sub(self, rhs: &Coordinate) -> Coordinate {
        &self - rhs
    }
}

impl Sub<Coordinate> for &Coordinate {
    type Output = Coordinate;
    fn sub(self, rhs: Coordinate) -> Coordinate {
        self - &rhs
    }
}

impl Mul for &Coordinate {
    type Output = Coordinate;
    fn mul(self, rhs: &Coordinate) -> Coordinate {
        assert_eq!(self.dims(), rhs.dims());
        Coordinate(self.0.iter().zip(rhs.0.iter()).map(|(a, b)| a * b).collect())
    }
}

impl Mul for Coordinate {
    type Output = Coordinate;
    fn mul(self, rhs: Coordinate) -> Coordinate {
        &self * &rhs
    }
}

/// Scalar multiplication.
impl Mul<i64> for &Coordinate {
    type Output = Coordinate;
    fn mul(self, rhs: i64) -> Coordinate {
        Coordinate(self.0.iter().map(|a| a * rhs).collect())
    }
}

impl Mul<i64> for Coordinate {
    type Output = Coordinate;
    fn mul(self, rhs: i64) -> Coordinate {
        &self * rhs
    }
}

/// Element-wise floor division (Python `//` semantics: rounds toward negative infinity).
impl Div for &Coordinate {
    type Output = Coordinate;
    fn div(self, rhs: &Coordinate) -> Coordinate {
        assert_eq!(self.dims(), rhs.dims());
        Coordinate(
            self.0
                .iter()
                .zip(rhs.0.iter())
                .map(|(a, b)| a.div_euclid(*b))
                .collect(),
        )
    }
}

impl Div for Coordinate {
    type Output = Coordinate;
    fn div(self, rhs: Coordinate) -> Coordinate {
        &self / &rhs
    }
}

impl Div<&Coordinate> for Coordinate {
    type Output = Coordinate;
    fn div(self, rhs: &Coordinate) -> Coordinate {
        &self / rhs
    }
}

impl Div<Coordinate> for &Coordinate {
    type Output = Coordinate;
    fn div(self, rhs: Coordinate) -> Coordinate {
        self / &rhs
    }
}

/// Element-wise remainder (Python `%` semantics: result has the sign of the divisor).
impl Rem for &Coordinate {
    type Output = Coordinate;
    fn rem(self, rhs: &Coordinate) -> Coordinate {
        assert_eq!(self.dims(), rhs.dims());
        Coordinate(
            self.0
                .iter()
                .zip(rhs.0.iter())
                .map(|(a, b)| a.rem_euclid(*b))
                .collect(),
        )
    }
}

impl Rem for Coordinate {
    type Output = Coordinate;
    fn rem(self, rhs: Coordinate) -> Coordinate {
        &self % &rhs
    }
}

impl Rem<&Coordinate> for Coordinate {
    type Output = Coordinate;
    fn rem(self, rhs: &Coordinate) -> Coordinate {
        &self % rhs
    }
}

impl Rem<Coordinate> for &Coordinate {
    type Output = Coordinate;
    fn rem(self, rhs: Coordinate) -> Coordinate {
        self % &rhs
    }
}

impl Neg for &Coordinate {
    type Output = Coordinate;
    fn neg(self) -> Coordinate {
        Coordinate(self.0.iter().map(|a| -a).collect())
    }
}

impl Neg for Coordinate {
    type Output = Coordinate;
    fn neg(self) -> Coordinate {
        -&self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arithmetic() {
        let a = Coordinate::new(vec![1, 2, 3]);
        let b = Coordinate::new(vec![4, 5, 6]);
        assert_eq!(&a + &b, Coordinate::new(vec![5, 7, 9]));
        assert_eq!(&b - &a, Coordinate::new(vec![3, 3, 3]));
        assert_eq!(&a * &b, Coordinate::new(vec![4, 10, 18]));
    }

    #[test]
    fn test_floor_div_python_semantics() {
        // Python: -7 // 3 == -3  (rounds toward -inf)
        let a = Coordinate::new(vec![-7, 7]);
        let b = Coordinate::new(vec![3, 3]);
        assert_eq!(&a / &b, Coordinate::new(vec![-3, 2]));
    }

    #[test]
    fn test_rem_python_semantics() {
        // Python: -7 % 3 == 2  (result has sign of divisor)
        let a = Coordinate::new(vec![-7, 7]);
        let b = Coordinate::new(vec![3, 3]);
        assert_eq!(&a % &b, Coordinate::new(vec![2, 1]));
    }

    #[test]
    fn test_neg() {
        let a = Coordinate::new(vec![1, -2, 3]);
        assert_eq!(-a, Coordinate::new(vec![-1, 2, -3]));
    }

    #[test]
    fn test_product() {
        let a = Coordinate::new(vec![2, 3, 4]);
        assert_eq!(a.product(), 24);
    }
}
