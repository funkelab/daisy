use crate::coordinate::Coordinate;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Sub};

/// A region of interest (ROI) defined by a beginning coordinate (offset) and a
/// shape. The ROI represents the half-open interval [begin, begin + shape).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct Roi {
    offset: Coordinate,
    shape: Coordinate,
}

impl Roi {
    /// Create an ROI from an offset and shape. Both must have the same
    /// dimensionality.
    pub fn new(offset: Coordinate, shape: Coordinate) -> Self {
        assert_eq!(
            offset.dims(),
            shape.dims(),
            "offset and shape must have the same number of dimensions"
        );
        Self { offset, shape }
    }

    /// Shorthand constructor from slices.
    pub fn from_slices(offset: &[i64], shape: &[i64]) -> Self {
        Self::new(Coordinate::from(offset), Coordinate::from(shape))
    }

    /// The beginning (inclusive) coordinate of this ROI.
    pub fn begin(&self) -> &Coordinate {
        &self.offset
    }

    /// Alias for `begin()`.
    pub fn offset(&self) -> &Coordinate {
        &self.offset
    }

    /// The shape (extent in each dimension) of this ROI.
    pub fn shape(&self) -> &Coordinate {
        &self.shape
    }

    /// The end (exclusive) coordinate of this ROI: `begin + shape`.
    pub fn end(&self) -> Coordinate {
        &self.offset + &self.shape
    }

    /// Number of dimensions.
    pub fn dims(&self) -> usize {
        self.offset.dims()
    }

    /// Grow (or shrink) the ROI by the given amounts on each side.
    /// `neg_begin` expands toward lower coordinates, `pos_end` expands toward
    /// higher coordinates. Negative values shrink.
    pub fn grow(&self, neg_begin: &Coordinate, pos_end: &Coordinate) -> Roi {
        let new_offset = &self.offset - neg_begin;
        let new_shape = &self.shape + neg_begin + pos_end;
        Roi::new(new_offset, new_shape)
    }

    /// Return the intersection of this ROI with another. If they do not
    /// overlap, the resulting ROI will have zero or negative shape components;
    /// callers should check for this if needed.
    pub fn intersect(&self, other: &Roi) -> Roi {
        let new_begin = self.offset.max(other.offset());
        let self_end = self.end();
        let other_end = other.end();
        let new_end = self_end.min(&other_end);
        let new_shape_raw = &new_end - &new_begin;
        // Clamp shape to zero — no negative extents.
        let new_shape = Coordinate::new(
            new_shape_raw
                .iter()
                .map(|&v| v.max(0))
                .collect(),
        );
        Roi::new(new_begin, new_shape)
    }

    /// Check whether this ROI fully contains another ROI.
    pub fn contains_roi(&self, other: &Roi) -> bool {
        let self_end = self.end();
        let other_end = other.end();
        self.offset
            .iter()
            .zip(other.offset.iter())
            .all(|(s, o)| s <= o)
            && self_end.iter().zip(other_end.iter()).all(|(s, o)| s >= o)
    }

    /// Check whether this ROI contains a point.
    pub fn contains_point(&self, point: &Coordinate) -> bool {
        let end = self.end();
        self.offset
            .iter()
            .zip(point.iter())
            .all(|(s, p)| s <= p)
            && end.iter().zip(point.iter()).all(|(e, p)| e > p)
    }
}

impl fmt::Display for Roi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Roi({}, {})", self.offset, self.shape)
    }
}

/// Translate an ROI by adding a coordinate offset.
impl Add<&Coordinate> for &Roi {
    type Output = Roi;
    fn add(self, rhs: &Coordinate) -> Roi {
        Roi::new(&self.offset + rhs, self.shape.clone())
    }
}

impl Add<Coordinate> for &Roi {
    type Output = Roi;
    fn add(self, rhs: Coordinate) -> Roi {
        self + &rhs
    }
}

impl Add<&Coordinate> for Roi {
    type Output = Roi;
    fn add(self, rhs: &Coordinate) -> Roi {
        &self + rhs
    }
}

impl Add<Coordinate> for Roi {
    type Output = Roi;
    fn add(self, rhs: Coordinate) -> Roi {
        &self + &rhs
    }
}

/// Translate an ROI by subtracting a coordinate offset.
impl Sub<&Coordinate> for &Roi {
    type Output = Roi;
    fn sub(self, rhs: &Coordinate) -> Roi {
        Roi::new(&self.offset - rhs, self.shape.clone())
    }
}

impl Sub<Coordinate> for &Roi {
    type Output = Roi;
    fn sub(self, rhs: Coordinate) -> Roi {
        self - &rhs
    }
}

impl Sub<&Coordinate> for Roi {
    type Output = Roi;
    fn sub(self, rhs: &Coordinate) -> Roi {
        &self - rhs
    }
}

impl Sub<Coordinate> for Roi {
    type Output = Roi;
    fn sub(self, rhs: Coordinate) -> Roi {
        &self - &rhs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_properties() {
        let roi = Roi::from_slices(&[10, 20], &[30, 40]);
        assert_eq!(roi.begin(), &Coordinate::new(vec![10, 20]));
        assert_eq!(roi.end(), Coordinate::new(vec![40, 60]));
        assert_eq!(roi.shape(), &Coordinate::new(vec![30, 40]));
        assert_eq!(roi.dims(), 2);
    }

    #[test]
    fn test_contains_roi() {
        let outer = Roi::from_slices(&[0, 0], &[100, 100]);
        let inner = Roi::from_slices(&[10, 10], &[20, 20]);
        assert!(outer.contains_roi(&inner));
        assert!(!inner.contains_roi(&outer));
    }

    #[test]
    fn test_contains_point() {
        let roi = Roi::from_slices(&[10, 20], &[30, 40]);
        assert!(roi.contains_point(&Coordinate::new(vec![10, 20])));
        assert!(roi.contains_point(&Coordinate::new(vec![39, 59])));
        assert!(!roi.contains_point(&Coordinate::new(vec![40, 60]))); // exclusive end
        assert!(!roi.contains_point(&Coordinate::new(vec![9, 20])));
    }

    #[test]
    fn test_grow() {
        let roi = Roi::from_slices(&[10, 10], &[20, 20]);
        let grown = roi.grow(
            &Coordinate::new(vec![5, 5]),
            &Coordinate::new(vec![5, 5]),
        );
        assert_eq!(grown, Roi::from_slices(&[5, 5], &[30, 30]));
    }

    #[test]
    fn test_intersect() {
        let a = Roi::from_slices(&[0, 0], &[20, 20]);
        let b = Roi::from_slices(&[10, 10], &[20, 20]);
        let i = a.intersect(&b);
        assert_eq!(i, Roi::from_slices(&[10, 10], &[10, 10]));
    }

    #[test]
    fn test_translate() {
        let roi = Roi::from_slices(&[10, 20], &[30, 40]);
        let offset = Coordinate::new(vec![5, 5]);
        let shifted = &roi + &offset;
        assert_eq!(shifted, Roi::from_slices(&[15, 25], &[30, 40]));
    }
}
