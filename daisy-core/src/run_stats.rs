//! Low-level measurement helpers shared by the block-profiling layer.
//!
//! Statistics themselves live with the blocks: `block_profile` measures a
//! block inside the worker, the payload rides home on `Block::stats`, and
//! `block_tracking` persists and aggregates it. This module holds only the
//! pieces that are useful on their own:
//!
//! - `process_cpu_time` — the whole process's CPU time, so work a block
//!   hands to other threads (zarr's IO and codec threads, thread pools) is
//!   counted. Every worker is its own process running one block at a time,
//!   so the process is the block's natural accounting unit.
//! - `Welford` / `Trend` — the O(1)-memory accumulators behind every
//!   `x ± y` figure and the per-block wall-time slope in the run report.
//! - `linear_trend` — the least-squares fit behind "blocks started at 2 ms
//!   and ended at 12 ms", applied to the per-block wall times collected by
//!   `block_tracking::TaskSummary`.
//!
//! There is deliberately no per-worker or process-wide stats struct here
//! any more: a second accumulator alongside the tracking layer is exactly
//! what made `blocks_processed` disagree with the scheduler's own counts.

use std::time::Duration;

/// Read this process's user+system CPU time, summed over all its threads.
/// Returns None on platforms without `getrusage` (e.g. Windows).
pub fn process_cpu_time() -> Option<Duration> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // SAFETY: getrusage only writes into the rusage we hand it.
        unsafe {
            let mut ru: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
                return None;
            }
            let user = duration_from_timeval(&ru.ru_utime);
            let sys = duration_from_timeval(&ru.ru_stime);
            Some(user + sys)
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn duration_from_timeval(tv: &libc::timeval) -> Duration {
    let secs = tv.tv_sec as u64;
    let micros = tv.tv_usec as u32;
    Duration::new(secs, micros * 1000)
}

/// Least-squares fit of y = m*x + b, where x = 0, 1, ..., n-1.
/// Returns (mean_y, slope_m). Slope is 0 for fewer than 2 points.
pub fn linear_trend(y: &[f64]) -> (f64, f64) {
    let n = y.len();
    if n == 0 {
        return (0.0, 0.0);
    }
    let mean_y: f64 = y.iter().sum::<f64>() / n as f64;
    if n < 2 {
        return (mean_y, 0.0);
    }
    let mean_x = (n - 1) as f64 / 2.0;
    let mut num = 0.0;
    let mut den = 0.0;
    for (i, &yi) in y.iter().enumerate() {
        let xi = i as f64;
        num += (xi - mean_x) * (yi - mean_y);
        den += (xi - mean_x) * (xi - mean_x);
    }
    let slope = if den == 0.0 { 0.0 } else { num / den };
    (mean_y, slope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_trend_constant_signal_has_zero_slope() {
        let (mean, slope) = linear_trend(&[3.0; 100]);
        assert!((mean - 3.0).abs() < 1e-9);
        assert!(slope.abs() < 1e-9);
    }

    #[test]
    fn linear_trend_recovers_known_slope() {
        // y = 2.0 + 0.5 * x for x in 0..10
        let y: Vec<f64> = (0..10).map(|i| 2.0 + 0.5 * i as f64).collect();
        let (mean, slope) = linear_trend(&y);
        assert!((slope - 0.5).abs() < 1e-9, "slope={slope}");
        // mean of 2.0, 2.5, …, 6.5 is 4.25
        assert!((mean - 4.25).abs() < 1e-9, "mean={mean}");
    }

    #[test]
    fn linear_trend_handles_short_inputs() {
        assert_eq!(linear_trend(&[]), (0.0, 0.0));
        assert_eq!(linear_trend(&[5.0]), (5.0, 0.0));
    }

    #[test]
    fn process_cpu_time_returns_some_on_linux_and_macos() {
        // Burn a tiny bit of CPU so the result isn't trivially zero.
        let mut s: u64 = 0;
        for i in 0..10_000 {
            s = s.wrapping_add(i);
        }
        std::hint::black_box(s);
        let t = process_cpu_time();
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(t.is_some(), "expected process CPU on this platform");
        }
    }
}

/// Running mean / sample standard deviation / min / max / sum, by
/// Welford's method. One of these backs every `x ± y` figure in the run
/// report, so the report costs O(1) memory per metric however many blocks
/// or workers feed it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Welford {
    n: u64,
    mean: f64,
    m2: f64,
    min: f64,
    max: f64,
    sum: f64,
}

impl Welford {
    pub fn push(&mut self, x: f64) {
        if !x.is_finite() {
            return;
        }
        if self.n == 0 {
            self.min = x;
            self.max = x;
        } else {
            self.min = self.min.min(x);
            self.max = self.max.max(x);
        }
        self.n += 1;
        self.sum += x;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        self.m2 += delta * (x - self.mean);
    }

    pub fn n(&self) -> u64 {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Sample standard deviation; 0 for fewer than two samples.
    pub fn std(&self) -> f64 {
        if self.n < 2 {
            0.0
        } else {
            (self.m2 / (self.n - 1) as f64).sqrt()
        }
    }

    pub fn min(&self) -> f64 {
        self.min
    }

    pub fn max(&self) -> f64 {
        self.max
    }

    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Fold another accumulator into this one (Chan et al.), so per-worker
    /// samples can be pooled into a per-task figure without re-walking them.
    pub fn merge(&mut self, other: &Welford) {
        if other.n == 0 {
            return;
        }
        if self.n == 0 {
            *self = other.clone();
            return;
        }
        let n = (self.n + other.n) as f64;
        let delta = other.mean - self.mean;
        self.mean += delta * other.n as f64 / n;
        self.m2 += other.m2 + delta * delta * self.n as f64 * other.n as f64 / n;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.n += other.n;
    }
}

impl serde::Serialize for Welford {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("Welford", 6)?;
        st.serialize_field("n", &self.n)?;
        st.serialize_field("mean", &self.mean)?;
        st.serialize_field("std", &self.std())?;
        st.serialize_field("min", &self.min)?;
        st.serialize_field("max", &self.max)?;
        st.serialize_field("sum", &self.sum)?;
        st.end()
    }
}

/// Running least-squares slope of y against sample index, equivalent to
/// `linear_trend` over the full series but in O(1) memory.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Trend {
    n: u64,
    sum_x: f64,
    sum_y: f64,
    sum_xy: f64,
    sum_xx: f64,
}

impl Trend {
    pub fn push(&mut self, y: f64) {
        if !y.is_finite() {
            return;
        }
        let x = self.n as f64;
        self.n += 1;
        self.sum_x += x;
        self.sum_y += y;
        self.sum_xy += x * y;
        self.sum_xx += x * x;
    }

    pub fn n(&self) -> u64 {
        self.n
    }

    pub fn mean(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            self.sum_y / self.n as f64
        }
    }

    /// Slope per sample; 0 for fewer than two samples.
    pub fn slope(&self) -> f64 {
        if self.n < 2 {
            return 0.0;
        }
        let n = self.n as f64;
        let den = n * self.sum_xx - self.sum_x * self.sum_x;
        if den == 0.0 {
            0.0
        } else {
            (n * self.sum_xy - self.sum_x * self.sum_y) / den
        }
    }
}

#[cfg(test)]
mod accumulator_tests {
    use super::*;

    #[test]
    fn welford_matches_hand_computed_moments() {
        let mut w = Welford::default();
        for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            w.push(x);
        }
        assert_eq!(w.n(), 8);
        assert!((w.mean() - 5.0).abs() < 1e-12);
        // Sample std of that series is sqrt(32/7).
        assert!((w.std() - (32.0f64 / 7.0).sqrt()).abs() < 1e-12, "{}", w.std());
        assert_eq!(w.min(), 2.0);
        assert_eq!(w.max(), 9.0);
        assert_eq!(w.sum(), 40.0);
    }

    #[test]
    fn welford_handles_short_inputs_and_non_finite() {
        let mut w = Welford::default();
        assert_eq!(w.std(), 0.0);
        w.push(f64::NAN);
        assert!(w.is_empty());
        w.push(3.0);
        assert_eq!(w.std(), 0.0);
        assert_eq!(w.mean(), 3.0);
    }

    #[test]
    fn welford_merge_equals_pushing_everything_into_one() {
        let xs: Vec<f64> = (0..50).map(|i| (i as f64 * 0.37).sin() * 10.0).collect();
        let mut all = Welford::default();
        let mut a = Welford::default();
        let mut b = Welford::default();
        for (i, &x) in xs.iter().enumerate() {
            all.push(x);
            if i % 3 == 0 {
                a.push(x);
            } else {
                b.push(x);
            }
        }
        a.merge(&b);
        assert_eq!(a.n(), all.n());
        assert!((a.mean() - all.mean()).abs() < 1e-9);
        assert!((a.std() - all.std()).abs() < 1e-9);
        assert_eq!(a.min(), all.min());
        assert_eq!(a.max(), all.max());
    }

    #[test]
    fn trend_matches_linear_trend() {
        let y: Vec<f64> = (0..40).map(|i| 3.0 + 0.25 * i as f64 + ((i * 7) % 5) as f64).collect();
        let mut t = Trend::default();
        for &v in &y {
            t.push(v);
        }
        let (mean, slope) = linear_trend(&y);
        assert!((t.mean() - mean).abs() < 1e-9);
        assert!((t.slope() - slope).abs() < 1e-9, "{} vs {}", t.slope(), slope);
        assert_eq!(Trend::default().slope(), 0.0);
    }
}
