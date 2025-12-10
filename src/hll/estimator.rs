//! HIP (Historical Inverse Probability) Estimator for HyperLogLog
//!
//! The HIP estimator provides improved cardinality estimation by maintaining
//! an accumulator that tracks the historical sequence of register updates.
//! This is more accurate than the standard HLL estimator, especially for
//! moderate cardinalities.

use crate::hll::{composite_interpolation, cubic_interpolation, harmonic_numbers};

/// HIP estimator with KxQ registers for improved cardinality estimation
///
/// This struct encapsulates all estimation-related state and logic,
/// allowing it to be composed into Array4, Array6, and Array8.
///
/// The estimator supports two modes:
/// - **In-order mode**: Uses HIP (Historical Inverse Probability) accumulator
///   for accurate sequential updates
/// - **Out-of-order mode**: Uses composite estimator (raw HLL + linear counting)
///   after deserialization or merging
#[derive(Debug, Clone)]
pub struct HipEstimator {
    /// HIP estimator accumulator
    hip_accum: f64,
    /// KxQ register for values < 32 (larger inverse powers)
    kxq0: f64,
    /// KxQ register for values >= 32 (tiny inverse powers)
    kxq1: f64,
    /// Out-of-order flag: when true, HIP updates are skipped
    out_of_order: bool,
}

impl PartialEq for HipEstimator {
    fn eq(&self, other: &Self) -> bool {
        // For serialization round-trip tests, f64 values should be bit-identical
        // after going through binary serialization
        self.hip_accum == other.hip_accum
            && self.kxq0 == other.kxq0
            && self.kxq1 == other.kxq1
            && self.out_of_order == other.out_of_order
    }
}

impl HipEstimator {
    /// Create a new HIP estimator for a sketch with 2^lg_config_k registers
    pub fn new(lg_config_k: u8) -> Self {
        let k = 1 << lg_config_k;
        Self {
            hip_accum: 0.0,
            kxq0: k as f64, // All registers start at 0, so kxq0 = k * (1/2^0) = k
            kxq1: 0.0,
            out_of_order: false,
        }
    }

    /// Update the estimator when a register changes from old_value to new_value
    ///
    /// This should be called BEFORE actually updating the register in the array.
    ///
    /// # Algorithm
    ///
    /// 1. Update HIP accumulator (unless out-of-order)
    /// 2. Update KxQ registers (always)
    ///
    /// The KxQ registers are split for numerical precision:
    /// - kxq0: sum of 1/2^v for v < 32
    /// - kxq1: sum of 1/2^v for v >= 32
    pub fn update(&mut self, lg_config_k: u8, old_value: u8, new_value: u8) {
        let k = (1 << lg_config_k) as f64;

        // Update HIP accumulator FIRST (unless out-of-order)
        // When out-of-order (from deserialization or merge), HIP is invalid
        if !self.out_of_order {
            self.hip_accum += k / (self.kxq0 + self.kxq1);
        }

        // Always update KxQ registers (regardless of OOO flag)
        self.update_kxq(old_value, new_value);
    }

    /// Update only the KxQ registers (internal helper)
    fn update_kxq(&mut self, old_value: u8, new_value: u8) {
        // Subtract old value contribution
        if old_value < 32 {
            self.kxq0 -= inv_pow2(old_value);
        } else {
            self.kxq1 -= inv_pow2(old_value);
        }

        // Add new value contribution
        if new_value < 32 {
            self.kxq0 += inv_pow2(new_value);
        } else {
            self.kxq1 += inv_pow2(new_value);
        }
    }

    /// Get the current cardinality estimate
    ///
    /// Dispatches to either HIP or composite estimator based on out-of-order flag.
    ///
    /// # Arguments
    /// * `lg_config_k` - Log2 of number of registers (k)
    /// * `cur_min` - Current minimum register value (for Array4, 0 for Array6/8)
    /// * `num_at_cur_min` - Number of registers at cur_min value
    pub fn estimate(&self, lg_config_k: u8, cur_min: u8, num_at_cur_min: u32) -> f64 {
        if self.out_of_order {
            self.get_composite_estimate(lg_config_k, cur_min, num_at_cur_min)
        } else {
            self.hip_accum
        }
    }

    /// Get raw HLL estimate using standard HyperLogLog formula
    ///
    /// Formula: correctionFactor * k^2 / (kxq0 + kxq1)
    ///
    /// Uses lg_k-specific correction factors for small k.
    fn get_raw_estimate(&self, lg_config_k: u8) -> f64 {
        let k = (1 << lg_config_k) as f64;

        // Correction factors from empirical analysis
        let correction_factor = match lg_config_k {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / k),
        };

        (correction_factor * k * k) / (self.kxq0 + self.kxq1)
    }

    /// Get linear counting (bitmap) estimate for small cardinalities
    ///
    /// Uses harmonic numbers to estimate based on empty registers.
    fn get_bitmap_estimate(&self, lg_config_k: u8, cur_min: u8, num_at_cur_min: u32) -> f64 {
        let k = 1 << lg_config_k;

        // Number of unhit (empty) buckets
        let num_unhit = if cur_min == 0 { num_at_cur_min } else { 0 };

        // Edge case: all buckets hit
        if num_unhit == 0 {
            return (k as f64) * (k as f64 / 0.5).ln();
        }

        let num_hit = k - num_unhit;
        harmonic_numbers::bitmap_estimate(k, num_hit)
    }

    /// Get composite estimate (blends raw HLL and linear counting)
    ///
    /// This is the primary estimator used when in out-of-order mode.
    /// It uses cubic interpolation on raw HLL estimate, then blends
    /// with linear counting for small cardinalities.
    fn get_composite_estimate(&self, lg_config_k: u8, cur_min: u8, num_at_cur_min: u32) -> f64 {
        let raw_est = self.get_raw_estimate(lg_config_k);

        // Get composite interpolation table
        let x_arr = composite_interpolation::get_x_arr(lg_config_k);
        let x_arr_len = composite_interpolation::get_x_arr_length();
        let y_stride = composite_interpolation::get_y_stride(lg_config_k) as f64;

        // Handle edge cases
        if raw_est < x_arr[0] {
            return 0.0;
        }

        let x_arr_len_m1 = x_arr_len - 1;

        // Above interpolation range: extrapolate linearly
        if raw_est > x_arr[x_arr_len_m1] {
            let final_y = y_stride * (x_arr_len_m1 as f64);
            let factor = final_y / x_arr[x_arr_len_m1];
            return raw_est * factor;
        }

        // Interpolate using cubic interpolation
        let adj_est = cubic_interpolation::using_x_arr_and_y_stride(x_arr, y_stride, raw_est);

        // Avoid linear counting if estimate is high
        // (threshold: 3*k ensures we're above potential linear counting instability)
        let k = 1 << lg_config_k;
        if adj_est > (3 * k) as f64 {
            return adj_est;
        }

        // Get linear counting estimate
        let lin_est = self.get_bitmap_estimate(lg_config_k, cur_min, num_at_cur_min);

        // Blend estimates based on crossover threshold
        // Use average to reduce bias from threshold comparison
        let avg_est = (adj_est + lin_est) / 2.0;

        // Crossover thresholds (empirically determined)
        let crossover = match lg_config_k {
            4 => 0.718,
            5 => 0.672,
            _ => 0.64,
        };

        let threshold = crossover * (k as f64);

        if avg_est > threshold {
            adj_est
        } else {
            lin_est
        }
    }

    /// Get the HIP accumulator value
    pub fn hip_accum(&self) -> f64 {
        self.hip_accum
    }

    /// Get the kxq0 register value
    pub fn kxq0(&self) -> f64 {
        self.kxq0
    }

    /// Get the kxq1 register value
    pub fn kxq1(&self) -> f64 {
        self.kxq1
    }

    /// Check if this estimator is in out-of-order mode
    pub fn is_out_of_order(&self) -> bool {
        self.out_of_order
    }

    /// Set the out-of-order flag
    ///
    /// This should be set to true when:
    /// - Deserializing a sketch from bytes
    /// - After a merge/union operation
    pub fn set_out_of_order(&mut self, ooo: bool) {
        self.out_of_order = ooo;
        if ooo {
            // When going out-of-order, invalidate HIP accumulator
            // (it will be recomputed if needed via composite estimator)
            self.hip_accum = 0.0;
        }
    }

    /// Set the HIP accumulator directly
    pub fn set_hip_accum(&mut self, value: f64) {
        self.hip_accum = value;
    }

    /// Set the kxq0 register directly
    pub fn set_kxq0(&mut self, value: f64) {
        self.kxq0 = value;
    }

    /// Set the kxq1 register directly
    pub fn set_kxq1(&mut self, value: f64) {
        self.kxq1 = value;
    }
}

/// Compute 1 / 2^value (inverse power of 2)
#[inline]
fn inv_pow2(value: u8) -> f64 {
    if value == 0 {
        1.0
    } else if value <= 63 {
        1.0 / (1u64 << value) as f64
    } else {
        f64::exp2(-(value as f64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimator_initialization() {
        let est = HipEstimator::new(10); // 1024 registers

        assert_eq!(est.hip_accum(), 0.0);
        assert_eq!(est.kxq0(), 1024.0); // All zeros = 1.0 each
        assert_eq!(est.kxq1(), 0.0);
        assert!(!est.is_out_of_order());
    }

    #[test]
    fn test_estimator_update() {
        let mut est = HipEstimator::new(8); // 256 registers

        // Update from 0 to 10
        est.update(8, 0, 10);

        // HIP should have increased
        assert!(est.hip_accum() > 0.0);

        // kxq0 should have changed (10 < 32)
        assert!(est.kxq0() < 256.0);
        assert_eq!(est.kxq1(), 0.0); // kxq1 unchanged
    }

    #[test]
    fn test_kxq_split() {
        let mut est = HipEstimator::new(8);

        // Update to value < 32 (goes to kxq0)
        est.update(8, 0, 10);
        let kxq0_after_10 = est.kxq0();
        let kxq1_after_10 = est.kxq1();

        assert!(kxq0_after_10 < 256.0);
        assert_eq!(kxq1_after_10, 0.0);

        // Update from 10 to 50 (crosses the 32 boundary)
        est.update(8, 10, 50);
        let kxq0_after_50 = est.kxq0();
        let kxq1_after_50 = est.kxq1();

        assert!(kxq0_after_50 < kxq0_after_10); // Removed 1/2^10 from kxq0 (decreases kxq0)
        assert!(kxq1_after_50 > 0.0); // Added 1/2^50 to kxq1
    }

    #[test]
    fn test_out_of_order_flag() {
        let mut est = HipEstimator::new(10);

        // Normal update
        est.update(8, 0, 5);
        let hip_normal = est.hip_accum();
        assert!(hip_normal > 0.0);

        // Set out-of-order
        est.set_out_of_order(true);
        assert!(est.is_out_of_order());
        assert_eq!(est.hip_accum(), 0.0); // HIP invalidated

        // Update while OOO - HIP should not change, but kxq should
        let kxq0_before = est.kxq0();
        est.update(8, 5, 10);
        assert_eq!(est.hip_accum(), 0.0); // HIP still 0
        assert_ne!(est.kxq0(), kxq0_before); // kxq changed
    }

    #[test]
    fn test_setters() {
        let mut est = HipEstimator::new(10);

        est.set_hip_accum(123.45);
        est.set_kxq0(678.9);
        est.set_kxq1(0.0012);

        assert_eq!(est.hip_accum(), 123.45);
        assert_eq!(est.kxq0(), 678.9);
        assert_eq!(est.kxq1(), 0.0012);
    }
}
