//! HyperLogLog Array8 mode - 8-bit (1 byte per slot) representation
//!
//! Array8 is the simplest HLL array implementation, storing one byte per slot.
//! This provides the maximum value range (0-255) with no bit-packing complexity.

use crate::hll::estimator::HipEstimator;
use crate::hll::{get_slot, get_value};

/// Core Array8 data structure - one byte per slot, no packing
pub struct Array8 {
    lg_config_k: u8,
    /// Direct byte array: bytes[slot] = value
    bytes: Box<[u8]>,
    /// Count of slots with value 0
    num_zeros: u32,
    /// HIP estimator for cardinality estimation
    estimator: HipEstimator,
}

impl Array8 {
    pub fn new(lg_config_k: u8) -> Self {
        let k = 1 << lg_config_k;

        Self {
            lg_config_k,
            bytes: vec![0u8; k as usize].into_boxed_slice(),
            num_zeros: k,
            estimator: HipEstimator::new(lg_config_k),
        }
    }

    /// Get value from a slot
    ///
    /// Direct array access - no bit manipulation required.
    #[inline]
    pub fn get(&self, slot: u32) -> u8 {
        self.bytes[slot as usize]
    }

    /// Set value in a slot
    ///
    /// Direct array write - no bit manipulation required.
    #[inline]
    fn put(&mut self, slot: u32, value: u8) {
        self.bytes[slot as usize] = value;
    }

    /// Update with a coupon
    pub fn update(&mut self, coupon: u32) {
        let mask = (1 << self.lg_config_k) - 1;
        let slot = get_slot(coupon) & mask;
        let new_value = get_value(coupon);

        let old_value = self.get(slot);

        if new_value > old_value {
            // Update HIP and KxQ registers via estimator
            self.estimator
                .update(self.lg_config_k, old_value, new_value);

            // Update the slot
            self.put(slot, new_value);

            // Track num_zeros (count of slots with value 0)
            if old_value == 0 {
                self.num_zeros -= 1;
            }
        }
    }

    /// Get the current cardinality estimate using HIP estimator
    pub fn estimate(&self) -> f64 {
        // Array8 doesn't use cur_min (always 0), so num_at_cur_min = num_zeros
        self.estimator.estimate(self.lg_config_k, 0, self.num_zeros)
    }

    /// Get the number of zero-valued slots
    pub fn num_zeros(&self) -> u32 {
        self.num_zeros
    }

    /// Get the total number of bytes used
    pub fn size_bytes(&self) -> usize {
        self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hll::{coupon, pack_coupon};

    #[test]
    fn test_array8_basic() {
        let arr = Array8::new(10); // 1024 buckets

        // Initially all slots should be 0
        assert_eq!(arr.get(0), 0);
        assert_eq!(arr.get(100), 0);
        assert_eq!(arr.get(1023), 0);
        assert_eq!(arr.num_zeros(), 1024);

        // Storage should be exactly k bytes
        assert_eq!(arr.size_bytes(), 1024);
    }

    #[test]
    fn test_storage_calculation() {
        // Array8 uses exactly k bytes
        assert_eq!(Array8::new(4).size_bytes(), 16);
        assert_eq!(Array8::new(8).size_bytes(), 256);
        assert_eq!(Array8::new(10).size_bytes(), 1024);
        assert_eq!(Array8::new(14).size_bytes(), 16384);
    }

    #[test]
    fn test_get_set() {
        let mut arr = Array8::new(4); // 16 slots

        // Test all possible 8-bit values
        for slot in 0..16 {
            arr.put(slot, (slot * 17) as u8); // Various values
        }

        for slot in 0..16 {
            assert_eq!(arr.get(slot), (slot * 17) as u8);
        }

        // Test full range (0-255)
        arr.put(0, 0);
        arr.put(1, 127);
        arr.put(2, 255);

        assert_eq!(arr.get(0), 0);
        assert_eq!(arr.get(1), 127);
        assert_eq!(arr.get(2), 255);
    }

    #[test]
    fn test_update_basic() {
        let mut arr = Array8::new(4);

        // Update slot 0 with value 5
        arr.update(pack_coupon(0, 5));
        assert_eq!(arr.get(0), 5);

        // Update with a smaller value (should be ignored)
        arr.update(pack_coupon(0, 3));
        assert_eq!(arr.get(0), 5);

        // Update with a larger value
        arr.update(pack_coupon(0, 42));
        assert_eq!(arr.get(0), 42);

        // Test value at max coupon range (63)
        // Note: pack_coupon only stores 6 bits (0-63)
        arr.update(pack_coupon(1, 63));
        assert_eq!(arr.get(1), 63);
    }

    #[test]
    fn test_num_zeros_tracking() {
        let mut arr = Array8::new(4); // 16 slots
        assert_eq!(arr.num_zeros(), 16);

        // Update one slot from 0 to non-zero
        arr.update(pack_coupon(0, 5));
        assert_eq!(arr.num_zeros(), 15);

        // Update same slot again (should not change num_zeros)
        arr.update(pack_coupon(0, 10));
        assert_eq!(arr.num_zeros(), 15);

        // Update another slot
        arr.update(pack_coupon(1, 3));
        assert_eq!(arr.num_zeros(), 14);

        // Update multiple slots to zero
        for i in 2..16 {
            arr.update(pack_coupon(i, 1));
        }
        assert_eq!(arr.num_zeros(), 0);
    }

    #[test]
    fn test_hip_estimator() {
        let mut arr = Array8::new(10); // 1024 buckets

        // Initially estimate should be 0
        assert_eq!(arr.estimate(), 0.0);

        // Add some unique values using real coupon hashing
        for i in 0..10_000u32 {
            let coupon = coupon(&mut &i.to_ne_bytes()[..]).unwrap();
            arr.update(coupon);
        }

        let estimate = arr.estimate();

        // Sanity checks
        assert!(estimate > 0.0, "Estimate should be positive");
        assert!(estimate.is_finite(), "Estimate should be finite");

        // Rough bounds for 10K unique items (very loose)
        assert!(estimate > 1_000.0, "Estimate seems too low");
        assert!(estimate < 100_000.0, "Estimate seems too high");
    }

    #[test]
    fn test_full_value_range() {
        let mut arr = Array8::new(8); // 256 slots

        // Test all possible 8-bit values (0-255)
        for val in 0..=255u8 {
            arr.put(val as u32, val);
        }

        for val in 0..=255u8 {
            assert_eq!(arr.get(val as u32), val);
        }
    }

    #[test]
    fn test_high_value_direct() {
        let mut arr = Array8::new(6); // 64 slots

        // Test that Array8 CAN store full range (0-255) directly
        // Even though coupons are limited to 6 bits (0-63)
        // Direct put/get bypasses coupon encoding
        let test_values = [16, 32, 64, 128, 200, 255];

        for (slot, &value) in test_values.iter().enumerate() {
            arr.put(slot as u32, value);
            assert_eq!(arr.get(slot as u32), value);
        }

        // Verify no cross-slot corruption
        for (slot, &value) in test_values.iter().enumerate() {
            assert_eq!(arr.get(slot as u32), value);
        }
    }

    #[test]
    fn test_kxq_register_split() {
        let mut arr = Array8::new(8); // 256 buckets

        // Test that values < 32 and >= 32 are handled correctly
        arr.update(pack_coupon(0, 10)); // value < 32, goes to kxq0
        arr.update(pack_coupon(1, 50)); // value >= 32, goes to kxq1

        // Initial kxq0 = 256 (all zeros = 1.0 each)
        assert!(arr.estimator.kxq0() < 256.0, "kxq0 should have decreased");

        // kxq1 should have a positive value (from 1/2^50)
        assert!(arr.estimator.kxq1() > 0.0, "kxq1 should be positive");
        assert!(
            arr.estimator.kxq1() < 1e-10,
            "kxq1 should be very small (1/2^50 ≈ 8.9e-16)"
        );
    }

    #[test]
    fn test_memory_comparison() {
        let lg_k = 10; // 1024 slots

        // Array4: k/2 bytes
        let array4_size = 512;

        // Array6: (k*3)/4 + 1 bytes
        let array6_size = 769;

        // Array8: k bytes
        let array8 = Array8::new(lg_k);
        assert_eq!(array8.size_bytes(), 1024);

        // Verify Array8 is largest
        assert!(array8.size_bytes() > array4_size);
        assert!(array8.size_bytes() > array6_size);

        // Array8 is 2x Array4, ~1.33x Array6
        assert_eq!(array8.size_bytes(), 2 * array4_size);
        assert!((array8.size_bytes() as f64) / (array6_size as f64) > 1.3);
        assert!((array8.size_bytes() as f64) / (array6_size as f64) < 1.4);
    }
}
