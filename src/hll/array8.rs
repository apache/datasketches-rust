// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! HyperLogLog Array8 mode - 8-bit (1 byte per slot) representation
//!
//! Array8 is the simplest HLL array implementation, storing one byte per slot.
//! This provides the maximum value range (0-255) with no bit-packing complexity.

use crate::hll::estimator::HipEstimator;
use crate::hll::{get_slot, get_value};

/// Core Array8 data structure - one byte per slot, no packing
#[derive(Debug, Clone)]
pub struct Array8 {
    lg_config_k: u8,
    /// Direct byte array: bytes[slot] = value
    bytes: Box<[u8]>,
    /// Count of slots with value 0
    num_zeros: u32,
    /// HIP estimator for cardinality estimation
    estimator: HipEstimator,
}

impl PartialEq for Array8 {
    fn eq(&self, other: &Self) -> bool {
        self.lg_config_k == other.lg_config_k
            && self.num_zeros == other.num_zeros
            && self.bytes.as_ref() == other.bytes.as_ref()
            && self.estimator == other.estimator
    }
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

    /// Get upper bound for cardinality estimate
    pub fn upper_bound(&self, num_std_dev: u8) -> f64 {
        self.estimator
            .upper_bound(self.lg_config_k, 0, self.num_zeros, num_std_dev)
    }

    /// Get lower bound for cardinality estimate
    pub fn lower_bound(&self, num_std_dev: u8) -> f64 {
        self.estimator
            .lower_bound(self.lg_config_k, 0, self.num_zeros, num_std_dev)
    }

    /// Set the HIP accumulator value
    ///
    /// This is used when promoting from coupon modes to carry forward the estimate
    pub fn set_hip_accum(&mut self, value: f64) {
        self.estimator.set_hip_accum(value);
    }

    /// Deserialize Array8 from HLL mode bytes
    ///
    /// Expects full HLL preamble (40 bytes) followed by k bytes of data.
    pub fn deserialize(
        bytes: &[u8],
        lg_config_k: u8,
        compact: bool,
        ooo: bool,
    ) -> std::io::Result<Self> {
        use std::io::{Error, ErrorKind};

        use crate::hll::serialization::*;

        let k = 1 << lg_config_k;
        let expected_len = if compact {
            HLL_PREAMBLE_SIZE // Just preamble for compact empty sketch
        } else {
            HLL_PREAMBLE_SIZE + k as usize
        };

        if bytes.len() < expected_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Array8 data too short: expected {}, got {}",
                    expected_len,
                    bytes.len()
                ),
            ));
        }

        // Read HIP estimator values from preamble
        let hip_accum = read_f64_le(bytes, HIP_ACCUM_DOUBLE);
        let kxq0 = read_f64_le(bytes, KXQ0_DOUBLE);
        let kxq1 = read_f64_le(bytes, KXQ1_DOUBLE);

        // Read num_at_cur_min (for Array8, this is num_zeros since cur_min=0)
        let num_zeros = read_u32_le(bytes, CUR_MIN_COUNT_INT);

        // Read byte array from offset HLL_BYTE_ARR_START
        let mut data = vec![0u8; k as usize];
        if !compact {
            data.copy_from_slice(&bytes[HLL_BYTE_ARR_START..HLL_BYTE_ARR_START + k as usize]);
        }

        // Create estimator and restore state
        let mut estimator = HipEstimator::new(lg_config_k);
        estimator.set_hip_accum(hip_accum);
        estimator.set_kxq0(kxq0);
        estimator.set_kxq1(kxq1);
        estimator.set_out_of_order(ooo);

        Ok(Self {
            lg_config_k,
            bytes: data.into_boxed_slice(),
            num_zeros,
            estimator,
        })
    }

    /// Serialize Array8 to bytes
    ///
    /// Produces full HLL preamble (40 bytes) followed by k bytes of data.
    pub fn serialize(&self, lg_config_k: u8) -> std::io::Result<Vec<u8>> {
        use crate::hll::serialization::*;

        let k = 1 << lg_config_k;
        let total_size = HLL_PREAMBLE_SIZE + k as usize;
        let mut bytes = vec![0u8; total_size];

        // Write standard header
        bytes[PREAMBLE_INTS_BYTE] = HLL_PREINTS;
        bytes[SER_VER_BYTE] = SER_VER;
        bytes[FAMILY_BYTE] = HLL_FAMILY_ID;
        bytes[LG_K_BYTE] = lg_config_k;
        bytes[LG_ARR_BYTE] = 0; // Not used for HLL mode

        // Write flags
        let mut flags = 0u8;
        if self.estimator.is_out_of_order() {
            flags |= OUT_OF_ORDER_FLAG_MASK;
        }
        bytes[FLAGS_BYTE] = flags;

        // cur_min is always 0 for Array8
        bytes[HLL_CUR_MIN_BYTE] = 0;

        // Mode byte: HLL mode with HLL8 type
        bytes[MODE_BYTE] = encode_mode_byte(CUR_MODE_HLL, TGT_HLL8);

        // Write HIP estimator values
        write_f64_le(&mut bytes, HIP_ACCUM_DOUBLE, self.estimator.hip_accum());
        write_f64_le(&mut bytes, KXQ0_DOUBLE, self.estimator.kxq0());
        write_f64_le(&mut bytes, KXQ1_DOUBLE, self.estimator.kxq1());

        // Write num_at_cur_min (num_zeros for Array8)
        write_u32_le(&mut bytes, CUR_MIN_COUNT_INT, self.num_zeros);

        // Write aux_count (always 0 for Array8)
        write_u32_le(&mut bytes, AUX_COUNT_INT, 0);

        // Write byte array
        bytes[HLL_BYTE_ARR_START..].copy_from_slice(&self.bytes);

        Ok(bytes)
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
    fn test_hip_estimator() {
        let mut arr = Array8::new(10); // 1024 buckets

        // Initially estimate should be 0
        assert_eq!(arr.estimate(), 0.0);

        // Add some unique values using real coupon hashing
        for i in 0..10_000u32 {
            let coupon = coupon(i);
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
}
