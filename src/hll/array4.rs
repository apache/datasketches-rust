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

//! HyperLogLog Array4 mode - 4-bit packed representation with exception handling
//!
//! Array4 stores HLL register values using 4 bits per slot (2 slots per byte).
//! When values exceed 4 bits after cur_min offset, they're stored in an auxiliary hash map.

use super::aux_map::AuxMap;
use crate::hll::estimator::HipEstimator;
use crate::hll::{get_slot, get_value};

const AUX_TOKEN: u8 = 15;

/// Core Array4 data structure - stores 4-bit values efficiently
#[derive(Debug, Clone)]
pub struct Array4 {
    lg_config_k: u8,
    /// Packed 4-bit values: 2 values per byte
    /// Even slots use low nibble, odd slots use high nibble
    bytes: Box<[u8]>,
    /// Current minimum value offset (optimization to delay aux map creation)
    cur_min: u8,
    /// Count of slots at exactly cur_min (when 0, increment cur_min)
    num_at_cur_min: u32,
    /// Exception table for values >= 15 after cur_min offset
    aux_map: Option<AuxMap>,
    /// HIP estimator for cardinality estimation
    estimator: HipEstimator,
}

impl PartialEq for Array4 {
    fn eq(&self, other: &Self) -> bool {
        self.lg_config_k == other.lg_config_k
            && self.cur_min == other.cur_min
            && self.num_at_cur_min == other.num_at_cur_min
            && self.bytes.as_ref() == other.bytes.as_ref()
            && self.aux_map == other.aux_map
            && self.estimator == other.estimator
    }
}

impl Array4 {
    pub fn new(lg_config_k: u8) -> Self {
        let num_bytes = 1 << (lg_config_k - 1);
        let num_at_cur_min = 1 << lg_config_k;
        Self {
            lg_config_k,
            bytes: vec![0u8; num_bytes].into_boxed_slice(),
            cur_min: 0,
            num_at_cur_min,
            aux_map: None,
            estimator: HipEstimator::new(lg_config_k),
        }
    }

    /// Get raw 4-bit value from slot (not adjusted for cur_min)
    #[inline]
    fn get_raw(&self, slot: u32) -> u8 {
        debug_assert!(slot >> 1 < self.bytes.len() as u32);

        let byte = self.bytes[(slot >> 1) as usize];
        if slot & 1 == 0 {
            byte & 15 // low nibble for even slots
        } else {
            byte >> 4 // high nibble for odd slots
        }
    }

    /// Set raw 4-bit value in slot
    #[inline]
    fn put_raw(&mut self, slot: u32, value: u8) {
        debug_assert!(value <= AUX_TOKEN);
        debug_assert!(slot >> 1 < self.bytes.len() as u32);

        let byte_idx = (slot >> 1) as usize;
        let old_byte = self.bytes[byte_idx];
        self.bytes[byte_idx] = if slot & 1 == 0 {
            (old_byte & 0xF0) | (value & 0x0F) // set low nibble
        } else {
            (old_byte & 0x0F) | (value << 4) // set high nibble
        };
    }

    pub fn update(&mut self, coupon: u32) {
        let mask = (1 << self.lg_config_k) - 1;
        let slot = get_slot(coupon) & mask;
        let new_value = get_value(coupon);

        // Quick rejection: if new value <= cur_min, no update needed
        if new_value <= self.cur_min {
            return;
        }

        let raw_stored = self.get_raw(slot);
        let lower_bound = raw_stored + self.cur_min;

        if new_value <= lower_bound {
            return;
        }

        // Get actual old value (might be in aux map)
        let old_value = if raw_stored < AUX_TOKEN {
            lower_bound
        } else {
            self.aux_map
                .as_ref()
                .expect("AUX_TOKEN without aux_map")
                .get(slot)
                .expect("AUX_TOKEN but slot not in aux_map")
        };

        if new_value <= old_value {
            return;
        }

        // Update HIP and KxQ registers via estimator
        self.estimator
            .update(self.lg_config_k, old_value, new_value);

        let shifted_new = new_value - self.cur_min;

        // Four cases based on old/new exception status
        match (raw_stored, shifted_new) {
            // Case 1: Both old and new are exceptions
            (AUX_TOKEN, shifted) if shifted >= AUX_TOKEN => {
                self.aux_map.as_mut().unwrap().replace(slot, new_value);
            }
            // Case 2: Old is exception, new is not (impossible without cur_min change)
            (AUX_TOKEN, _) => {
                unreachable!("AUX_TOKEN present with non-exception new value");
            }
            // Case 3: Old not exception, new is exception
            (_, shifted) if shifted >= AUX_TOKEN => {
                self.put_raw(slot, AUX_TOKEN);
                let aux = self
                    .aux_map
                    .get_or_insert_with(|| AuxMap::new(self.lg_config_k));
                aux.insert(slot, new_value);
            }
            // Case 4: Neither is exception
            _ => {
                self.put_raw(slot, shifted_new);
            }
        }

        // Handle cur_min adjustment
        if old_value == self.cur_min {
            self.num_at_cur_min -= 1;
            while self.num_at_cur_min == 0 {
                self.shift_to_bigger_cur_min();
            }
        }
    }

    /// Increment cur_min and adjust all values
    ///
    /// This is called when no slots remain at cur_min value.
    /// All stored values are decremented by 1, and exceptions
    /// that fall back into the 4-bit range are moved from aux map.
    fn shift_to_bigger_cur_min(&mut self) {
        let new_cur_min = self.cur_min + 1;
        let k = 1 << self.lg_config_k;
        let mut num_at_new = 0;

        // Decrement all stored values in the main array
        for slot in 0..k {
            let raw = self.get_raw(slot);
            debug_assert_ne!(raw, 0, "value cannot be 0 when shifting cur_min");
            if raw < AUX_TOKEN {
                let decremented = raw - 1;
                self.put_raw(slot, decremented);
                if decremented == 0 {
                    num_at_new += 1;
                }
            }
        }

        // Rebuild aux map: some exceptions may no longer be exceptions
        if let Some(old_aux) = self.aux_map.take() {
            let mut new_aux = None;

            for (slot, old_actual_val) in old_aux.into_iter() {
                debug_assert_ne!(
                    self.get_raw(slot),
                    AUX_TOKEN,
                    "AuxMap contains slow without AUX_TOKEN"
                );

                let new_shifted = old_actual_val - new_cur_min;

                if new_shifted < AUX_TOKEN {
                    self.put_raw(slot, new_shifted);
                } else {
                    // Still an exception
                    let aux = new_aux.get_or_insert_with(|| AuxMap::new(self.lg_config_k));
                    aux.insert(slot, old_actual_val);
                }
            }
            self.aux_map = new_aux;
        }

        self.cur_min = new_cur_min;
        self.num_at_cur_min = num_at_new;
    }

    /// Get the current cardinality estimate using HIP estimator
    pub fn estimate(&self) -> f64 {
        // Array4 tracks cur_min and num_at_cur_min dynamically
        self.estimator
            .estimate(self.lg_config_k, self.cur_min, self.num_at_cur_min)
    }

    /// Get upper bound for cardinality estimate
    pub fn upper_bound(&self, num_std_dev: u8) -> f64 {
        self.estimator.upper_bound(
            self.lg_config_k,
            self.cur_min,
            self.num_at_cur_min,
            num_std_dev,
        )
    }

    /// Get lower bound for cardinality estimate
    pub fn lower_bound(&self, num_std_dev: u8) -> f64 {
        self.estimator.lower_bound(
            self.lg_config_k,
            self.cur_min,
            self.num_at_cur_min,
            num_std_dev,
        )
    }

    /// Set the HIP accumulator value
    ///
    /// This is used when promoting from coupon modes to carry forward the estimate
    pub fn set_hip_accum(&mut self, value: f64) {
        self.estimator.set_hip_accum(value);
    }

    /// Deserialize Array4 from HLL mode bytes
    ///
    /// Expects full HLL preamble (40 bytes) followed by packed 4-bit data and optional aux map.
    pub fn deserialize(
        bytes: &[u8],
        lg_config_k: u8,
        compact: bool,
        ooo: bool,
    ) -> crate::error::SerdeResult<Self> {
        use crate::error::SerdeError;
        use crate::hll::serialization::*;
        use crate::hll::{get_slot, get_value};

        let num_bytes = 1 << (lg_config_k - 1); // k/2 bytes for 4-bit packing

        // Read cur_min from header
        let cur_min = bytes[HLL_CUR_MIN_BYTE];

        // Read HIP estimator values from preamble
        let hip_accum = read_f64_le(bytes, HIP_ACCUM_DOUBLE);
        let kxq0 = read_f64_le(bytes, KXQ0_DOUBLE);
        let kxq1 = read_f64_le(bytes, KXQ1_DOUBLE);

        // Read num_at_cur_min and aux_count
        let num_at_cur_min = read_u32_le(bytes, CUR_MIN_COUNT_INT);
        let aux_count = read_u32_le(bytes, AUX_COUNT_INT);

        // Calculate expected length
        let expected_len = if compact {
            HLL_PREAMBLE_SIZE // Just preamble for compact empty sketch
        } else {
            HLL_PREAMBLE_SIZE + num_bytes + (aux_count as usize * COUPON_SIZE_BYTES)
        };

        if bytes.len() < expected_len {
            return Err(SerdeError::InsufficientData(format!(
                "expected {}, got {}",
                expected_len,
                bytes.len()
            )));
        }

        // Read packed 4-bit byte array from HLL_BYTE_ARR_START
        let mut data = vec![0u8; num_bytes];
        if !compact {
            data.copy_from_slice(&bytes[HLL_BYTE_ARR_START..HLL_BYTE_ARR_START + num_bytes]);
        }

        // Read aux map if present
        let mut aux_map = None;
        if aux_count > 0 {
            let mut aux = AuxMap::new(lg_config_k);
            let aux_start = HLL_BYTE_ARR_START + num_bytes;

            for i in 0..aux_count {
                let offset = aux_start + (i as usize * COUPON_SIZE_BYTES);
                let coupon = read_u32_le(bytes, offset);
                let slot = get_slot(coupon) & ((1 << lg_config_k) - 1);
                let value = get_value(coupon);
                aux.insert(slot, value);
            }
            aux_map = Some(aux);
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
            cur_min,
            num_at_cur_min,
            aux_map,
            estimator,
        })
    }

    /// Serialize Array4 to bytes
    ///
    /// Produces full HLL preamble (40 bytes) followed by packed 4-bit data and optional aux map.
    pub fn serialize(&self, lg_config_k: u8) -> crate::error::SerdeResult<Vec<u8>> {
        use crate::hll::pack_coupon;
        use crate::hll::serialization::*;

        let num_bytes = 1 << (lg_config_k - 1); // k/2 bytes for 4-bit packing

        // Collect aux map entries if present
        let aux_entries: Vec<(u32, u8)> = if let Some(aux) = &self.aux_map {
            aux.iter().collect()
        } else {
            vec![]
        };

        let aux_count = aux_entries.len() as u32;
        let total_size = HLL_PREAMBLE_SIZE + num_bytes + (aux_count as usize * COUPON_SIZE_BYTES);
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

        // Write cur_min
        bytes[HLL_CUR_MIN_BYTE] = self.cur_min;

        // Mode byte: HLL mode with HLL4 type
        bytes[MODE_BYTE] = encode_mode_byte(CUR_MODE_HLL, TGT_HLL4);

        // Write HIP estimator values
        write_f64_le(&mut bytes, HIP_ACCUM_DOUBLE, self.estimator.hip_accum());
        write_f64_le(&mut bytes, KXQ0_DOUBLE, self.estimator.kxq0());
        write_f64_le(&mut bytes, KXQ1_DOUBLE, self.estimator.kxq1());

        // Write num_at_cur_min
        write_u32_le(&mut bytes, CUR_MIN_COUNT_INT, self.num_at_cur_min);

        // Write aux_count
        write_u32_le(&mut bytes, AUX_COUNT_INT, aux_count);

        // Write packed 4-bit byte array
        bytes[HLL_BYTE_ARR_START..HLL_BYTE_ARR_START + num_bytes].copy_from_slice(&self.bytes);

        // Write aux map entries if present
        let aux_start = HLL_BYTE_ARR_START + num_bytes;
        for (i, (slot, value)) in aux_entries.iter().enumerate() {
            let offset = aux_start + (i * COUPON_SIZE_BYTES);
            let coupon = pack_coupon(*slot, *value);
            write_u32_le(&mut bytes, offset, coupon);
        }

        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hll::{coupon, pack_coupon};

    #[test]
    fn test_get_set_raw() {
        let mut data = Array4::new(4); // 16 buckets

        // Test even slot (low nibble)
        data.put_raw(0, 5);
        assert_eq!(data.get_raw(0), 5);

        // Test odd slot (high nibble)
        data.put_raw(1, 7);
        assert_eq!(data.get_raw(1), 7);

        // Both values should be stored in the same byte
        assert_eq!(data.bytes[0], 0x75); // 0111_0101 = 7 << 4 | 5

        // Test multiple slots
        data.put_raw(2, 15);
        data.put_raw(3, 3);
        assert_eq!(data.get_raw(2), 15);
        assert_eq!(data.get_raw(3), 3);
    }

    #[test]
    fn test_hip_estimator_basic() {
        let mut arr = Array4::new(10); // 1024 buckets

        // Initially estimate should be 0
        assert_eq!(arr.estimate(), 0.0);

        // Add some unique values to different slots
        for i in 0..10_000u32 {
            let coupon = coupon(i);
            arr.update(coupon);
        }

        // Estimate should be positive and roughly in the ballpark
        // (not exact, but should be non-zero and not NaN/Inf)
        let estimate = arr.estimate();

        assert!(estimate > 0.0, "Estimate should be positive");
        assert!(estimate.is_finite(), "Estimate should be finite");
        assert!(estimate < 100_000.0, "Estimate should be reasonable");

        // Rough sanity check: with 100 updates to different slots,
        // estimate should be in a reasonable range (very loose bounds)
        assert!(
            estimate > 1_000.0,
            "Estimate seems too low for 10_000 updates"
        );
        assert!(
            estimate < 100_000.0,
            "Estimate seems too high for 10_000 updates"
        );
    }

    #[test]
    fn test_kxq_register_split() {
        let mut arr = Array4::new(8); // 256 buckets

        // Test that values < 32 and >= 32 are handled correctly
        arr.update(pack_coupon(0, 10)); // value < 32, goes to kxq0
        arr.update(pack_coupon(1, 40)); // value >= 32, goes to kxq1

        // Verify registers were updated (not exact values, just check they changed)
        // kxq0 should have decreased (we removed a 0 and added a 10)
        // Initial kxq0 = 256 (all zeros = 1.0 each)
        assert!(arr.estimator.kxq0() < 256.0, "kxq0 should have decreased");

        // kxq1 should have a small positive value (from 1/2^40)
        assert!(arr.estimator.kxq1() > 0.0, "kxq1 should be positive");
        assert!(
            arr.estimator.kxq1() < 0.001,
            "kxq1 should be small (1/2^40 is tiny)"
        );
    }
}
