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

use std::sync::OnceLock;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::common::NumStdDev;
use crate::error::Error;
use crate::hll::Coupon;
use crate::hll::estimator::EstimateState;
use crate::hll::estimator::Estimator;
use crate::hll::serialization::CUR_MODE_HLL;
use crate::hll::serialization::HLL_PREAMBLE_SIZE;
use crate::hll::serialization::HLL_PREINTS;
use crate::hll::serialization::OUT_OF_ORDER_FLAG_MASK;
use crate::hll::serialization::SERIAL_VERSION;
use crate::hll::serialization::TGT_HLL8;
use crate::hll::serialization::encode_mode_byte;

/// Registers stored one per byte.
#[derive(Debug, Clone)]
pub struct Array8 {
    lg_config_k: u8,
    bytes: Box<[u8]>,
    /// An absent cache means a bulk merge invalidated HIP and the register-derived fields.
    /// Shared queries initialize it once; exclusive updates can maintain or invalidate it.
    cache: OnceLock<CachedState>,
}

#[derive(Debug, Clone, PartialEq)]
struct CachedState {
    num_zeros: u32,
    estimator: Estimator,
}

impl PartialEq for Array8 {
    fn eq(&self, other: &Self) -> bool {
        self.lg_config_k == other.lg_config_k
            && self.bytes == other.bytes
            && self.cached_state() == other.cached_state()
    }
}

impl Array8 {
    pub fn new(lg_config_k: u8) -> Self {
        let k = 1 << lg_config_k;

        Self {
            lg_config_k,
            bytes: vec![0u8; k as usize].into_boxed_slice(),
            cache: OnceLock::from(CachedState {
                num_zeros: k,
                estimator: Estimator::new(lg_config_k),
            }),
        }
    }

    #[inline]
    pub fn get(&self, slot: u32) -> u8 {
        self.bytes[slot as usize]
    }

    #[inline]
    fn put(&mut self, slot: u32, value: u8) {
        self.bytes[slot as usize] = value;
    }

    pub fn update(&mut self, coupon: Coupon) {
        let mask = (1 << self.lg_config_k) - 1;
        let slot = coupon.slot() & mask;
        let new_value = coupon.value();

        let old_value = self.get(slot);

        if new_value > old_value {
            if let Some(cache) = self.cache.get_mut() {
                cache
                    .estimator
                    .update(self.lg_config_k, old_value, new_value);
                if old_value == 0 {
                    cache.num_zeros -= 1;
                }
            }
            self.put(slot, new_value);
        }
    }

    pub fn estimate(&self) -> f64 {
        let cache = self.cached_state();
        // Array8 doesn't use cur_min (always 0), so num_at_cur_min = num_zeros
        cache
            .estimator
            .estimate(self.lg_config_k, 0, cache.num_zeros)
    }

    pub fn composite_estimate(&self) -> f64 {
        let cache = self.cached_state();
        cache
            .estimator
            .composite_estimate(self.lg_config_k, 0, cache.num_zeros)
    }

    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        let cache = self.cached_state();
        cache
            .estimator
            .upper_bound(self.lg_config_k, 0, cache.num_zeros, num_std_dev)
    }

    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        let cache = self.cached_state();
        cache
            .estimator
            .lower_bound(self.lg_config_k, 0, cache.num_zeros, num_std_dev)
    }

    pub fn is_empty(&self) -> bool {
        if let Some(cache) = self.cache.get() {
            return cache.num_zeros == (1 << self.lg_config_k);
        }
        // Union checks emptiness between merges. A nonzero register answers that without
        // rebuilding all estimator fields and defeating deferred reconstruction.
        self.bytes.iter().all(|&value| value == 0)
    }

    pub fn values(&self) -> &[u8] {
        &self.bytes
    }

    pub fn num_registers(&self) -> usize {
        1 << self.lg_config_k
    }

    pub fn estimate_state(&self) -> EstimateState {
        self.cache.get().map_or(EstimateState::Composite, |cache| {
            cache.estimator.estimate_state()
        })
    }

    pub fn restore_estimate_state(&mut self, state: EstimateState) {
        if let Some(cache) = self.cache.get_mut() {
            cache.estimator.restore_estimate_state(state);
        } else if let EstimateState::Hip(_) = state {
            let mut cache = self.rebuild_cached_values();
            cache.estimator.restore_estimate_state(state);
            self.cache = OnceLock::from(cache);
        }
    }

    /// Bypasses estimator updates. Call `invalidate_estimator` after all writes.
    pub fn set_register(&mut self, slot: usize, value: u8) {
        self.bytes[slot] = value;
    }

    /// Invalidates HIP and defers rebuilding register-derived fields until a query needs them.
    pub fn invalidate_estimator(&mut self) {
        self.cache.take();
    }

    /// Merges registers and invalidates HIP because their update order is unavailable.
    pub fn merge_array_same_lgk(&mut self, src: &[u8]) {
        assert_eq!(
            src.len(),
            self.bytes.len(),
            "Source and destination must have same lg_k"
        );

        for (i, &val) in src.iter().enumerate() {
            self.bytes[i] = self.bytes[i].max(val);
        }

        self.invalidate_estimator();
    }

    /// Register values come from a separate hash word, so folding slot indices needs no
    /// adjustment to the values. Merging independent histories invalidates HIP.
    pub fn merge_array_with_downsample(&mut self, src: &[u8], src_lg_k: u8) {
        assert!(
            src_lg_k > self.lg_config_k,
            "Source lg_k must be greater than destination lg_k for downsampling"
        );
        assert_eq!(
            src.len(),
            1 << src_lg_k,
            "Source length must match 2^src_lg_k"
        );

        let dst_mask = (1 << self.lg_config_k) - 1;

        for (src_slot, &val) in src.iter().enumerate() {
            let dst_slot = (src_slot as u32 & dst_mask) as usize;
            self.bytes[dst_slot] = self.bytes[dst_slot].max(val);
        }

        self.invalidate_estimator();
    }

    fn cached_state(&self) -> &CachedState {
        self.cache.get_or_init(|| self.rebuild_cached_values())
    }

    fn rebuild_cached_values(&self) -> CachedState {
        let num_zeros = self.bytes.iter().filter(|&&v| v == 0).count() as u32;

        let mut kxq0_sum = 0.0;
        let mut kxq1_sum = 0.0;

        for &val in self.bytes.iter() {
            if val == 0 {
                kxq0_sum += 1.0;
            } else if val < 32 {
                kxq0_sum += 1.0 / (1u64 << val) as f64;
            } else {
                kxq1_sum += 1.0 / (1u64 << val) as f64;
            }
        }

        let mut estimator = Estimator::new(self.lg_config_k);
        estimator.restore_kxq(kxq0_sum, kxq1_sum);
        estimator.invalidate_hip();
        CachedState {
            num_zeros,
            estimator,
        }
    }

    pub fn deserialize_registers(
        mut cursor: SketchSlice,
        lg_config_k: u8,
        ooo: bool,
    ) -> Result<Self, Error> {
        let k = 1usize << lg_config_k;

        let hip_accum = cursor
            .read_f64_le()
            .map_err(insufficient_data("hip_accum"))?;
        let kxq0 = cursor.read_f64_le().map_err(insufficient_data("kxq0"))?;
        let kxq1 = cursor.read_f64_le().map_err(insufficient_data("kxq1"))?;

        // Read num_at_cur_min (for Array8, this is num_zeros since cur_min=0)
        let num_zeros = cursor
            .read_u32_le()
            .map_err(insufficient_data("num_zeros"))?;
        let aux_count = cursor
            .read_u32_le()
            .map_err(insufficient_data("aux_count"))?;
        if num_zeros as usize > k || aux_count != 0 {
            return Err(Error::deserial(
                "HLL8 zero count must not exceed k and auxiliary count must be zero",
            ));
        }
        let available_bytes = cursor.remaining().len();
        if available_bytes < k {
            return Err(Error::insufficient_data_of(
                "HLL8 payload",
                format_args!("expected {k} bytes, got {available_bytes}"),
            ));
        }

        let mut data = vec![0u8; k];
        cursor
            .read_exact(&mut data)
            .map_err(insufficient_data("data"))?;

        let estimator = Estimator::from_serialized(hip_accum, kxq0, kxq1, ooo);

        Ok(Self {
            lg_config_k,
            bytes: data.into_boxed_slice(),
            cache: OnceLock::from(CachedState {
                num_zeros,
                estimator,
            }),
        })
    }

    pub fn serialize(&self, lg_config_k: u8) -> Vec<u8> {
        let cache = self.cached_state();
        let k = 1 << lg_config_k;
        let total_size = HLL_PREAMBLE_SIZE + k as usize;
        let mut bytes = SketchBytes::with_capacity(total_size);

        bytes.write_u8(HLL_PREINTS);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::HLL.id);
        bytes.write_u8(lg_config_k);
        bytes.write_u8(0); // unused for HLL mode

        let mut flags = 0u8;
        if cache.estimator.uses_composite_estimate() {
            flags |= OUT_OF_ORDER_FLAG_MASK;
        }
        bytes.write_u8(flags);

        // cur_min is always 0 for Array8
        bytes.write_u8(0);

        bytes.write_u8(encode_mode_byte(CUR_MODE_HLL, TGT_HLL8));

        bytes.write_f64_le(cache.estimator.hip_accum());
        bytes.write_f64_le(cache.estimator.kxq0());
        bytes.write_f64_le(cache.estimator.kxq1());

        // Write num_at_cur_min (num_zeros for Array8)
        bytes.write_u32_le(cache.num_zeros);

        // Write aux_count (always 0 for Array8)
        bytes.write_u32_le(0);

        bytes.write(&self.bytes);

        bytes.into_bytes()
    }

    /// Returns an estimate of memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use googletest::assert_that;
    use googletest::prelude::gt;
    use googletest::prelude::is_finite;
    use googletest::prelude::lt;

    use super::*;
    use crate::hll::Coupon;

    #[test]
    fn test_array8_basic() {
        let arr = Array8::new(10); // 1024 buckets

        assert_eq!(arr.get(0), 0);
        assert_eq!(arr.get(100), 0);
        assert_eq!(arr.get(1023), 0);
    }

    #[test]
    fn test_get_set() {
        let mut arr = Array8::new(4); // 16 slots

        for slot in 0..16 {
            arr.put(slot, (slot * 17) as u8);
        }

        for slot in 0..16 {
            assert_eq!(arr.get(slot), (slot * 17) as u8);
        }

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

        arr.update(Coupon::pack(0, 5));
        assert_eq!(arr.get(0), 5);

        arr.update(Coupon::pack(0, 3));
        assert_eq!(arr.get(0), 5);

        arr.update(Coupon::pack(0, 42));
        assert_eq!(arr.get(0), 42);

        arr.update(Coupon::pack(1, 63));
        assert_eq!(arr.get(1), 63);
    }

    #[test]
    fn test_hip_estimator() {
        let mut arr = Array8::new(10); // 1024 buckets

        assert_eq!(arr.estimate(), 0.0);

        for i in 0..10_000u32 {
            arr.update(Coupon::from_value(i));
        }

        let estimate = arr.estimate();

        assert_that!(estimate, gt(0.0));
        assert_that!(estimate, is_finite());

        assert_that!(estimate, gt(1_000.0));
        assert_that!(estimate, lt(100_000.0));
    }

    #[test]
    fn test_full_value_range() {
        let mut arr = Array8::new(8); // 256 slots

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

        // Direct storage accepts byte values beyond the six-bit coupon range.
        let test_values = [16, 32, 64, 128, 200, 255];

        for (slot, &value) in test_values.iter().enumerate() {
            arr.put(slot as u32, value);
            assert_eq!(arr.get(slot as u32), value);
        }

        for (slot, &value) in test_values.iter().enumerate() {
            assert_eq!(arr.get(slot as u32), value);
        }
    }

    #[test]
    fn test_kxq_register_split() {
        let mut arr = Array8::new(8); // 256 buckets

        arr.update(Coupon::pack(0, 10)); // value < 32, goes to kxq0
        arr.update(Coupon::pack(1, 50)); // value >= 32, goes to kxq1

        // Initial kxq0 = 256 (all zeros = 1.0 each)
        assert_that!(arr.cached_state().estimator.kxq0(), lt(256.0));

        // kxq1 should have a positive value (from 1/2^50)
        assert_that!(arr.cached_state().estimator.kxq1(), gt(0.0));
        assert_that!(arr.cached_state().estimator.kxq1(), lt(1e-10));
    }

    #[test]
    fn test_values_access() {
        let mut arr = Array8::new(4); // 16 slots

        arr.put(0, 10);
        arr.put(5, 25);
        arr.put(15, 63);

        let vals = arr.values();
        assert_eq!(vals.len(), 16);
        assert_eq!(vals[0], 10);
        assert_eq!(vals[5], 25);
        assert_eq!(vals[15], 63);
        assert_eq!(vals[1], 0); // Untouched slot
    }

    #[test]
    fn test_merge_array_same_lgk() {
        let mut dst = Array8::new(4); // 16 slots
        let mut src = Array8::new(4); // 16 slots

        dst.put(0, 10);
        dst.put(1, 20);
        dst.put(2, 30);

        src.put(1, 15); // Smaller than dst[1]=20, should keep 20
        src.put(2, 35); // Larger than dst[2]=30, should update to 35
        src.put(3, 40); // New value

        dst.merge_array_same_lgk(src.values());

        assert_eq!(dst.get(0), 10, "dst[0] unchanged");
        assert_eq!(dst.get(1), 20, "dst[1] kept max value");
        assert_eq!(dst.get(2), 35, "dst[2] updated to larger value");
        assert_eq!(dst.get(3), 40, "dst[3] got new value");

        // Bulk merges require composite estimation.
        assert!(dst.cached_state().estimator.uses_composite_estimate());

        assert_eq!(dst.cached_state().num_zeros, 12);
    }

    #[test]
    fn test_merge_array_with_downsample() {
        // Downsampling from lg_k=5 (32 slots) to lg_k=4 (16 slots)
        let mut dst = Array8::new(4); // 16 slots
        let mut src = Array8::new(5); // 32 slots

        dst.put(0, 10);
        dst.put(1, 20);

        // Set up src - slots 0 and 16 both map to dst slot 0
        src.put(0, 15); // maps to dst[0], max(10, 15) = 15
        src.put(16, 25); // maps to dst[0], max(15, 25) = 25
        src.put(1, 18); // maps to dst[1], max(20, 18) = 20
        src.put(17, 30); // maps to dst[1], max(20, 30) = 30

        dst.merge_array_with_downsample(src.values(), 5);

        assert_eq!(dst.get(0), 25, "dst[0] = max(10, 15, 25)");
        assert_eq!(dst.get(1), 30, "dst[1] = max(20, 18, 30)");

        // Bulk merges require composite estimation.
        assert!(dst.cached_state().estimator.uses_composite_estimate());
    }

    #[test]
    #[should_panic(expected = "Source and destination must have same lg_k")]
    fn test_merge_same_lgk_panics_on_size_mismatch() {
        let mut dst = Array8::new(4); // 16 slots
        let src = Array8::new(5); // 32 slots - wrong size!

        dst.merge_array_same_lgk(src.values());
    }

    #[test]
    #[should_panic(expected = "Source lg_k must be greater")]
    fn test_merge_downsample_panics_if_not_downsampling() {
        let mut dst = Array8::new(5); // 32 slots
        let src = Array8::new(4); // 16 slots - can't upsample!

        dst.merge_array_with_downsample(src.values(), 4);
    }

    #[test]
    fn test_rebuild_cached_values() {
        let mut arr = Array8::new(4); // 16 slots

        arr.put(0, 10);
        arr.put(1, 20);
        arr.put(2, 30);

        arr.put(3, 31);
        arr.put(4, 32);
        arr.put(5, 63);
        arr.invalidate_estimator();

        let cache = arr.cached_state();
        assert_eq!(cache.num_zeros, 10);
        assert_eq!(
            cache.estimator.kxq0(),
            10.0 + 2_f64.powi(-10) + 2_f64.powi(-20) + 2_f64.powi(-30) + 2_f64.powi(-31)
        );
        assert_eq!(cache.estimator.kxq1(), 2_f64.powi(-32) + 2_f64.powi(-63));
    }

    #[test]
    fn test_merge_preserves_max_semantics() {
        let mut dst = Array8::new(4);
        let mut src = Array8::new(4);

        for i in 0..16 {
            dst.put(i, i as u8);
        }

        for i in 0..16 {
            src.put(i, (15 - i) as u8);
        }

        dst.merge_array_same_lgk(src.values());

        for i in 0..16 {
            let expected = (i as u8).max((15 - i) as u8);
            assert_eq!(
                dst.get(i),
                expected,
                "slot {} should be max({}, {}) = {}",
                i,
                i,
                15 - i,
                expected
            );
        }
    }
}
