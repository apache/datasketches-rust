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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::common::NumStdDev;
use crate::error::Error;
use crate::hll::Coupon;
use crate::hll::HllType;
use crate::hll::estimator::EstimateState;
use crate::hll::estimator::Estimator;
use crate::hll::serialization::HLL_PREAMBLE_SIZE;
use crate::hll::serialization::write_hll_preamble;

const VAL_MASK_6: u16 = 0x3F;

/// Registers packed into consecutive six-bit fields.
#[derive(Debug, Clone)]
pub struct Array6 {
    lg_config_k: u8,
    bytes: Box<[u8]>,
    num_zeros: u32,
    estimator: Estimator,
}

impl Array6 {
    pub fn new(lg_config_k: u8) -> Self {
        let k = 1 << lg_config_k;
        let num_bytes = num_bytes_for_k(k);

        Self {
            lg_config_k,
            bytes: vec![0u8; num_bytes].into_boxed_slice(),
            num_zeros: k,
            estimator: Estimator::new(lg_config_k),
        }
    }

    #[inline]
    pub fn get(&self, slot: u32) -> u8 {
        let (index, shift) = register_position(slot);
        let bytes: &[u8; 2] = self.bytes[index..index + 2].try_into().unwrap();
        let word = u16::from_le_bytes(*bytes);
        u8::try_from((word >> shift) & VAL_MASK_6).unwrap()
    }

    pub fn num_registers(&self) -> usize {
        1 << self.lg_config_k
    }

    pub fn estimate_state(&self) -> EstimateState {
        self.estimator.estimate_state()
    }

    pub fn update(&mut self, coupon: Coupon) {
        let mask = (1 << self.lg_config_k) - 1;
        self.update_register(coupon.slot() & mask, coupon.value());
    }

    pub fn update_coupons(&mut self, coupons: impl Iterator<Item = Coupon>) {
        let mask = (1 << self.lg_config_k) - 1;
        for coupon in coupons {
            self.update_register(coupon.slot() & mask, coupon.value());
        }
    }

    #[inline]
    fn update_register(&mut self, slot: u32, new_value: u8) {
        let (index, shift) = register_position(slot);
        let bytes: &mut [u8; 2] = (&mut self.bytes[index..index + 2]).try_into().unwrap();
        let word = u16::from_le_bytes(*bytes);
        let old_value = u8::try_from((word >> shift) & VAL_MASK_6).unwrap();

        if new_value > old_value {
            self.estimator
                .update(self.lg_config_k, old_value, new_value);

            let word = (word & !(VAL_MASK_6 << shift)) | (u16::from(new_value) << shift);
            *bytes = word.to_le_bytes();

            if old_value == 0 {
                self.num_zeros -= 1;
            }
        }
    }

    pub fn estimate(&self) -> f64 {
        // Array6 doesn't use cur_min (always 0), so num_at_cur_min = num_zeros
        self.estimator.estimate(self.lg_config_k, 0, self.num_zeros)
    }

    pub fn composite_estimate(&self) -> f64 {
        self.estimator
            .composite_estimate(self.lg_config_k, 0, self.num_zeros)
    }

    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.estimator
            .upper_bound(self.lg_config_k, 0, self.num_zeros, num_std_dev)
    }

    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.estimator
            .lower_bound(self.lg_config_k, 0, self.num_zeros, num_std_dev)
    }

    pub fn restore_estimate_state(&mut self, state: EstimateState) {
        self.estimator.restore_estimate_state(state);
    }

    pub fn is_empty(&self) -> bool {
        self.num_zeros == (1 << self.lg_config_k)
    }

    pub fn deserialize_registers(
        mut cursor: SketchSlice,
        lg_config_k: u8,
        ooo: bool,
    ) -> Result<Self, Error> {
        let k = 1 << lg_config_k;
        let num_bytes = num_bytes_for_k(k);

        let hip_accum = cursor
            .read_f64_le()
            .map_err(insufficient_data("hip_accum"))?;
        let kxq0 = cursor.read_f64_le().map_err(insufficient_data("kxq0"))?;
        let kxq1 = cursor.read_f64_le().map_err(insufficient_data("kxq1"))?;

        // Read num_at_cur_min (for Array6, this is num_zeros since cur_min=0)
        let num_zeros = cursor
            .read_u32_le()
            .map_err(insufficient_data("num_zeros"))?;
        let aux_count = cursor
            .read_u32_le()
            .map_err(insufficient_data("aux_count"))?;
        if num_zeros > k || aux_count != 0 {
            return Err(Error::deserial(
                "HLL6 zero count must not exceed k and auxiliary count must be zero",
            ));
        }
        let available_bytes = cursor.remaining().len();
        if available_bytes < num_bytes {
            return Err(Error::insufficient_data_of(
                "HLL6 payload",
                format_args!("expected {num_bytes} bytes, got {available_bytes}"),
            ));
        }

        let mut data = vec![0u8; num_bytes];
        cursor
            .read_exact(&mut data)
            .map_err(insufficient_data("data"))?;

        let estimator = Estimator::from_serialized(hip_accum, kxq0, kxq1, ooo);

        Ok(Self {
            lg_config_k,
            bytes: data.into_boxed_slice(),
            num_zeros,
            estimator,
        })
    }

    pub fn serialize(&self, lg_config_k: u8) -> Vec<u8> {
        let k = 1 << lg_config_k;
        let num_bytes = num_bytes_for_k(k);
        let total_size = HLL_PREAMBLE_SIZE + num_bytes;
        let mut bytes = SketchBytes::with_capacity(total_size);

        write_hll_preamble(
            &mut bytes,
            lg_config_k,
            HllType::Hll6,
            0,
            self.num_zeros,
            0,
            &self.estimator,
        );

        bytes.write(&self.bytes);

        bytes.into_bytes()
    }

    /// Returns an estimate of memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.bytes.len()
    }
}

#[inline]
fn register_position(slot: u32) -> (usize, u32) {
    let bit = slot * 6;
    (usize::try_from(bit >> 3).unwrap(), bit & 7)
}

pub fn num_bytes_for_k(k: u32) -> usize {
    // The final register's 16-bit read/write window needs one padding byte.
    (((k * 3) >> 2) + 1) as usize
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
    fn test_num_bytes_calculation() {
        assert_eq!(num_bytes_for_k(16), (16 * 3 / 4) + 1);

        assert_eq!(num_bytes_for_k(1024), (1024 * 3 / 4) + 1);
    }

    #[test]
    fn test_update_preserves_neighboring_registers() {
        let mut arr = Array6::new(4);
        arr.update(Coupon::pack(13, 63));
        arr.update(Coupon::pack(15, 51));
        arr.update(Coupon::pack(14, 42));
        arr.update(Coupon::pack(14, 48));

        assert_eq!(arr.get(13), 63);
        assert_eq!(arr.get(14), 48);
        assert_eq!(arr.get(15), 51);
    }

    #[test]
    fn test_hip_estimator() {
        let mut arr = Array6::new(10); // 1024 buckets

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
    fn test_full_range() {
        let mut arr = Array6::new(6); // 64 slots

        for val in 0..64u8 {
            arr.update(Coupon::pack(u32::from(val), val));
        }

        for val in 0..64u8 {
            assert_eq!(arr.get(u32::from(val)), val);
        }
    }

    #[test]
    fn test_kxq_register_split() {
        let mut arr = Array6::new(8); // 256 buckets

        arr.update(Coupon::pack(0, 10)); // value < 32, goes to kxq0
        arr.update(Coupon::pack(1, 40)); // value >= 32, goes to kxq1

        // Initial kxq0 = 256 (all zeros = 1.0 each)
        assert_that!(arr.estimator.kxq0(), lt(256.0));

        // kxq1 should have a small positive value (from 1/2^40)
        assert_that!(arr.estimator.kxq1(), gt(0.0));
        assert_that!(arr.estimator.kxq1(), lt(0.001));
    }
}
