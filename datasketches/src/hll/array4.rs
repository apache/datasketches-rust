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
use crate::codec::family::Family;
use crate::common::NumStdDev;
use crate::error::Error;
use crate::hll::Coupon;
use crate::hll::aux_map::AuxMap;
use crate::hll::estimator::EstimateState;
use crate::hll::estimator::Estimator;
use crate::hll::serialization::COMPACT_FLAG_MASK;
use crate::hll::serialization::COUPON_SIZE_BYTES;
use crate::hll::serialization::CUR_MODE_HLL;
use crate::hll::serialization::HLL_PREAMBLE_SIZE;
use crate::hll::serialization::HLL_PREINTS;
use crate::hll::serialization::OUT_OF_ORDER_FLAG_MASK;
use crate::hll::serialization::SERIAL_VERSION;
use crate::hll::serialization::TGT_HLL4;
use crate::hll::serialization::encode_mode_byte;

const AUX_TOKEN: u8 = 15;

#[derive(Clone, Copy)]
pub enum AuxFormat {
    Compact,
    Updatable { lg_arr: u8 },
}

impl AuxFormat {
    pub fn from_header(compact: bool, lg_arr: u8) -> Self {
        if compact {
            Self::Compact
        } else {
            Self::Updatable { lg_arr }
        }
    }
}

/// Registers stored as four-bit offsets from their current minimum.
#[derive(Debug, Clone, PartialEq)]
pub struct Array4 {
    lg_config_k: u8,
    /// Even slots occupy the low nibble; odd slots occupy the high nibble.
    bytes: Box<[u8]>,
    cur_min: u8,
    /// When this reaches zero, the offset must increase until it reaches the new minimum.
    num_at_cur_min: u32,
    /// Every `AUX_TOKEN` nibble has an entry here containing the absolute register value.
    aux_map: Option<AuxMap>,
    estimator: Estimator,
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
            estimator: Estimator::new(lg_config_k),
        }
    }

    #[inline]
    fn get_raw(&self, slot: u32) -> u8 {
        debug_assert!(slot >> 1 < self.bytes.len() as u32);

        let byte = self.bytes[(slot >> 1) as usize];
        if slot & 1 == 0 { byte & 15 } else { byte >> 4 }
    }

    pub fn get(&self, slot: u32) -> u8 {
        let raw = self.get_raw(slot);

        if raw < AUX_TOKEN {
            self.cur_min + raw
        } else {
            self.aux_map
                .as_ref()
                .and_then(|map| map.get(slot))
                .unwrap_or(self.cur_min)
        }
    }

    pub fn num_registers(&self) -> usize {
        1 << self.lg_config_k
    }

    pub fn estimate_state(&self) -> EstimateState {
        self.estimator.estimate_state()
    }

    #[inline]
    fn put_raw(&mut self, slot: u32, value: u8) {
        debug_assert!(value <= AUX_TOKEN);
        debug_assert!(slot >> 1 < self.bytes.len() as u32);

        let byte_idx = (slot >> 1) as usize;
        let old_byte = self.bytes[byte_idx];
        self.bytes[byte_idx] = if slot & 1 == 0 {
            (old_byte & 0xF0) | (value & 0x0F)
        } else {
            (old_byte & 0x0F) | (value << 4)
        };
    }

    pub fn update(&mut self, coupon: Coupon) {
        let mask = (1 << self.lg_config_k) - 1;
        let slot = coupon.slot() & mask;
        let new_value = coupon.value();

        if new_value <= self.cur_min {
            return;
        }

        let raw_stored = self.get_raw(slot);
        let lower_bound = raw_stored + self.cur_min;

        if new_value <= lower_bound {
            return;
        }

        let old_value = if raw_stored < AUX_TOKEN {
            lower_bound
        } else {
            self.aux_map
                .as_ref()
                .expect("aux_map should be initialized since stored value is AUX_TOKEN")
                .get(slot)
                .expect("slot should be in aux_map since associated value is AUX_TOKEN")
        };

        if new_value <= old_value {
            return;
        }

        self.estimator
            .update(self.lg_config_k, old_value, new_value);

        let shifted_new = new_value - self.cur_min;

        match (raw_stored, shifted_new) {
            (AUX_TOKEN, shifted) if shifted >= AUX_TOKEN => {
                self.aux_map
                    .as_mut()
                    .expect("aux_map should be initialized since stored value is AUX_TOKEN")
                    .replace(slot, new_value);
            }
            // Registers only increase here; an exception cannot disappear until cur_min changes.
            (AUX_TOKEN, _) => {
                unreachable!("AUX_TOKEN present with non-exception new value");
            }
            (_, shifted) if shifted >= AUX_TOKEN => {
                self.put_raw(slot, AUX_TOKEN);
                let aux = self
                    .aux_map
                    .get_or_insert_with(|| AuxMap::new(self.lg_config_k));
                aux.insert(slot, new_value);
            }
            _ => {
                self.put_raw(slot, shifted_new);
            }
        }

        if old_value == self.cur_min {
            self.num_at_cur_min -= 1;
            while self.num_at_cur_min == 0 {
                self.shift_to_bigger_cur_min();
            }
        }
    }

    /// Requires no registers at `cur_min`. Raising the offset makes some auxiliary values
    /// fit in the nibbles again; the logical register values must stay unchanged.
    fn shift_to_bigger_cur_min(&mut self) {
        let new_cur_min = self.cur_min + 1;
        let k = 1 << self.lg_config_k;
        let mut num_at_new = 0;

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
                debug_assert_eq!(
                    self.get_raw(slot),
                    AUX_TOKEN,
                    "AuxMap contains slot != AUX_TOKEN"
                );

                let new_shifted = old_actual_val - new_cur_min;

                if new_shifted < AUX_TOKEN {
                    self.put_raw(slot, new_shifted);
                } else {
                    let aux = new_aux.get_or_insert_with(|| AuxMap::new(self.lg_config_k));
                    aux.insert(slot, old_actual_val);
                }
            }
            self.aux_map = new_aux;
        }

        self.cur_min = new_cur_min;
        self.num_at_cur_min = num_at_new;
    }

    pub fn estimate(&self) -> f64 {
        self.estimator
            .estimate(self.lg_config_k, self.cur_min, self.num_at_cur_min)
    }

    pub fn composite_estimate(&self) -> f64 {
        self.estimator
            .composite_estimate(self.lg_config_k, self.cur_min, self.num_at_cur_min)
    }

    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.estimator.upper_bound(
            self.lg_config_k,
            self.cur_min,
            self.num_at_cur_min,
            num_std_dev,
        )
    }

    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.estimator.lower_bound(
            self.lg_config_k,
            self.cur_min,
            self.num_at_cur_min,
            num_std_dev,
        )
    }

    pub fn restore_estimate_state(&mut self, state: EstimateState) {
        self.estimator.restore_estimate_state(state);
    }

    pub fn is_empty(&self) -> bool {
        self.num_at_cur_min == (1 << self.lg_config_k) && self.cur_min == 0
    }

    pub fn deserialize(
        mut cursor: SketchSlice,
        cur_min: u8,
        lg_config_k: u8,
        aux_format: AuxFormat,
        ooo: bool,
    ) -> Result<Self, Error> {
        let k = 1usize << lg_config_k;
        let num_bytes = 1usize << (lg_config_k - 1);

        let hip_accum = cursor
            .read_f64_le()
            .map_err(insufficient_data("hip_accum"))?;
        let kxq0 = cursor.read_f64_le().map_err(insufficient_data("kxq0"))?;
        let kxq1 = cursor.read_f64_le().map_err(insufficient_data("kxq1"))?;

        let num_at_cur_min = cursor
            .read_u32_le()
            .map_err(insufficient_data("num_at_cur_min"))?;
        let aux_count = cursor
            .read_u32_le()
            .map_err(insufficient_data("aux_count"))?;
        if num_at_cur_min as usize > k || aux_count as usize > k {
            return Err(Error::deserial(
                "HLL4 register or auxiliary count exceeds k",
            ));
        }
        let (aux_slots, compact) = match aux_format {
            AuxFormat::Compact => (aux_count as usize, true),
            AuxFormat::Updatable { lg_arr } => {
                let slots = if aux_count == 0 {
                    0
                } else {
                    1usize
                        .checked_shl(u32::from(lg_arr))
                        .ok_or_else(|| Error::deserial(format!("invalid HLL4 lg_arr: {lg_arr}")))?
                };
                (slots, false)
            }
        };
        let required_bytes = aux_slots
            .checked_mul(COUPON_SIZE_BYTES)
            .and_then(|aux_bytes| num_bytes.checked_add(aux_bytes))
            .ok_or_else(|| Error::deserial("HLL4 payload length overflows"))?;
        let available_bytes = cursor.remaining().len();
        if available_bytes < required_bytes {
            return Err(Error::insufficient_data_of(
                "HLL4 payload",
                format_args!("expected {required_bytes} bytes, got {available_bytes}"),
            ));
        }

        let mut data = vec![0u8; num_bytes];
        cursor
            .read_exact(&mut data)
            .map_err(insufficient_data("data"))?;

        let mut aux_map = None;
        if aux_count > 0 {
            let mut aux = AuxMap::new(lg_config_k);
            let mut decoded_count = 0;
            for i in 0..aux_slots {
                let coupon = cursor.read_u32_le().map_err(|error| {
                    Error::insufficient_data_of("HLL4 auxiliary slot", error)
                        .with_context("index", i)
                })?;
                let coupon = Coupon(coupon);
                if coupon.is_empty() && !compact {
                    continue;
                }
                let slot = coupon.slot() & ((1 << lg_config_k) - 1);
                let value = coupon.value();
                if coupon.is_empty() || aux.get(slot).is_some() {
                    return Err(Error::deserial(
                        "HLL4 auxiliary entries must be non-empty and unique",
                    ));
                }
                aux.insert(slot, value);
                decoded_count += 1;
            }
            if decoded_count != aux_count as usize {
                return Err(Error::deserial(format!(
                    "HLL4 auxiliary count is {aux_count}, decoded {decoded_count}"
                )));
            }
            aux_map = Some(aux);
        }

        let estimator = Estimator::from_serialized(hip_accum, kxq0, kxq1, ooo);

        Ok(Self {
            lg_config_k,
            bytes: data.into_boxed_slice(),
            cur_min,
            num_at_cur_min,
            aux_map,
            estimator,
        })
    }

    /// Writes packed registers followed by compact auxiliary entries.
    pub fn serialize(&self, lg_config_k: u8) -> Vec<u8> {
        let num_bytes = 1 << (lg_config_k - 1);

        let aux_entries: Vec<(u32, u8)> = if let Some(aux) = &self.aux_map {
            aux.iter().collect()
        } else {
            vec![]
        };

        let aux_count = aux_entries.len() as u32;
        let total_size = HLL_PREAMBLE_SIZE + num_bytes + (aux_count as usize * COUPON_SIZE_BYTES);
        let mut bytes = SketchBytes::with_capacity(total_size);

        bytes.write_u8(HLL_PREINTS);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::HLL.id);
        bytes.write_u8(lg_config_k);
        bytes.write_u8(0); // lg_arr is unused for compact auxiliary storage.

        let mut flags = COMPACT_FLAG_MASK;
        if self.estimator.uses_composite_estimate() {
            flags |= OUT_OF_ORDER_FLAG_MASK;
        }
        bytes.write_u8(flags);

        bytes.write_u8(self.cur_min);

        bytes.write_u8(encode_mode_byte(CUR_MODE_HLL, TGT_HLL4));

        bytes.write_f64_le(self.estimator.hip_accum());
        bytes.write_f64_le(self.estimator.kxq0());
        bytes.write_f64_le(self.estimator.kxq1());

        bytes.write_u32_le(self.num_at_cur_min);

        bytes.write_u32_le(aux_count);

        bytes.write(&self.bytes);

        for (slot, value) in aux_entries.iter().copied() {
            bytes.write_u32_le(Coupon::pack(slot, value).raw());
        }

        bytes.into_bytes()
    }

    /// Returns the estimated size of the heap allocations in bytes
    pub fn estimated_size(&self) -> usize {
        self.bytes.len()
            + self
                .aux_map
                .as_ref()
                .map(|a| a.estimated_size())
                .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use googletest::assert_that;
    use googletest::prelude::gt;
    use googletest::prelude::is_finite;
    use googletest::prelude::lt;
    use googletest::prelude::none;

    use super::*;
    use crate::hll::Coupon;

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

        data.put_raw(2, 15);
        data.put_raw(3, 3);
        assert_eq!(data.get_raw(2), 15);
        assert_eq!(data.get_raw(3), 3);
    }

    #[test]
    fn test_hip_estimator_basic() {
        let mut arr = Array4::new(10); // 1024 buckets

        assert_eq!(arr.estimate(), 0.0);

        for i in 0..10_000u32 {
            arr.update(Coupon::from_value(i));
        }

        let estimate = arr.estimate();

        assert_that!(estimate, gt(0.0));
        assert_that!(estimate, is_finite());
        assert_that!(estimate, lt(100_000.0));

        assert_that!(estimate, gt(1_000.0));
        assert_that!(estimate, lt(100_000.0));
    }

    #[test]
    fn test_kxq_register_split() {
        let mut arr = Array4::new(8); // 256 buckets

        arr.update(Coupon::pack(0, 10)); // value < 32, goes to kxq0
        arr.update(Coupon::pack(1, 40)); // value >= 32, goes to kxq1

        // kxq0 should have decreased (we removed a 0 and added a 10)
        // Initial kxq0 = 256 (all zeros = 1.0 each)
        assert_that!(arr.estimator.kxq0(), lt(256.0));

        // kxq1 should have a small positive value (from 1/2^40)
        assert_that!(arr.estimator.kxq1(), gt(0.0));
        assert_that!(arr.estimator.kxq1(), lt(0.001));
    }

    #[test]
    fn test_shift_cur_min_rebuilds_aux_entry() {
        let lg_config_k = 4;
        let num_slots = 1_u32 << lg_config_k;
        let mut arr = Array4::new(lg_config_k);

        arr.update(Coupon::pack(0, 15));
        assert_eq!(arr.get_raw(0), AUX_TOKEN);
        assert_eq!(arr.aux_map.as_ref().and_then(|aux| aux.get(0)), Some(15));

        for slot in 1..num_slots {
            arr.update(Coupon::pack(slot, 1));
        }

        assert_eq!(arr.cur_min, 1);
        assert_eq!(arr.num_at_cur_min, num_slots - 1);
        assert_eq!(arr.get_raw(0), 14);
        assert_eq!(arr.get(0), 15);
        assert_that!(arr.aux_map, none());

        for slot in 1..num_slots {
            assert_eq!(arr.get(slot), 1);
        }
    }
}
