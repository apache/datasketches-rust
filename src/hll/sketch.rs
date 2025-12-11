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

use std::hash::Hash;
use std::io;

use crate::hll::array4::Array4;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
use crate::hll::container::Container;
use crate::hll::hash_set::HashSet;
use crate::hll::list::List;
use crate::hll::serialization::*;
use crate::hll::{HllType, RESIZE_DENOM, RESIZE_NUMER, coupon};

/// Current sketch mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CurMode {
    List = 0,
    Set = 1,
    Hll = 2,
}

#[derive(Debug, Clone)]
pub struct HllSketch {
    lg_config_k: u8,
    mode: Mode,
}

impl PartialEq for HllSketch {
    fn eq(&self, other: &Self) -> bool {
        if self.lg_config_k != other.lg_config_k {
            return false;
        }

        match (&self.mode, &other.mode) {
            (
                Mode::List {
                    list: l1,
                    hll_type: t1,
                },
                Mode::List {
                    list: l2,
                    hll_type: t2,
                },
            ) => l1 == l2 && t1 == t2,
            (
                Mode::Set {
                    set: s1,
                    hll_type: t1,
                },
                Mode::Set {
                    set: s2,
                    hll_type: t2,
                },
            ) => s1 == s2 && t1 == t2,
            (Mode::Array4(a1), Mode::Array4(a2)) => a1 == a2,
            (Mode::Array6(a1), Mode::Array6(a2)) => a1 == a2,
            (Mode::Array8(a1), Mode::Array8(a2)) => a1 == a2,
            _ => false, // Different modes are not equal
        }
    }
}

#[derive(Debug, Clone)]
enum Mode {
    List { list: List, hll_type: HllType },
    Set { set: HashSet, hll_type: HllType },
    Array4(Array4),
    Array6(Array6),
    Array8(Array8),
}

impl HllSketch {
    /// Create a new HLL sketch
    ///
    /// # Arguments
    /// * `lg_config_k` - Log2 of the number of buckets (K). Must be in [4, 21].
    ///   - lg_k=4: 16 buckets, ~26% relative error
    ///   - lg_k=12: 4096 buckets, ~1.6% relative error (common choice)
    ///   - lg_k=21: 2M buckets, ~0.4% relative error
    /// * `hll_type` - Target HLL array type (Hll4, Hll6, or Hll8)
    ///
    /// # Panics
    /// Panics if lg_config_k is not in [4, 21]
    pub fn new(lg_config_k: u8, hll_type: HllType) -> Self {
        assert!(
            lg_config_k > 4 && lg_config_k < 21,
            "lg_config_k must be in [4, 21]"
        );

        let list = List::default();

        Self {
            lg_config_k,
            mode: Mode::List { list, hll_type },
        }
    }

    pub fn lg_config_k(&self) -> u8 {
        self.lg_config_k
    }

    /// Update the sketch with a value
    ///
    /// This accepts any type that implements `Hash`. The value is hashed
    /// and converted to a coupon, which is then inserted into the sketch.
    /// The sketch will automatically promote from List → Set → HLL as needed.
    pub fn update<T: Hash>(&mut self, value: T) {
        let coupon = coupon(value);
        self.update_with_coupon(coupon);
    }

    /// Update the sketch with a raw coupon value
    ///
    /// This is useful when you've already computed the coupon externally,
    /// or when deserializing and replaying coupons.
    fn update_with_coupon(&mut self, coupon: u32) {
        // Perform the update
        match &mut self.mode {
            Mode::List { list, hll_type } => {
                list.update(coupon);
                let should_promote = list.container().is_full();
                if should_promote {
                    self.mode = if self.lg_config_k < 8 {
                        promote_container_to_array(list.container(), *hll_type, self.lg_config_k)
                    } else {
                        promote_container_to_set(list.container(), *hll_type)
                    }
                }
            }
            Mode::Set { set, hll_type } => {
                set.update(coupon);
                let should_promote = RESIZE_DENOM as usize * set.container().len()
                    > RESIZE_NUMER as usize * set.container().capacity();
                if should_promote {
                    self.mode = if set.container().lg_size() == self.lg_config_k as usize - 3 {
                        promote_container_to_array(set.container(), *hll_type, self.lg_config_k)
                    } else {
                        grow_set(set, *hll_type)
                    }
                }
            }
            Mode::Array4(arr) => arr.update(coupon),
            Mode::Array6(arr) => arr.update(coupon),
            Mode::Array8(arr) => arr.update(coupon),
        }
    }

    /// Get the current cardinality estimate
    pub fn estimate(&self) -> f64 {
        match &self.mode {
            Mode::List { list, .. } => list.container().estimate(),
            Mode::Set { set, .. } => set.container().estimate(),
            Mode::Array4(arr) => arr.estimate(),
            Mode::Array6(arr) => arr.estimate(),
            Mode::Array8(arr) => arr.estimate(),
        }
    }

    /// Get upper bound for cardinality estimate
    ///
    /// Returns the upper confidence bound for the cardinality estimate based on
    /// the number of standard deviations requested.
    pub fn upper_bound(&self, num_std_dev: u8) -> f64 {
        match &self.mode {
            Mode::List { list, .. } => list.container().upper_bound(num_std_dev),
            Mode::Set { set, .. } => set.container().upper_bound(num_std_dev),
            Mode::Array4(arr) => arr.upper_bound(num_std_dev),
            Mode::Array6(arr) => arr.upper_bound(num_std_dev),
            Mode::Array8(arr) => arr.upper_bound(num_std_dev),
        }
    }

    /// Get lower bound for cardinality estimate
    ///
    /// Returns the lower confidence bound for the cardinality estimate based on
    /// the number of standard deviations requested.
    pub fn lower_bound(&self, num_std_dev: u8) -> f64 {
        match &self.mode {
            Mode::List { list, .. } => list.container().lower_bound(num_std_dev),
            Mode::Set { set, .. } => set.container().lower_bound(num_std_dev),
            Mode::Array4(arr) => arr.lower_bound(num_std_dev),
            Mode::Array6(arr) => arr.lower_bound(num_std_dev),
            Mode::Array8(arr) => arr.lower_bound(num_std_dev),
        }
    }

    pub fn deserialize(bytes: &[u8]) -> io::Result<HllSketch> {
        if bytes.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sketch data too short (< 8 bytes)",
            ));
        }

        // Read and validate preamble
        let preamble_ints = bytes[PREAMBLE_INTS_BYTE];
        let ser_ver = bytes[SER_VER_BYTE];
        let family_id = bytes[FAMILY_BYTE];
        let lg_config_k = bytes[LG_K_BYTE];
        let flags = bytes[FLAGS_BYTE];
        let mode_byte = bytes[MODE_BYTE];

        // Verify family ID (HLL = 7)
        if family_id != HLL_FAMILY_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid family: expected 7 (HLL), got {}", family_id),
            ));
        }

        // Verify serialization version
        if ser_ver != SER_VER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid serialization version: expected {}, got {}",
                    SER_VER, ser_ver
                ),
            ));
        }

        // Verify lg_k range (4-21 are valid)
        if !(4..=21).contains(&lg_config_k) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid lg_k: {}, must be in [4; 21]", lg_config_k),
            ));
        }

        // Extract mode and type
        let cur_mode = extract_cur_mode_enum(mode_byte);
        let hll_type = extract_hll_type_enum(mode_byte);
        let empty = (flags & EMPTY_FLAG_MASK) != 0;
        let compact = (flags & COMPACT_FLAG_MASK) != 0;
        let ooo = (flags & OUT_OF_ORDER_FLAG_MASK) != 0;

        // Deserialize based on mode
        let mode =
            match cur_mode {
                CurMode::List => {
                    if preamble_ints != LIST_PREINTS {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "invalid preamble ints for LIST mode: expected {}, got {}",
                                LIST_PREINTS, preamble_ints
                            ),
                        ));
                    }

                    let list = List::deserialize(bytes, empty, compact)?;
                    Mode::List { list, hll_type }
                }
                CurMode::Set => {
                    if preamble_ints != HASH_SET_PREINTS {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "invalid preamble ints for SET mode: expected {}, got {}",
                                HASH_SET_PREINTS, preamble_ints
                            ),
                        ));
                    }

                    let set = HashSet::deserialize(bytes, compact)?;
                    Mode::Set { set, hll_type }
                }
                CurMode::Hll => {
                    if preamble_ints != HLL_PREINTS {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "invalid preamble ints for HLL mode: expected {}, got {}",
                                HLL_PREINTS, preamble_ints
                            ),
                        ));
                    }

                    match hll_type {
                        HllType::Hll4 => Array4::deserialize(bytes, lg_config_k, compact, ooo)
                            .map(Mode::Array4)?,
                        HllType::Hll6 => Array6::deserialize(bytes, lg_config_k, compact, ooo)
                            .map(Mode::Array6)?,
                        HllType::Hll8 => Array8::deserialize(bytes, lg_config_k, compact, ooo)
                            .map(Mode::Array8)?,
                    }
                }
            };

        Ok(HllSketch { lg_config_k, mode })
    }

    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        match &self.mode {
            Mode::List { list, hll_type } => list.serialize(self.lg_config_k, *hll_type),
            Mode::Set { set, hll_type } => set.serialize(self.lg_config_k, *hll_type),
            Mode::Array4(arr) => arr.serialize(self.lg_config_k),
            Mode::Array6(arr) => arr.serialize(self.lg_config_k),
            Mode::Array8(arr) => arr.serialize(self.lg_config_k),
        }
    }
}

fn promote_container_to_set(container: &Container, hll_type: HllType) -> Mode {
    let mut set = HashSet::default();
    for coupon in container.iter() {
        set.update(coupon);
    }

    Mode::Set { set, hll_type }
}

fn grow_set(old_set: &HashSet, hll_type: HllType) -> Mode {
    let new_size = old_set.container().lg_size() + 1;
    let mut new_set = HashSet::new(new_size);
    for coupon in old_set.container().iter() {
        new_set.update(coupon);
    }

    Mode::Set {
        set: new_set,
        hll_type,
    }
}

fn promote_container_to_array(container: &Container, hll_type: HllType, lg_config_k: u8) -> Mode {
    match hll_type {
        HllType::Hll4 => {
            let mut array = Array4::new(lg_config_k);
            for coupon in container.iter() {
                array.update(coupon);
            }
            array.set_hip_accum(container.estimate());
            Mode::Array4(array)
        }
        HllType::Hll6 => {
            let mut array = Array6::new(lg_config_k);
            for coupon in container.iter() {
                array.update(coupon);
            }
            array.set_hip_accum(container.estimate());
            Mode::Array6(array)
        }
        HllType::Hll8 => {
            let mut array = Array8::new(lg_config_k);
            for coupon in container.iter() {
                array.update(coupon);
            }
            array.set_hip_accum(container.estimate());
            Mode::Array8(array)
        }
    }
}

/// Extract current mode from mode byte using serialization module
fn extract_cur_mode_enum(mode_byte: u8) -> CurMode {
    match extract_cur_mode(mode_byte) {
        CUR_MODE_LIST => CurMode::List,
        CUR_MODE_SET => CurMode::Set,
        CUR_MODE_HLL => CurMode::Hll,
        _ => unreachable!(),
    }
}

/// Extract target HLL type from mode byte using serialization module
fn extract_hll_type_enum(mode_byte: u8) -> HllType {
    match extract_tgt_hll_type(mode_byte) {
        TGT_HLL4 => HllType::Hll4,
        TGT_HLL6 => HllType::Hll6,
        TGT_HLL8 => HllType::Hll8,
        _ => unreachable!(),
    }
}
