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

//! HyperLogLog sketch implementation
//!
//! This module provides the main [`HllSketch`] struct, which is the primary interface
//! for creating and using HLL sketches for cardinality estimation.

use std::hash::Hash;

use crate::error::SerdeError;
use crate::hll::array4::Array4;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
use crate::hll::container::Container;
use crate::hll::hash_set::HashSet;
use crate::hll::list::List;
use crate::hll::mode::Mode;
use crate::hll::serialization::*;
use crate::hll::{HllType, NumStdDev, RESIZE_DENOMINATOR, RESIZE_NUMERATOR, coupon};

/// A HyperLogLog sketch.
///
/// See the [hll module level documentation](crate::hll) for more.
#[derive(Debug, Clone, PartialEq)]
pub struct HllSketch {
    lg_config_k: u8,
    mode: Mode,
}

impl HllSketch {
    /// Create a new HLL sketch
    ///
    /// # Arguments
    ///
    /// * `lg_config_k` - Log2 of the number of buckets (K). Must be in [4, 21].
    ///   - lg_k=4: 16 buckets, ~26% relative error
    ///   - lg_k=12: 4096 buckets, ~1.6% relative error (common choice)
    ///   - lg_k=21: 2M buckets, ~0.4% relative error
    /// * `hll_type` - Target HLL array type (Hll4, Hll6, or Hll8)
    ///
    /// # Panics
    ///
    /// If lg_config_k is not in range [4, 21]
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

    /// Create an HLL sketch directly from a Mode
    ///
    /// This is used internally (e.g., by union operations) to construct
    /// sketches in specific modes without going through List mode first.
    ///
    /// # Arguments
    ///
    /// * `lg_config_k` - Log2 of the number of buckets (K)
    /// * `mode` - The mode to initialize the sketch with
    pub(super) fn from_mode(lg_config_k: u8, mode: Mode) -> Self {
        Self { lg_config_k, mode }
    }

    /// Get the current mode of the sketch
    pub(super) fn mode(&self) -> &Mode {
        &self.mode
    }

    /// Get mutable access to the current mode
    ///
    /// # Safety
    ///
    /// Caller must maintain internal invariants (num_zeros, estimator state).
    pub(super) fn mode_mut(&mut self) -> &mut Mode {
        &mut self.mode
    }

    /// Check if the sketch is empty (no values have been added)
    pub fn is_empty(&self) -> bool {
        match &self.mode {
            Mode::List { list, .. } => list.container().is_empty(),
            Mode::Set { set, .. } => set.container().is_empty(),
            Mode::Array4(arr) => arr.is_empty(),
            Mode::Array6(arr) => arr.is_empty(),
            Mode::Array8(arr) => arr.is_empty(),
        }
    }

    /// Get the target HLL type for this sketch
    pub fn target_type(&self) -> HllType {
        match &self.mode {
            Mode::List { hll_type, .. } => *hll_type,
            Mode::Set { hll_type, .. } => *hll_type,
            Mode::Array4(_) => HllType::Hll4,
            Mode::Array6(_) => HllType::Hll6,
            Mode::Array8(_) => HllType::Hll8,
        }
    }

    /// Get the configured lg_config_k
    pub fn lg_config_k(&self) -> u8 {
        self.lg_config_k
    }

    /// Update the sketch with a value
    ///
    /// This accepts any type that implements `Hash`. The value is hashed
    /// and converted to a coupon, which is then inserted into the sketch.
    pub fn update<T: Hash>(&mut self, value: T) {
        let coupon = coupon(value);
        self.update_with_coupon(coupon);
    }

    /// Update the sketch with a raw coupon value
    ///
    /// Maintains all sketch invariants including mode transitions and estimator updates.
    pub(super) fn update_with_coupon(&mut self, coupon: u32) {
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
                let should_promote = RESIZE_DENOMINATOR as usize * set.container().len()
                    > RESIZE_NUMERATOR as usize * set.container().capacity();
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
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
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
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        match &self.mode {
            Mode::List { list, .. } => list.container().lower_bound(num_std_dev),
            Mode::Set { set, .. } => set.container().lower_bound(num_std_dev),
            Mode::Array4(arr) => arr.lower_bound(num_std_dev),
            Mode::Array6(arr) => arr.lower_bound(num_std_dev),
            Mode::Array8(arr) => arr.lower_bound(num_std_dev),
        }
    }

    /// Deserializes an HLL sketch from bytes
    pub fn deserialize(bytes: &[u8]) -> Result<HllSketch, SerdeError> {
        if bytes.len() < 8 {
            return Err(SerdeError::InsufficientData(
                "sketch data too short (< 8 bytes)".to_string(),
            ));
        }

        // Read and validate preamble
        let preamble_ints = bytes[PREAMBLE_INTS_BYTE];
        let ser_ver = bytes[SER_VER_BYTE];
        let family_id = bytes[FAMILY_BYTE];
        let lg_config_k = bytes[LG_K_BYTE];
        let flags = bytes[FLAGS_BYTE];
        let mode_byte = bytes[MODE_BYTE];

        // Verify family ID
        if family_id != HLL_FAMILY_ID {
            return Err(SerdeError::InvalidFamily(format!(
                "expected {} (HLL), got {}",
                HLL_FAMILY_ID, family_id
            )));
        }

        // Verify serialization version
        if ser_ver != SER_VER {
            return Err(SerdeError::UnsupportedVersion(format!(
                "expected {}, got {}",
                SER_VER, ser_ver
            )));
        }

        // Verify lg_k range (4-21 are valid)
        if !(4..=21).contains(&lg_config_k) {
            return Err(SerdeError::InvalidParameter(format!(
                "lg_k must be in [4; 21], got {}",
                lg_config_k
            )));
        }

        let hll_type = match extract_tgt_hll_type(mode_byte) {
            TGT_HLL4 => HllType::Hll4,
            TGT_HLL6 => HllType::Hll6,
            TGT_HLL8 => HllType::Hll8,
            hll_type => {
                return Err(SerdeError::MalformedData(format!(
                    "invalid HLL type: {}",
                    hll_type
                )));
            }
        };

        let empty = (flags & EMPTY_FLAG_MASK) != 0;
        let compact = (flags & COMPACT_FLAG_MASK) != 0;
        let ooo = (flags & OUT_OF_ORDER_FLAG_MASK) != 0;

        // Deserialize based on mode
        let mode =
            match extract_cur_mode(mode_byte) {
                CUR_MODE_LIST => {
                    if preamble_ints != LIST_PREINTS {
                        return Err(SerdeError::MalformedData(format!(
                            "LIST mode preamble: expected {}, got {}",
                            LIST_PREINTS, preamble_ints
                        )));
                    }

                    let list = List::deserialize(bytes, empty, compact)?;
                    Mode::List { list, hll_type }
                }
                CUR_MODE_SET => {
                    if preamble_ints != HASH_SET_PREINTS {
                        return Err(SerdeError::MalformedData(format!(
                            "SET mode preamble: expected {}, got {}",
                            HASH_SET_PREINTS, preamble_ints
                        )));
                    }

                    let set = HashSet::deserialize(bytes, compact)?;
                    Mode::Set { set, hll_type }
                }
                CUR_MODE_HLL => {
                    if preamble_ints != HLL_PREINTS {
                        return Err(SerdeError::MalformedData(format!(
                            "HLL mode preamble: expected {}, got {}",
                            HLL_PREINTS, preamble_ints
                        )));
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
                mode => return Err(SerdeError::MalformedData(format!("invalid mode: {}", mode))),
            };

        Ok(HllSketch { lg_config_k, mode })
    }

    /// Serializes the HLL sketch to bytes
    pub fn serialize(&self) -> Vec<u8> {
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
