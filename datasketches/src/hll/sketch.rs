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

use std::fmt;
use std::hash::Hash;

use crate::codec::SketchSlice;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::common::NumStdDev;
use crate::error::Error;
use crate::hll::Coupon;
use crate::hll::HllType;
use crate::hll::RESIZE_DENOMINATOR;
use crate::hll::RESIZE_NUMERATOR;
use crate::hll::array4::Array4;
use crate::hll::array4::AuxFormat;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
use crate::hll::container::Container;
use crate::hll::estimator::EstimateState;
use crate::hll::hash_set::HashSet;
use crate::hll::list::List;
use crate::hll::mode::Mode;
use crate::hll::serialization::COMPACT_FLAG_MASK;
use crate::hll::serialization::CUR_MODE_HLL;
use crate::hll::serialization::CUR_MODE_LIST;
use crate::hll::serialization::CUR_MODE_SET;
use crate::hll::serialization::EMPTY_FLAG_MASK;
use crate::hll::serialization::HASH_SET_PREINTS;
use crate::hll::serialization::HLL_PREINTS;
use crate::hll::serialization::LIST_PREINTS;
use crate::hll::serialization::OUT_OF_ORDER_FLAG_MASK;
use crate::hll::serialization::SERIAL_VERSION;
use crate::hll::serialization::TGT_HLL4;
use crate::hll::serialization::TGT_HLL6;
use crate::hll::serialization::TGT_HLL8;
use crate::hll::serialization::extract_cur_mode;
use crate::hll::serialization::extract_tgt_hll_type;

/// A HyperLogLog sketch.
///
/// See the [module level documentation](super) for more.
#[derive(Clone, PartialEq)]
pub struct HllSketch {
    lg_config_k: u8,
    mode: Mode,
}

impl HllSketch {
    /// Creates an empty sketch with `2^lg_config_k` registers and the requested [`HllType`].
    ///
    /// Increasing `lg_config_k` uses more memory and reduces estimation error.
    ///
    /// # Errors
    ///
    /// Returns an error if `lg_config_k` is outside `[4, 21]`.
    pub fn new(lg_config_k: u8, hll_type: HllType) -> Result<Self, Error> {
        if !(4..=21).contains(&lg_config_k) {
            return Err(Error::invalid_argument(format!(
                "lg_config_k must be in [4, 21], got {lg_config_k}"
            )));
        }

        let list = List::default();

        Ok(Self {
            lg_config_k,
            mode: Mode::List { list, hll_type },
        })
    }

    pub(super) fn from_mode(lg_config_k: u8, mode: Mode) -> Self {
        Self { lg_config_k, mode }
    }

    pub(super) fn mode(&self) -> &Mode {
        &self.mode
    }

    /// Callers must keep register-derived caches and estimate state consistent with the mode.
    pub(super) fn mode_mut(&mut self) -> &mut Mode {
        &mut self.mode
    }

    /// Returns `true` if no values have been added to the sketch.
    pub fn is_empty(&self) -> bool {
        match &self.mode {
            Mode::List { list, .. } => list.container().is_empty(),
            Mode::Set { set, .. } => set.container().is_empty(),
            Mode::Array4(arr) => arr.is_empty(),
            Mode::Array6(arr) => arr.is_empty(),
            Mode::Array8(arr) => arr.is_empty(),
        }
    }

    /// Returns the target HLL type for this sketch.
    pub fn target_type(&self) -> HllType {
        match &self.mode {
            Mode::List { hll_type, .. } => *hll_type,
            Mode::Set { hll_type, .. } => *hll_type,
            Mode::Array4(_) => HllType::Hll4,
            Mode::Array6(_) => HllType::Hll6,
            Mode::Array8(_) => HllType::Hll8,
        }
    }

    /// Returns the configured `lg_k`.
    pub fn lg_config_k(&self) -> u8 {
        self.lg_config_k
    }

    /// Updates the sketch with a value.
    ///
    /// You may use [`hash::value`](crate::hash::value) wrappers when another DataSketches
    /// implementation requires a specific value hashing strategy.
    ///
    /// To reuse the hash across sketches, see [`Coupon`] and
    /// [`update_with_coupon`](Self::update_with_coupon).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hash::value::raw_bytes;
    /// use datasketches::hll::HllSketch;
    /// use datasketches::hll::HllType;
    ///
    /// let mut sketch = HllSketch::new(10, HllType::Hll8).unwrap();
    /// sketch.update("apple");
    /// assert!(sketch.estimate() >= 1.0);
    ///
    /// let mut sketch = HllSketch::new(10, HllType::Hll8).unwrap();
    /// sketch.update(raw_bytes::from_str("apple"));
    /// assert!(sketch.estimate() >= 1.0);
    /// ```
    pub fn update<T: Hash>(&mut self, value: T) {
        self.update_with_coupon(Coupon::from_value(value));
    }

    /// Updates the sketch with a precomputed [`Coupon`], without hashing the input again.
    pub fn update_with_coupon(&mut self, coupon: Coupon) {
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

    /// Returns the current cardinality estimate.
    pub fn estimate(&self) -> f64 {
        match &self.mode {
            Mode::List { list, .. } => list.container().estimate(),
            Mode::Set { set, .. } => set.container().estimate(),
            Mode::Array4(arr) => arr.estimate(),
            Mode::Array6(arr) => arr.estimate(),
            Mode::Array8(arr) => arr.estimate(),
        }
    }

    /// Returns the upper confidence bound for `num_std_dev` standard deviations.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        match &self.mode {
            Mode::List { list, .. } => list.container().upper_bound(num_std_dev),
            Mode::Set { set, .. } => set.container().upper_bound(num_std_dev),
            Mode::Array4(arr) => arr.upper_bound(num_std_dev),
            Mode::Array6(arr) => arr.upper_bound(num_std_dev),
            Mode::Array8(arr) => arr.upper_bound(num_std_dev),
        }
    }

    /// Returns the lower confidence bound for `num_std_dev` standard deviations.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        match &self.mode {
            Mode::List { list, .. } => list.container().lower_bound(num_std_dev),
            Mode::Set { set, .. } => set.container().lower_bound(num_std_dev),
            Mode::Array4(arr) => arr.lower_bound(num_std_dev),
            Mode::Array6(arr) => arr.lower_bound(num_std_dev),
            Mode::Array8(arr) => arr.lower_bound(num_std_dev),
        }
    }

    /// Deserializes an HLL sketch from bytes.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the image is truncated or contains an invalid preamble,
    /// configuration, or payload.
    pub fn deserialize(bytes: &[u8]) -> Result<HllSketch, Error> {
        let mut cursor = SketchSlice::new(bytes);

        let preamble_ints = cursor
            .read_u8()
            .map_err(insufficient_data("preamble_ints"))?;
        let serial_version = cursor
            .read_u8()
            .map_err(insufficient_data("serial_version"))?;
        let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
        let lg_config_k = cursor.read_u8().map_err(insufficient_data("lg_config_k"))?;
        // lg_arr used in List/Set modes
        let lg_arr = cursor.read_u8().map_err(insufficient_data("lg_arr"))?;
        let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
        // The contextual state byte:
        // * coupon count in LIST mode
        // * cur_min in HLL mode
        // * unused in SET mode
        let state = cursor.read_u8().map_err(insufficient_data("state"))?;
        let mode_byte = cursor.read_u8().map_err(insufficient_data("mode"))?;

        Family::HLL.validate_id(family_id)?;

        ensure_serial_version_is(SERIAL_VERSION, serial_version)?;

        if !(4..=21).contains(&lg_config_k) {
            return Err(Error::deserial(format!(
                "lg_k must be in [4; 21], got {lg_config_k}",
            )));
        }

        let hll_type = match extract_tgt_hll_type(mode_byte) {
            TGT_HLL4 => HllType::Hll4,
            TGT_HLL6 => HllType::Hll6,
            TGT_HLL8 => HllType::Hll8,
            hll_type => {
                return Err(Error::deserial(format!("invalid HLL type: {hll_type}")));
            }
        };

        let empty = (flags & EMPTY_FLAG_MASK) != 0;
        let compact = (flags & COMPACT_FLAG_MASK) != 0;
        let ooo = (flags & OUT_OF_ORDER_FLAG_MASK) != 0;

        // Each mode reader starts after the shared eight-byte header.
        let mode =
            match extract_cur_mode(mode_byte) {
                CUR_MODE_LIST => {
                    if preamble_ints != LIST_PREINTS {
                        return Err(Error::deserial(format!(
                            "LIST mode preamble: expected {}, got {}",
                            LIST_PREINTS, preamble_ints,
                        )));
                    }

                    if lg_arr != 3 {
                        return Err(Error::deserial(format!(
                            "LIST mode lg_arr: expected 3, got {lg_arr}"
                        )));
                    }
                    let lg_arr = lg_arr as usize;
                    let coupon_count = state as usize;
                    let list = List::deserialize(cursor, lg_arr, coupon_count, empty, compact)?;
                    Mode::List { list, hll_type }
                }
                CUR_MODE_SET => {
                    if preamble_ints != HASH_SET_PREINTS {
                        return Err(Error::deserial(format!(
                            "SET mode preamble: expected {}, got {}",
                            HASH_SET_PREINTS, preamble_ints
                        )));
                    }

                    let max_lg_arr = lg_config_k.saturating_sub(3);
                    if !(5..=max_lg_arr).contains(&lg_arr) {
                        return Err(Error::deserial(format!(
                            "SET mode lg_arr must be in [5, {max_lg_arr}], got {lg_arr}"
                        )));
                    }
                    let lg_arr = lg_arr as usize;
                    let set = HashSet::deserialize(cursor, lg_arr, compact)?;
                    Mode::Set { set, hll_type }
                }
                CUR_MODE_HLL => {
                    if preamble_ints != HLL_PREINTS {
                        return Err(Error::deserial(format!(
                            "HLL mode preamble: expected {}, got {}",
                            HLL_PREINTS, preamble_ints
                        )));
                    }

                    match hll_type {
                        HllType::Hll4 => {
                            let aux = AuxFormat::from_header(compact, lg_arr);
                            Array4::deserialize(cursor, state, lg_config_k, aux, ooo)
                                .map(Mode::Array4)?
                        }
                        HllType::Hll6 => Array6::deserialize_registers(cursor, lg_config_k, ooo)
                            .map(Mode::Array6)?,
                        HllType::Hll8 => Array8::deserialize_registers(cursor, lg_config_k, ooo)
                            .map(Mode::Array8)?,
                    }
                }
                mode => return Err(Error::deserial(format!("invalid mode: {mode}"))),
            };

        Ok(HllSketch { lg_config_k, mode })
    }

    /// Serializes the HLL sketch to bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllSketch;
    /// use datasketches::hll::HllType;
    ///
    /// let mut sketch = HllSketch::new(10, HllType::Hll8).unwrap();
    /// sketch.update("apple");
    ///
    /// let bytes = sketch.serialize();
    /// let decoded = HllSketch::deserialize(&bytes).unwrap();
    /// assert!(decoded.estimate() >= 1.0);
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        match &self.mode {
            Mode::List { list, hll_type } => list.serialize(self.lg_config_k, *hll_type),
            Mode::Set { set, hll_type } => set.serialize(self.lg_config_k, *hll_type),
            Mode::Array4(arr) => arr.serialize(self.lg_config_k),
            Mode::Array6(arr) => arr.serialize(self.lg_config_k),
            Mode::Array8(arr) => arr.serialize(self.lg_config_k),
        }
    }

    /// Returns the estimated memory usage in bytes, including owned heap allocations.
    pub fn estimated_size(&self) -> usize {
        let heap_size = match &self.mode {
            Mode::List { list, .. } => list.container().estimated_size(),
            Mode::Set { set, .. } => set.container().estimated_size(),
            Mode::Array4(arr) => arr.estimated_size(),
            Mode::Array6(arr) => arr.estimated_size(),
            Mode::Array8(arr) => arr.estimated_size(),
        };

        size_of::<Self>() + heap_size
    }
}

impl fmt::Debug for HllSketch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (mode, estimate_state) = match &self.mode {
            Mode::List { .. } => ("List", None),
            Mode::Set { .. } => ("Set", None),
            Mode::Array4(array) => ("Hll", Some(array.estimate_state())),
            Mode::Array6(array) => ("Hll", Some(array.estimate_state())),
            Mode::Array8(array) => ("Hll", Some(array.estimate_state())),
        };

        let mut debug = f.debug_struct("HllSketch");
        debug
            .field("lg_config_k", &self.lg_config_k())
            .field("target_type", &self.target_type())
            .field("mode", &mode)
            .field("is_empty", &self.is_empty());
        if let Some(state) = estimate_state {
            let estimator = match state {
                EstimateState::Hip(_) => "HIP",
                EstimateState::Composite => "Composite",
            };
            debug.field("estimator", &estimator);
        }
        debug
            .field("estimate", &self.estimate())
            .field(
                "bounds",
                &(self.lower_bound(NumStdDev::One)..=self.upper_bound(NumStdDev::One)),
            )
            .finish()
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
            array.restore_estimate_state(EstimateState::Hip(container.estimate()));
            Mode::Array4(array)
        }
        HllType::Hll6 => {
            let mut array = Array6::new(lg_config_k);
            for coupon in container.iter() {
                array.update(coupon);
            }
            array.restore_estimate_state(EstimateState::Hip(container.estimate()));
            Mode::Array6(array)
        }
        HllType::Hll8 => {
            let mut array = Array8::new(lg_config_k);
            for coupon in container.iter() {
                array.update(coupon);
            }
            array.restore_estimate_state(EstimateState::Hip(container.estimate()));
            Mode::Array8(array)
        }
    }
}
