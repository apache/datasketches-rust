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

use crate::common::NumStdDev;
use crate::error::Error;
use crate::hll::Coupon;
use crate::hll::HllSketch;
use crate::hll::HllType;
use crate::hll::array4::Array4;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
use crate::hll::container::Container;
use crate::hll::estimator::EstimateState;
use crate::hll::mode::Mode;

/// An HLL union for combining multiple HLL sketches.
///
/// Merging sketches with different configurations may reduce the union's effective `lg_k`. Once
/// reduced, it remains lower until [`reset`](Self::reset), so estimates and bounds reflect the
/// reduced register count. The requested [`HllType`] changes only the result representation, not
/// its statistical accuracy.
///
/// See the [module level documentation](super) for more.
#[derive(Clone)]
pub struct HllUnion {
    lg_max_k: u8,
    /// Array modes always use `Array8`; packed results are produced by `to_sketch`.
    gadget: HllSketch,
}

impl fmt::Debug for HllUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HllUnion")
            .field("lg_max_k", &self.lg_max_k)
            .field("lg_config_k", &self.gadget.lg_config_k())
            .field("is_empty", &self.is_empty())
            .field("estimate", &self.estimate())
            .field(
                "bounds",
                &(self.lower_bound(NumStdDev::One)..=self.upper_bound(NumStdDev::One)),
            )
            .finish()
    }
}

impl HllUnion {
    /// Creates an empty union with a maximum of `2^lg_max_k` registers.
    ///
    /// Input sketches with a larger `lg_k` are downsampled.
    ///
    /// # Errors
    ///
    /// Returns an error if `lg_max_k` is outside `[4, 21]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllType;
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut union = HllUnion::new(10).unwrap();
    /// union.update_value("apple");
    /// let result = union.to_sketch(HllType::Hll8);
    /// assert!(result.estimate() >= 1.0);
    /// ```
    pub fn new(lg_max_k: u8) -> Result<Self, Error> {
        let gadget = HllSketch::new(lg_max_k, HllType::Hll8)?;

        Ok(Self { lg_max_k, gadget })
    }

    /// Updates the union with a value, using the same hashing as [`HllSketch::update`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut union = HllUnion::new(10).unwrap();
    /// union.update_value("apple");
    /// assert!(union.estimate() >= 1.0);
    /// ```
    pub fn update_value<T: Hash>(&mut self, value: T) {
        self.gadget.update(value);
    }

    /// Merges a sketch into the union, accepting any [`HllType`] and `lg_k`.
    ///
    /// The union's effective `lg_k` may decrease. Empty sketches have no effect.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllSketch;
    /// use datasketches::hll::HllType;
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut left = HllSketch::new(10, HllType::Hll8).unwrap();
    /// let mut right = HllSketch::new(10, HllType::Hll8).unwrap();
    /// left.update("apple");
    /// right.update("banana");
    ///
    /// let mut union = HllUnion::new(10).unwrap();
    /// union.update(&left);
    /// union.update(&right);
    /// let result = union.to_sketch(HllType::Hll8);
    /// assert!(result.estimate() >= 2.0);
    /// ```
    pub fn update(&mut self, sketch: &HllSketch) {
        if sketch.is_empty() {
            return;
        }

        let src_lg_k = sketch.lg_config_k();
        let dst_lg_k = self.gadget.lg_config_k();

        match sketch.mode() {
            Mode::List { list, .. } => {
                if self.gadget.is_empty() && src_lg_k == dst_lg_k {
                    self.gadget = HllSketch::from_mode(
                        src_lg_k,
                        Mode::List {
                            list: list.clone(),
                            hll_type: HllType::Hll8,
                        },
                    );
                } else {
                    self.gadget.update_with_coupons(list.container().iter());
                }
            }
            Mode::Set { set, .. } => {
                if self.gadget.is_empty() && src_lg_k == dst_lg_k {
                    self.gadget = HllSketch::from_mode(
                        src_lg_k,
                        Mode::Set {
                            set: set.clone(),
                            hll_type: HllType::Hll8,
                        },
                    );
                } else {
                    self.gadget.update_with_coupons(set.container().iter());
                }
            }
            Mode::Array4(src) => {
                self.update_from_array(ArrayMode::Array4(src), src_lg_k, dst_lg_k);
            }
            Mode::Array6(src) => {
                self.update_from_array(ArrayMode::Array6(src), src_lg_k, dst_lg_k);
            }
            Mode::Array8(src) => {
                self.update_from_array(ArrayMode::Array8(src), src_lg_k, dst_lg_k);
            }
        }
    }

    fn update_from_array(&mut self, src_mode: ArrayMode<'_>, src_lg_k: u8, dst_lg_k: u8) {
        if self.gadget.is_empty() {
            let new_array = copy_or_downsample(src_mode, src_lg_k, self.lg_max_k);
            let final_lg_k = new_array.num_registers().trailing_zeros() as u8;
            self.gadget = HllSketch::from_mode(final_lg_k, Mode::Array8(new_array));
            return;
        }

        let lg_max_k = self.lg_max_k;
        match self.gadget.mode_mut() {
            Mode::List { list, .. } => {
                let next =
                    promote_coupons_and_merge_array(src_mode, src_lg_k, lg_max_k, list.container());
                self.gadget = next;
            }
            Mode::Set { set, .. } => {
                let next =
                    promote_coupons_and_merge_array(src_mode, src_lg_k, lg_max_k, set.container());
                self.gadget = next;
            }
            Mode::Array8(dst_array) => {
                if src_lg_k < dst_lg_k {
                    let mut new_array = Array8::new(src_lg_k);
                    new_array.merge_array_with_downsample(dst_array.values(), dst_lg_k);
                    merge_array_same_lgk(&mut new_array, src_mode);
                    self.gadget = HllSketch::from_mode(src_lg_k, Mode::Array8(new_array));
                } else {
                    merge_array_into_array8(dst_array, dst_lg_k, src_mode, src_lg_k);
                }
            }
            Mode::Array4(_) => unreachable!("HllUnion gadget should never be in Array4 mode"),
            Mode::Array6(_) => unreachable!("HllUnion gadget should never be in Array6 mode"),
        }
    }

    /// Returns an independent sketch with the requested [`HllType`].
    ///
    /// The result retains the union's effective `lg_k` and cardinality estimate.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllType;
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut union = HllUnion::new(10).unwrap();
    /// union.update_value("apple");
    /// let result = union.to_sketch(HllType::Hll6);
    /// assert_eq!(result.target_type(), HllType::Hll6);
    /// assert_eq!(result.estimate(), union.estimate());
    /// ```
    pub fn to_sketch(&self, hll_type: HllType) -> HllSketch {
        let gadget_type = self.gadget.target_type();

        if hll_type == gadget_type {
            return self.gadget.clone();
        }

        match self.gadget.mode() {
            Mode::List { list, .. } => HllSketch::from_mode(
                self.gadget.lg_config_k(),
                Mode::List {
                    list: list.clone(),
                    hll_type,
                },
            ),
            Mode::Set { set, .. } => HllSketch::from_mode(
                self.gadget.lg_config_k(),
                Mode::Set {
                    set: set.clone(),
                    hll_type,
                },
            ),
            Mode::Array8(array8) => {
                convert_array8_to_type(array8, self.gadget.lg_config_k(), hll_type)
            }
            Mode::Array4(_) => unreachable!("HllUnion gadget should never be in Array4 mode"),
            Mode::Array6(_) => unreachable!("HllUnion gadget should never be in Array6 mode"),
        }
    }

    /// Serializes the union's result as an [`HllSketch`] with the requested [`HllType`].
    ///
    /// Produces an equivalent sketch to [`Self::to_sketch`] followed by [`HllSketch::serialize`]
    /// without allocating or cloning an intermediate sketch. For [`HllType::Hll6`],
    /// [`HllType::Hll8`], coupon modes, and [`HllType::Hll4`] with at most one auxiliary entry,
    /// the output matches `to_sketch(hll_type).serialize()` byte-for-byte; for [`HllType::Hll4`]
    /// with multiple auxiliary entries, auxiliary entries are written in ascending slot order
    /// rather than hash-table order.
    ///
    /// The image retains the effective `lg_k` and estimate, but not the union's configured maximum
    /// `lg_k`. It can be read with [`HllSketch::deserialize`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllSketch;
    /// use datasketches::hll::HllType;
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut union = HllUnion::new(12).unwrap();
    /// union.update_value("apple");
    /// let mut partial = HllSketch::deserialize(&union.serialize(HllType::Hll8)).unwrap();
    /// partial.update("banana");
    /// union.update(&partial);
    /// assert!((union.estimate() - 2.0).abs() < 0.01);
    /// ```
    pub fn serialize(&self, hll_type: HllType) -> Vec<u8> {
        let lg_config_k = self.gadget.lg_config_k();
        match self.gadget.mode() {
            Mode::List { list, .. } => list.serialize(lg_config_k, hll_type),
            Mode::Set { set, .. } => set.serialize(lg_config_k, hll_type),
            Mode::Array8(arr) => arr.serialize_as(lg_config_k, hll_type),
            Mode::Array4(_) => unreachable!("HllUnion gadget should never be in Array4 mode"),
            Mode::Array6(_) => unreachable!("HllUnion gadget should never be in Array6 mode"),
        }
    }

    /// Returns the union's current effective `lg_k`.
    pub fn lg_config_k(&self) -> u8 {
        self.gadget.lg_config_k()
    }

    /// Returns the maximum configured `lg_k`.
    pub fn lg_max_k(&self) -> u8 {
        self.lg_max_k
    }

    /// Returns `true` if the union is empty.
    pub fn is_empty(&self) -> bool {
        self.gadget.is_empty()
    }

    /// Clears the union and restores its configured maximum `lg_k`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllSketch;
    /// use datasketches::hll::HllType;
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut sketch = HllSketch::new(10, HllType::Hll8).unwrap();
    /// for value in 0..10_000 {
    ///     sketch.update(value);
    /// }
    /// let mut union = HllUnion::new(12).unwrap();
    /// union.update(&sketch);
    /// assert_eq!(union.lg_config_k(), 10);
    ///
    /// union.reset();
    /// assert!(union.is_empty());
    /// assert_eq!(union.lg_config_k(), 12);
    /// ```
    pub fn reset(&mut self) {
        self.gadget = HllSketch::new(self.lg_max_k, HllType::Hll8).unwrap();
    }

    /// Returns the union's current cardinality estimate.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut union = HllUnion::new(10).unwrap();
    /// for value in ["apple", "banana", "apple"] {
    ///     union.update_value(value);
    /// }
    /// let estimate = union.estimate();
    /// assert!((estimate - 2.0).abs() < 0.01);
    /// ```
    pub fn estimate(&self) -> f64 {
        self.gadget.estimate()
    }

    /// Returns the union's cardinality estimate using the composite estimator.
    ///
    /// See [`HllSketch::composite_estimate`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut union = HllUnion::new(12).unwrap();
    /// for value in 0..10_000 {
    ///     union.update_value(value);
    /// }
    /// let estimate = union.composite_estimate();
    /// assert!((estimate - 10_000.0).abs() < 500.0);
    /// ```
    pub fn composite_estimate(&self) -> f64 {
        self.gadget.composite_estimate()
    }

    /// Returns the upper confidence bound for `num_std_dev` standard deviations.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::common::NumStdDev;
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut union = HllUnion::new(10).unwrap();
    /// for value in 0..10_000 {
    ///     union.update_value(value);
    /// }
    /// let upper = union.upper_bound(NumStdDev::Two);
    /// assert!(upper >= union.estimate());
    /// ```
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.gadget.upper_bound(num_std_dev)
    }

    /// Returns the lower confidence bound for `num_std_dev` standard deviations.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::common::NumStdDev;
    /// use datasketches::hll::HllUnion;
    ///
    /// let mut union = HllUnion::new(10).unwrap();
    /// for value in 0..10_000 {
    ///     union.update_value(value);
    /// }
    /// let lower = union.lower_bound(NumStdDev::Two);
    /// assert!(lower <= union.estimate());
    /// ```
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.gadget.lower_bound(num_std_dev)
    }

    /// Returns an estimate of heap memory used by this union in bytes.
    ///
    /// Excludes inline storage and allocator overhead.
    pub fn estimated_size(&self) -> usize {
        self.gadget.estimated_size()
    }
}

#[derive(Clone, Copy)]
enum ArrayMode<'a> {
    Array4(&'a Array4),
    Array6(&'a Array6),
    Array8(&'a Array8),
}

impl ArrayMode<'_> {
    fn estimate_state(self) -> EstimateState {
        match self {
            Self::Array4(src) => src.estimate_state(),
            Self::Array6(src) => src.estimate_state(),
            Self::Array8(src) => src.estimate_state(),
        }
    }
}

fn promote_coupons_and_merge_array<S: AsRef<[Coupon]>>(
    src_mode: ArrayMode<'_>,
    src_lg_k: u8,
    lg_max_k: u8,
    coupons: &Container<S>,
) -> HllSketch {
    let mut new_array = copy_or_downsample(src_mode, src_lg_k, lg_max_k);
    for coupon in coupons.iter() {
        new_array.update(coupon);
    }
    let final_lg_k = new_array.num_registers().trailing_zeros() as u8;
    HllSketch::from_mode(final_lg_k, Mode::Array8(new_array))
}

/// The caller must downsize the destination first if the source has a smaller `lg_k`.
fn merge_array_into_array8(
    dst_array8: &mut Array8,
    dst_lg_k: u8,
    src_mode: ArrayMode<'_>,
    src_lg_k: u8,
) {
    assert!(
        src_lg_k >= dst_lg_k,
        "merge_array_into_array8 requires src_lg_k >= dst_lg_k (got src={}, dst={})",
        src_lg_k,
        dst_lg_k
    );

    if dst_lg_k == src_lg_k {
        merge_array_same_lgk(dst_array8, src_mode);
    } else {
        merge_array_with_downsample(dst_array8, dst_lg_k, src_mode, src_lg_k);
    }
}

fn merge_array46_same_lgk(dst: &mut Array8, num_registers: usize, get_value: impl Fn(u32) -> u8) {
    for slot in 0..num_registers {
        let val = get_value(slot as u32);
        let current = dst.values()[slot];
        if val > current {
            dst.set_register(slot, val);
        }
    }
    dst.invalidate_estimator();
}

/// Merging independent register histories invalidates the HIP accumulator.
fn merge_array_same_lgk(dst: &mut Array8, src_mode: ArrayMode<'_>) {
    match src_mode {
        ArrayMode::Array8(src) => {
            dst.merge_array_same_lgk(src.values());
        }
        ArrayMode::Array6(src) => {
            merge_array46_same_lgk(dst, src.num_registers(), |slot| src.get(slot));
        }
        ArrayMode::Array4(src) => {
            merge_array46_same_lgk(dst, src.num_registers(), |slot| src.get(slot));
        }
    }
}

fn merge_array46_with_downsample(
    dst: &mut Array8,
    dst_lg_k: u8,
    num_registers: usize,
    get_value: impl Fn(u32) -> u8,
) {
    let dst_mask = (1 << dst_lg_k) - 1;
    for src_slot in 0..num_registers {
        let val = get_value(src_slot as u32);
        if val > 0 {
            let dst_slot = (src_slot as u32 & dst_mask) as usize;
            let current = dst.values()[dst_slot];
            if val > current {
                dst.set_register(dst_slot, val);
            }
        }
    }
    dst.invalidate_estimator();
}

/// Multiple source registers map to each destination register via masking.
/// Merging independent register histories invalidates the HIP accumulator.
fn merge_array_with_downsample(
    dst: &mut Array8,
    dst_lg_k: u8,
    src_mode: ArrayMode<'_>,
    src_lg_k: u8,
) {
    assert!(
        src_lg_k > dst_lg_k,
        "merge_array_with_downsample requires src_lg_k > dst_lg_k (got src={}, dst={})",
        src_lg_k,
        dst_lg_k
    );

    match src_mode {
        ArrayMode::Array8(src) => {
            dst.merge_array_with_downsample(src.values(), src_lg_k);
        }
        ArrayMode::Array6(src) => {
            merge_array46_with_downsample(dst, dst_lg_k, src.num_registers(), |slot| src.get(slot));
        }
        ArrayMode::Array4(src) => {
            merge_array46_with_downsample(dst, dst_lg_k, src.num_registers(), |slot| src.get(slot));
        }
    }
}

/// Changing representation preserves the estimate state; replaying registers alone would change
/// HIP.
fn convert_array8_to_type(src: &Array8, lg_config_k: u8, ty: HllType) -> HllSketch {
    let state = src.estimate_state();
    match ty {
        HllType::Hll8 => HllSketch::from_mode(lg_config_k, Mode::Array8(src.clone())),
        HllType::Hll6 => {
            let mut array6 = Array6::new(lg_config_k);
            for slot in 0..src.num_registers() {
                let val = src.values()[slot];
                if val > 0 {
                    let clamped_val = val.min(63);
                    let coupon = Coupon::pack(slot as u32, clamped_val);
                    array6.update(coupon);
                }
            }
            array6.restore_estimate_state(state);

            HllSketch::from_mode(lg_config_k, Mode::Array6(array6))
        }
        HllType::Hll4 => {
            let mut array4 = Array4::new(lg_config_k);
            for slot in 0..src.num_registers() {
                let val = src.values()[slot];
                if val > 0 {
                    let coupon = Coupon::pack(slot as u32, val);
                    array4.update(coupon);
                }
            }
            array4.restore_estimate_state(state);

            HllSketch::from_mode(lg_config_k, Mode::Array4(array4))
        }
    }
}

fn copy_array46_via_coupons(dst: &mut Array8, num_registers: usize, get_value: impl Fn(u32) -> u8) {
    for slot in 0..num_registers {
        let val = get_value(slot as u32);
        if val > 0 {
            let coupon = Coupon::pack(slot as u32, val);
            dst.update(coupon);
        }
    }
}

/// Preserves the source estimate state because the result represents the same logical sketch.
fn copy_or_downsample(src_mode: ArrayMode<'_>, src_lg_k: u8, tgt_lg_k: u8) -> Array8 {
    let state = src_mode.estimate_state();

    let mut result = if src_lg_k <= tgt_lg_k {
        match src_mode {
            ArrayMode::Array8(src) => src.clone(),
            ArrayMode::Array6(src) => {
                let mut result = Array8::new(src_lg_k);
                copy_array46_via_coupons(&mut result, src.num_registers(), |slot| src.get(slot));
                result
            }
            ArrayMode::Array4(src) => {
                let mut result = Array8::new(src_lg_k);
                copy_array46_via_coupons(&mut result, src.num_registers(), |slot| src.get(slot));
                result
            }
        }
    } else {
        let mut result = Array8::new(tgt_lg_k);
        merge_array_with_downsample(&mut result, tgt_lg_k, src_mode, src_lg_k);
        result
    };

    result.restore_estimate_state(state);
    result
}
