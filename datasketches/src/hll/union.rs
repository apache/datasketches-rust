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

use crate::common::NumStdDev;
use crate::error::Error;
use crate::hll::Coupon;
use crate::hll::HllSketch;
use crate::hll::HllType;
use crate::hll::array4::Array4;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
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
#[derive(Debug, Clone)]
pub struct HllUnion {
    lg_max_k: u8,
    /// Array modes always use `Array8`; packed results are produced by `to_sketch`.
    gadget: HllSketch,
}

impl HllUnion {
    /// Creates an empty union with a maximum of `2^lg_max_k` registers.
    ///
    /// Input sketches with a larger `lg_k` are downsampled.
    ///
    /// # Errors
    ///
    /// Returns an error if `lg_max_k` is outside `[4, 21]`.
    pub fn new(lg_max_k: u8) -> Result<Self, Error> {
        let gadget = HllSketch::new(lg_max_k, HllType::Hll8)?;

        Ok(Self { lg_max_k, gadget })
    }

    /// Updates the union with a value, using the same hashing as [`HllSketch::update`].
    pub fn update_value<T: Hash>(&mut self, value: T) {
        self.gadget.update(value);
    }

    /// Merges a sketch into the union, accepting any [`HllType`] and `lg_k`.
    ///
    /// The union's effective `lg_k` may decrease. Empty sketches have no effect.
    pub fn update(&mut self, sketch: &HllSketch) {
        if sketch.is_empty() {
            return;
        }

        let src_lg_k = sketch.lg_config_k();
        let dst_lg_k = self.gadget.lg_config_k();
        let src_mode = sketch.mode();

        match src_mode {
            Mode::List { .. } | Mode::Set { .. } => {
                self.update_from_list_or_set(sketch, src_mode, src_lg_k, dst_lg_k);
            }
            Mode::Array4(_) | Mode::Array6(_) | Mode::Array8(_) => {
                self.update_from_array(src_mode, src_lg_k, dst_lg_k);
            }
        }
    }

    fn update_from_list_or_set(
        &mut self,
        sketch: &HllSketch,
        src_mode: &Mode,
        src_lg_k: u8,
        dst_lg_k: u8,
    ) {
        if self.gadget.is_empty() && src_lg_k == dst_lg_k {
            self.gadget = if sketch.target_type() == HllType::Hll8 {
                sketch.clone()
            } else {
                convert_coupon_mode_to_hll8(src_mode, src_lg_k)
            };
        } else {
            merge_coupons_into_gadget(&mut self.gadget, src_mode);
        }
    }

    fn update_from_array(&mut self, src_mode: &Mode, src_lg_k: u8, dst_lg_k: u8) {
        if self.gadget.is_empty() {
            let new_array = copy_or_downsample(src_mode, src_lg_k, self.lg_max_k);
            let final_lg_k = new_array.num_registers().trailing_zeros() as u8;
            self.gadget = HllSketch::from_mode(final_lg_k, Mode::Array8(new_array));
            return;
        }

        let is_gadget_array = matches!(self.gadget.mode(), Mode::Array8(_));

        if is_gadget_array {
            self.merge_array_into_array_gadget(src_mode, src_lg_k, dst_lg_k);
        } else {
            self.promote_gadget_and_merge_array(src_mode, src_lg_k);
        }
    }

    fn merge_array_into_array_gadget(&mut self, src_mode: &Mode, src_lg_k: u8, dst_lg_k: u8) {
        if src_lg_k < dst_lg_k {
            let mut new_array = Array8::new(src_lg_k);

            match self.gadget.mode() {
                Mode::Array8(old_gadget) => {
                    merge_array_with_downsample(
                        &mut new_array,
                        src_lg_k,
                        &Mode::Array8(old_gadget.clone()),
                        dst_lg_k,
                    );
                }
                _ => {
                    unreachable!("gadget mode changed unexpectedly; should never be Array4/Array6")
                }
            }

            merge_array_same_lgk(&mut new_array, src_mode);
            self.gadget = HllSketch::from_mode(src_lg_k, Mode::Array8(new_array));
        } else {
            match self.gadget.mode_mut() {
                Mode::Array8(dst_array) => {
                    merge_array_into_array8(dst_array, dst_lg_k, src_mode, src_lg_k);
                }
                _ => {
                    unreachable!("gadget mode changed unexpectedly; should never be Array4/Array6")
                }
            }
        }
    }

    fn promote_gadget_and_merge_array(&mut self, src_mode: &Mode, src_lg_k: u8) {
        let mut new_array = copy_or_downsample(src_mode, src_lg_k, self.lg_max_k);

        let old_gadget_mode = self.gadget.mode();
        merge_coupons_into_mode(&mut new_array, old_gadget_mode);

        let final_lg_k = new_array.num_registers().trailing_zeros() as u8;
        self.gadget = HllSketch::from_mode(final_lg_k, Mode::Array8(new_array));
    }

    /// Returns an independent sketch with the requested [`HllType`].
    ///
    /// The result retains the union's effective `lg_k` and cardinality estimate.
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
            Mode::Array4(_) | Mode::Array6(_) => {
                unreachable!("gadget mode changed unexpectedly; should never be Array4/Array6")
            }
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
    pub fn reset(&mut self) {
        self.gadget = HllSketch::new(self.lg_max_k, HllType::Hll8).unwrap();
    }

    /// Returns the union's current cardinality estimate.
    pub fn estimate(&self) -> f64 {
        self.gadget.estimate()
    }

    /// Returns the upper confidence bound for `num_std_dev` standard deviations.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.gadget.upper_bound(num_std_dev)
    }

    /// Returns the lower confidence bound for `num_std_dev` standard deviations.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.gadget.lower_bound(num_std_dev)
    }

    /// Returns the estimated memory usage in bytes, including owned heap allocations.
    pub fn estimated_size(&self) -> usize {
        // The gadget's inline size is already covered by size_of::<Self>().
        size_of::<Self>() - size_of::<HllSketch>() + self.gadget.estimated_size()
    }
}

fn convert_coupon_mode_to_hll8(src_mode: &Mode, src_lg_k: u8) -> HllSketch {
    match src_mode {
        Mode::List { list, .. } => HllSketch::from_mode(
            src_lg_k,
            Mode::List {
                list: list.clone(),
                hll_type: HllType::Hll8,
            },
        ),
        Mode::Set { set, .. } => HllSketch::from_mode(
            src_lg_k,
            Mode::Set {
                set: set.clone(),
                hll_type: HllType::Hll8,
            },
        ),
        _ => unreachable!("convert_coupon_mode_to_hll8 called with non-coupon mode"),
    }
}

fn merge_coupons_into_gadget(gadget: &mut HllSketch, src_mode: &Mode) {
    match src_mode {
        Mode::List { list, .. } => {
            for coupon in list.container().iter() {
                gadget.update_with_coupon(coupon);
            }
        }
        Mode::Set { set, .. } => {
            for coupon in set.container().iter() {
                gadget.update_with_coupon(coupon);
            }
        }
        Mode::Array4(_) | Mode::Array6(_) | Mode::Array8(_) => {
            unreachable!(
                "merge_coupons_into_gadget called with array mode; array modes should use merge_array_into_array8"
            );
        }
    }
}

fn merge_coupons_into_mode(dst: &mut Array8, src_mode: &Mode) {
    match src_mode {
        Mode::List { list, .. } => {
            for coupon in list.container().iter() {
                dst.update(coupon);
            }
        }
        Mode::Set { set, .. } => {
            for coupon in set.container().iter() {
                dst.update(coupon);
            }
        }
        Mode::Array4(_) | Mode::Array6(_) | Mode::Array8(_) => {
            unreachable!(
                "merge_coupons_into_mode called with array mode; array modes should use copy_or_downsample"
            );
        }
    }
}

/// The caller must downsize the destination first if the source has a smaller `lg_k`.
fn merge_array_into_array8(dst_array8: &mut Array8, dst_lg_k: u8, src_mode: &Mode, src_lg_k: u8) {
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

fn get_array_estimate_state(mode: &Mode) -> EstimateState {
    match mode {
        Mode::Array8(src) => src.estimate_state(),
        Mode::Array6(src) => src.estimate_state(),
        Mode::Array4(src) => src.estimate_state(),
        Mode::List { .. } | Mode::Set { .. } => {
            unreachable!(
                "get_array_estimate_state called with non-array mode; List/Set not supported"
            );
        }
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
    dst.rebuild_estimator_from_registers();
}

/// Merging independent register histories invalidates the HIP accumulator.
fn merge_array_same_lgk(dst: &mut Array8, src_mode: &Mode) {
    match src_mode {
        Mode::Array8(src) => {
            dst.merge_array_same_lgk(src.values());
        }
        Mode::Array6(src) => {
            merge_array46_same_lgk(dst, src.num_registers(), |slot| src.get(slot));
        }
        Mode::Array4(src) => {
            merge_array46_same_lgk(dst, src.num_registers(), |slot| src.get(slot));
        }
        _ => {
            unreachable!("merge_array_same_lgk called with non-array mode; List/Set not supported")
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
    dst.rebuild_estimator_from_registers();
}

/// Multiple source registers map to each destination register via masking.
/// Merging independent register histories invalidates the HIP accumulator.
fn merge_array_with_downsample(dst: &mut Array8, dst_lg_k: u8, src_mode: &Mode, src_lg_k: u8) {
    assert!(
        src_lg_k > dst_lg_k,
        "merge_array_with_downsample requires src_lg_k > dst_lg_k (got src={}, dst={})",
        src_lg_k,
        dst_lg_k
    );

    match src_mode {
        Mode::Array8(src) => {
            dst.merge_array_with_downsample(src.values(), src_lg_k);
        }
        Mode::Array6(src) => {
            merge_array46_with_downsample(dst, dst_lg_k, src.num_registers(), |slot| src.get(slot));
        }
        Mode::Array4(src) => {
            merge_array46_with_downsample(dst, dst_lg_k, src.num_registers(), |slot| src.get(slot));
        }
        _ => unreachable!(
            "merge_array_with_downsample called with non-array mode; List/Set not supported"
        ),
    }
}

/// Changing representation preserves the estimate state; replaying registers alone would change
/// HIP.
fn convert_array8_to_type(src: &Array8, lg_config_k: u8, target_type: HllType) -> HllSketch {
    let estimate_state = src.estimate_state();
    match target_type {
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
            array6.restore_estimate_state(estimate_state);

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
            array4.restore_estimate_state(estimate_state);

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
fn copy_or_downsample(src_mode: &Mode, src_lg_k: u8, tgt_lg_k: u8) -> Array8 {
    let estimate_state = get_array_estimate_state(src_mode);

    let mut result = if src_lg_k <= tgt_lg_k {
        let mut result = Array8::new(src_lg_k);

        match src_mode {
            Mode::Array8(src) => {
                result.merge_array_same_lgk(src.values());
            }
            Mode::Array6(src) => {
                copy_array46_via_coupons(&mut result, src.num_registers(), |slot| src.get(slot));
            }
            Mode::Array4(src) => {
                copy_array46_via_coupons(&mut result, src.num_registers(), |slot| src.get(slot));
            }
            Mode::List { .. } | Mode::Set { .. } => {
                unreachable!(
                    "copy_or_downsample called with non-array mode; List/Set not supported"
                );
            }
        }

        result
    } else {
        let mut result = Array8::new(tgt_lg_k);
        merge_array_with_downsample(&mut result, tgt_lg_k, src_mode, src_lg_k);
        result
    };

    result.restore_estimate_state(estimate_state);
    result
}
