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

//! HyperLogLog Union for combining multiple HLL sketches
//!
//! The HLL Union allows combining multiple HLL sketches into a single unified
//! sketch, enabling set union operations for cardinality estimation.
//!
//! # Overview
//!
//! The union maintains an internal "gadget" sketch that accumulates the union
//! of all input sketches. It can handle sketches with:
//! - Different lg_k values (automatically resizes as needed)
//! - Different modes (List, Set, Array4/6/8)
//! - Different target HLL types

use crate::hll::array4::Array4;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
use crate::hll::mode::Mode;
use crate::hll::{HllSketch, HllType, pack_coupon};

/// An HLL Union for combining multiple HLL sketches.
///
/// The union maintains an internal sketch (the "gadget") that accumulates
/// the union of all input sketches. It automatically handles sketches with
/// different configurations and modes.
#[derive(Debug, Clone)]
pub struct HllUnion {
    /// Maximum lg_k that this union can handle
    lg_max_k: u8,
    /// Internal sketch that accumulates the union
    gadget: HllSketch,
}

impl HllUnion {
    /// Create a new HLL Union
    ///
    /// # Arguments
    ///
    /// * `lg_max_k` - Maximum log2 of the number of buckets. Must be in [4, 21].
    ///   This determines the maximum precision the union can handle. Input sketches
    ///   with larger lg_k will be down-sampled.
    ///
    /// # Panics
    ///
    /// Panics if `lg_max_k` is not in the range [4, 21].
    pub fn new(lg_max_k: u8) -> Self {
        assert!(
            (4..=21).contains(&lg_max_k),
            "lg_max_k must be in [4, 21], got {}",
            lg_max_k
        );

        // Start with an empty gadget at lg_max_k using Hll8
        let gadget = HllSketch::new(lg_max_k, HllType::Hll8);

        Self { lg_max_k, gadget }
    }

    /// Update the union with another sketch
    ///
    /// Merges the input sketch into the union's internal gadget, handling:
    /// - Sketches with different lg_k values (resizes/downsamples as needed)
    /// - Sketches in different modes (List, Set, Array4/6/8)
    /// - Sketches with different target HLL types
    pub fn update(&mut self, sketch: &HllSketch) {
        if sketch.is_empty() {
            return;
        }

        let src_lg_k = sketch.lg_config_k();
        let dst_lg_k = self.gadget.lg_config_k();
        let src_mode = sketch.mode();

        match src_mode {
            // Source is List or Set - iterate coupons into gadget
            Mode::List { .. } | Mode::Set { .. } => {
                merge_coupons_into_gadget(&mut self.gadget, src_mode);
            }

            // Source is Array - merge into gadget's Array8
            Mode::Array4(_) | Mode::Array6(_) | Mode::Array8(_) => {
                let is_gadget_array = matches!(self.gadget.mode(), Mode::Array8(_));

                if is_gadget_array {
                    // Both arrays - need to handle downsizing if necessary
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
                            _ => unreachable!(
                                "gadget mode changed unexpectedly; should never be Array4/Array6"
                            ),
                        }

                        merge_array_same_lgk(&mut new_array, src_mode);
                        self.gadget = HllSketch::from_mode(src_lg_k, Mode::Array8(new_array));
                    } else {
                        match self.gadget.mode_mut() {
                            Mode::Array8(dst_array) => {
                                merge_array_into_array8(dst_array, dst_lg_k, src_mode, src_lg_k);
                            }
                            _ => unreachable!(
                                "gadget mode changed unexpectedly; should never be Array4/Array6"
                            ),
                        }
                    }
                } else {
                    // Gadget is List/Set, source is Array - promote gadget
                    let mut new_array = copy_or_downsample(src_mode, src_lg_k, self.lg_max_k);

                    let old_gadget_mode = self.gadget.mode();
                    merge_coupons_into_mode(&mut new_array, old_gadget_mode);

                    let final_lg_k = new_array.num_registers().trailing_zeros() as u8;
                    self.gadget = HllSketch::from_mode(final_lg_k, Mode::Array8(new_array));
                }
            }
        }
    }

    /// Get the union result as a new sketch
    ///
    /// Returns a copy of the internal gadget sketch with the specified target HLL type.
    /// If the requested type differs from the gadget's type, conversion is performed.
    ///
    /// # Arguments
    ///
    /// * `hll_type` - The target HLL type for the result sketch (Hll4, Hll6, or Hll8)
    pub fn get_result(&self, hll_type: HllType) -> HllSketch {
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

    /// Reset the union to its initial empty state
    ///
    /// Clears all data from the internal gadget, allowing the union to be reused
    /// for a new set of operations.
    pub fn reset(&mut self) {
        self.gadget = HllSketch::new(self.lg_max_k, HllType::Hll8);
    }

    /// Check if the union is empty
    pub fn is_empty(&self) -> bool {
        self.gadget.is_empty()
    }

    /// Get the current cardinality estimate of the union
    pub fn estimate(&self) -> f64 {
        self.gadget.estimate()
    }

    /// Get the current lg_config_k of the internal gadget
    pub fn lg_config_k(&self) -> u8 {
        self.gadget.lg_config_k()
    }

    /// Get the maximum lg_k this union can handle
    pub fn lg_max_k(&self) -> u8 {
        self.lg_max_k
    }
}

/// Merge coupons from a List or Set mode into the gadget
///
/// Iterates over all coupons in the source and updates the gadget.
/// The gadget handles mode transitions automatically (List → Set → Array).
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

/// Merge coupons from a List or Set mode into an Array8
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

/// Merge an HLL array into an Array8
///
/// Handles merging from Array4, Array6, or Array8 sources. Dispatches based on lg_k:
/// - Same lg_k: optimized bulk merge
/// - src lg_k > dst lg_k: downsample src into dst
/// - src lg_k < dst lg_k: handled by caller (requires gadget replacement)
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

/// Extract HIP accumulator from an array mode
fn get_array_hip_accum(mode: &Mode) -> f64 {
    match mode {
        Mode::Array8(src) => src.hip_accum(),
        Mode::Array6(src) => src.hip_accum(),
        Mode::Array4(src) => src.hip_accum(),
        Mode::List { .. } | Mode::Set { .. } => {
            unreachable!("get_array_hip_accum called with non-array mode; List/Set not supported");
        }
    }
}

/// Merge Array4/Array6 into Array8 by iterating registers
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

/// Merge arrays with same lg_k
///
/// Takes the max of corresponding registers. HIP accumulator is invalidated by the merge.
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

/// Merge Array4/Array6 into Array8 with downsampling
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

/// Merge arrays with downsampling (src lg_k > dst lg_k)
///
/// Multiple source registers map to each destination register via masking.
/// HIP accumulator is invalidated by the merge.
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

/// Convert Array8 to a different HLL type
///
/// Creates a new sketch with the requested type by copying register values
/// from the Array8 source. Preserves the HIP accumulator.
fn convert_array8_to_type(src: &Array8, lg_config_k: u8, target_type: HllType) -> HllSketch {
    match target_type {
        HllType::Hll8 => HllSketch::from_mode(lg_config_k, Mode::Array8(src.clone())),
        HllType::Hll6 => {
            let mut array6 = Array6::new(lg_config_k);
            for slot in 0..src.num_registers() {
                let val = src.values()[slot];
                if val > 0 {
                    let clamped_val = val.min(63);
                    let coupon = pack_coupon(slot as u32, clamped_val);
                    array6.update(coupon);
                }
            }

            let src_est = src.estimate();
            let arr6_est = array6.estimate();
            if src_est > arr6_est {
                array6.set_hip_accum(src_est);
            }

            HllSketch::from_mode(lg_config_k, Mode::Array6(array6))
        }
        HllType::Hll4 => {
            let mut array4 = Array4::new(lg_config_k);
            for slot in 0..src.num_registers() {
                let val = src.values()[slot];
                if val > 0 {
                    let coupon = pack_coupon(slot as u32, val);
                    array4.update(coupon);
                }
            }

            let src_est = src.estimate();
            let arr4_est = array4.estimate();
            if src_est > arr4_est {
                array4.set_hip_accum(src_est);
            }

            HllSketch::from_mode(lg_config_k, Mode::Array4(array4))
        }
    }
}

/// Copy Array4/Array6 registers into Array8 by converting to coupons
fn copy_array46_via_coupons(dst: &mut Array8, num_registers: usize, get_value: impl Fn(u32) -> u8) {
    for slot in 0..num_registers {
        let val = get_value(slot as u32);
        if val > 0 {
            let coupon = pack_coupon(slot as u32, val);
            dst.update(coupon);
        }
    }
}

/// Copy or downsample a source array to create a new Array8
///
/// Directly copies if src_lg_k <= tgt_lg_k, downsamples otherwise.
/// Result is marked as out-of-order and HIP accumulator is preserved.
fn copy_or_downsample(src_mode: &Mode, src_lg_k: u8, tgt_lg_k: u8) -> Array8 {
    if src_lg_k <= tgt_lg_k {
        let mut result = Array8::new(src_lg_k);
        let src_hip = get_array_hip_accum(src_mode);

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

        result.set_hip_accum(src_hip);
        result
    } else {
        // Downsample from src to tgt
        let mut result = Array8::new(tgt_lg_k);
        merge_array_with_downsample(&mut result, tgt_lg_k, src_mode, src_lg_k);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_basic_list_mode() {
        // Test the simplest case: union of two sketches in List mode
        let mut union = HllUnion::new(12);

        // Create first sketch and add some values
        let mut sketch1 = HllSketch::new(12, HllType::Hll8);
        sketch1.update("foo");
        sketch1.update("bar");
        sketch1.update("baz");

        // Create second sketch with overlapping and new values
        let mut sketch2 = HllSketch::new(12, HllType::Hll8);
        sketch2.update("bar"); // duplicate
        sketch2.update("qux"); // new
        sketch2.update("quux"); // new

        // Union them
        union.update(&sketch1);
        union.update(&sketch2);

        // Get result
        let result = union.get_result(HllType::Hll8);

        // Should have ~5 unique values (foo, bar, baz, qux, quux)
        let estimate = result.estimate();
        assert!(
            (4.0..=6.0).contains(&estimate),
            "Expected estimate around 5, got {}",
            estimate
        );

        // Should not be empty
        assert!(!result.is_empty());
    }

    #[test]
    fn test_union_empty_sketch() {
        let mut union = HllUnion::new(10);
        let empty_sketch = HllSketch::new(10, HllType::Hll8);

        // Updating with empty sketch should not panic
        union.update(&empty_sketch);

        // Union should still be empty
        assert!(union.is_empty());
    }

    #[test]
    fn test_union_estimate_accuracy() {
        let mut union = HllUnion::new(12);

        // Add 1000 unique values across multiple sketches
        // This will cause sketches to promote to Array mode
        let mut sketch1 = HllSketch::new(12, HllType::Hll8);
        for i in 0..500 {
            sketch1.update(i);
        }

        let mut sketch2 = HllSketch::new(12, HllType::Hll8);
        for i in 400..900 {
            // 400-500 overlap with sketch1
            sketch2.update(i);
        }

        union.update(&sketch1);
        union.update(&sketch2);

        let result = union.get_result(HllType::Hll8);
        let estimate = result.estimate();

        // Should estimate ~900 unique values (0-899)
        // With lg_k=12, we expect ~1.6% relative error
        // So estimate should be within 900 ± 50 or so
        assert!(
            estimate > 800.0 && estimate < 1000.0,
            "Expected estimate around 900, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_array_to_array_same_lgk() {
        // Test merging two Array mode sketches with same lg_k
        let mut union = HllUnion::new(12);

        // Create two sketches that will be in Array mode (add enough values)
        let mut sketch1 = HllSketch::new(12, HllType::Hll8);
        for i in 0..10_000 {
            sketch1.update(i);
        }

        let mut sketch2 = HllSketch::new(12, HllType::Hll8);
        for i in 5_000..15_000 {
            sketch2.update(i);
        }

        // Both should be in Array mode now
        assert!(matches!(sketch1.mode(), Mode::Array8(_)));
        assert!(matches!(sketch2.mode(), Mode::Array8(_)));

        union.update(&sketch1);
        union.update(&sketch2);

        let result = union.get_result(HllType::Hll8);
        let estimate = result.estimate();

        // Should estimate ~15,000 unique values (0-14,999)
        // With lg_k=12, we expect ~1.6% relative error
        assert!(
            estimate > 14_000.0 && estimate < 16_000.0,
            "Expected estimate around 15000, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_downsampling_src_larger() {
        // Test src_lg_k > dst_lg_k (downsampling)
        let mut union = HllUnion::new(10); // Union at lg_k=10

        // Create sketch at lg_k=12 (higher precision)
        let mut sketch = HllSketch::new(12, HllType::Hll8);
        for i in 0..5_000 {
            sketch.update(i);
        }

        // Union should downsample sketch to lg_k=10
        union.update(&sketch);

        let result = union.get_result(HllType::Hll8);
        let estimate = result.estimate();

        // Should still estimate ~5,000 unique values
        assert!(
            estimate > 4_000.0 && estimate < 6_000.0,
            "Expected estimate around 5000, got {}",
            estimate
        );
        assert_eq!(result.lg_config_k(), 10, "Result should be at lg_k=10");
    }

    #[test]
    fn test_union_gadget_downsizing_src_smaller() {
        // Test src_lg_k < dst_lg_k (gadget downsizing)
        let mut union = HllUnion::new(12);

        // First update with lg_k=12 sketch to establish gadget at lg_k=12
        let mut sketch1 = HllSketch::new(12, HllType::Hll8);
        for i in 0..10_000 {
            sketch1.update(i);
        }
        union.update(&sketch1);
        assert_eq!(union.lg_config_k(), 12, "Gadget should be at lg_k=12");

        // Now update with lg_k=10 sketch (lower precision)
        let mut sketch2 = HllSketch::new(10, HllType::Hll8);
        for i in 5_000..15_000 {
            sketch2.update(i);
        }

        // This should trigger gadget downsizing to lg_k=10
        union.update(&sketch2);

        let result = union.get_result(HllType::Hll8);
        let estimate = result.estimate();

        // Should estimate ~15,000 unique values
        assert!(
            estimate > 13_000.0 && estimate < 17_000.0,
            "Expected estimate around 15000, got {}",
            estimate
        );
        assert_eq!(
            result.lg_config_k(),
            10,
            "Gadget should have downsized to lg_k=10"
        );
    }

    #[test]
    fn test_union_list_to_array() {
        // Test union where first sketch is List and second is Array
        let mut union = HllUnion::new(12);

        // First sketch: small (List mode)
        let mut sketch1 = HllSketch::new(12, HllType::Hll8);
        sketch1.update("a");
        sketch1.update("b");
        sketch1.update("c");
        assert!(matches!(sketch1.mode(), Mode::List { .. }));

        // Second sketch: large (Array mode)
        let mut sketch2 = HllSketch::new(12, HllType::Hll8);
        for i in 0..10_000 {
            sketch2.update(i);
        }
        assert!(matches!(sketch2.mode(), Mode::Array8(_)));

        union.update(&sketch1);
        union.update(&sketch2);

        let result = union.get_result(HllType::Hll8);
        let estimate = result.estimate();

        // Should estimate ~10,003 unique values
        assert!(
            estimate > 9_500.0 && estimate < 10_500.0,
            "Expected estimate around 10000, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_array_to_list() {
        // Test union where first sketch is Array and second is List
        let mut union = HllUnion::new(12);

        // First sketch: large (Array mode)
        let mut sketch1 = HllSketch::new(12, HllType::Hll8);
        for i in 0..10_000 {
            sketch1.update(i);
        }
        assert!(matches!(sketch1.mode(), Mode::Array8(_)));

        // Second sketch: small (List mode)
        let mut sketch2 = HllSketch::new(12, HllType::Hll8);
        sketch2.update("a");
        sketch2.update("b");
        sketch2.update("c");
        assert!(matches!(sketch2.mode(), Mode::List { .. }));

        union.update(&sketch1);
        union.update(&sketch2);

        let result = union.get_result(HllType::Hll8);
        let estimate = result.estimate();

        // Should estimate ~10,003 unique values
        assert!(
            estimate > 9_500.0 && estimate < 10_500.0,
            "Expected estimate around 10000, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_mixed_hll_types() {
        // Test union with different HLL types (Hll4, Hll6, Hll8)
        let mut union = HllUnion::new(12);

        // Sketch with Hll4
        let mut sketch1 = HllSketch::new(12, HllType::Hll4);
        for i in 0..3_000 {
            sketch1.update(i);
        }

        // Sketch with Hll6
        let mut sketch2 = HllSketch::new(12, HllType::Hll6);
        for i in 2_000..5_000 {
            sketch2.update(i);
        }

        // Sketch with Hll8
        let mut sketch3 = HllSketch::new(12, HllType::Hll8);
        for i in 4_000..7_000 {
            sketch3.update(i);
        }

        union.update(&sketch1);
        union.update(&sketch2);
        union.update(&sketch3);

        let result = union.get_result(HllType::Hll8);
        let estimate = result.estimate();

        // Should estimate ~7,000 unique values (0-6,999)
        assert!(
            estimate > 6_000.0 && estimate < 8_000.0,
            "Expected estimate around 7000, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_multiple_downsizing_operations() {
        // Test multiple gadget downsizing operations
        let mut union = HllUnion::new(12);

        // Start with lg_k=12
        let mut sketch1 = HllSketch::new(12, HllType::Hll8);
        for i in 0..5_000 {
            sketch1.update(i);
        }
        union.update(&sketch1);
        assert_eq!(union.lg_config_k(), 12);

        // Downsize to lg_k=10
        let mut sketch2 = HllSketch::new(10, HllType::Hll8);
        for i in 4_000..8_000 {
            sketch2.update(i);
        }
        union.update(&sketch2);
        assert_eq!(union.lg_config_k(), 10);

        // Downsize again to lg_k=8
        let mut sketch3 = HllSketch::new(8, HllType::Hll8);
        for i in 7_000..10_000 {
            sketch3.update(i);
        }
        union.update(&sketch3);
        assert_eq!(union.lg_config_k(), 8);

        let result = union.get_result(HllType::Hll8);
        let estimate = result.estimate();

        // Should estimate ~10,000 unique values (0-9,999)
        // Lower precision means higher error
        assert!(
            estimate > 8_000.0 && estimate < 12_000.0,
            "Expected estimate around 10000, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_get_result_type_conversion_hll6() {
        // Test getting result as Hll6
        let mut union = HllUnion::new(12);

        let mut sketch = HllSketch::new(12, HllType::Hll8);
        for i in 0..5_000 {
            sketch.update(i);
        }

        union.update(&sketch);

        // Get result as Hll6
        let result = union.get_result(HllType::Hll6);

        // Verify it's Hll6
        assert_eq!(result.target_type(), HllType::Hll6);

        // Estimate should be similar
        let estimate = result.estimate();
        assert!(
            estimate > 4_000.0 && estimate < 6_000.0,
            "Expected estimate around 5000, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_get_result_type_conversion_hll4() {
        // Test getting result as Hll4
        let mut union = HllUnion::new(12);

        let mut sketch = HllSketch::new(12, HllType::Hll8);
        for i in 0..5_000 {
            sketch.update(i);
        }

        union.update(&sketch);

        // Get result as Hll4
        let result = union.get_result(HllType::Hll4);

        // Verify it's Hll4
        assert_eq!(result.target_type(), HllType::Hll4);

        // Estimate should be similar (Hll4 may have slightly different precision)
        let estimate = result.estimate();
        assert!(
            estimate > 4_000.0 && estimate < 6_000.0,
            "Expected estimate around 5000, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_get_result_no_conversion_needed() {
        // Test that requesting Hll8 when gadget is Hll8 just clones
        let mut union = HllUnion::new(12);

        let mut sketch = HllSketch::new(12, HllType::Hll8);
        for i in 0..1_000 {
            sketch.update(i);
        }

        union.update(&sketch);

        // Get result as Hll8 (no conversion needed)
        let result = union.get_result(HllType::Hll8);

        // Verify it's Hll8
        assert_eq!(result.target_type(), HllType::Hll8);

        // Estimate should match
        let estimate = result.estimate();
        assert!(
            estimate > 900.0 && estimate < 1_100.0,
            "Expected estimate around 1000, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_get_result_from_list_mode() {
        // Test type conversion when gadget is still in List mode
        let mut union = HllUnion::new(12);

        // Add just a few values so gadget stays in List mode
        let mut sketch = HllSketch::new(12, HllType::Hll8);
        sketch.update("a");
        sketch.update("b");
        sketch.update("c");

        union.update(&sketch);

        // Get result as Hll6 - should just change the target type
        let result = union.get_result(HllType::Hll6);

        assert_eq!(result.target_type(), HllType::Hll6);
        assert!(matches!(result.mode(), Mode::List { .. }));

        let estimate = result.estimate();
        assert!(
            (3.0..=5.0).contains(&estimate),
            "Expected estimate around 3, got {}",
            estimate
        );
    }

    #[test]
    fn test_union_hll6_arrays_with_overlap() {
        // Test unioning Hll6 sketches (which will be in Array6 mode)
        let mut union = HllUnion::new(12);

        let mut sketch1 = HllSketch::new(12, HllType::Hll6);
        for i in 0..10_000 {
            sketch1.update(i);
        }

        let mut sketch2 = HllSketch::new(12, HllType::Hll6);
        for i in 5_000..15_000 {
            sketch2.update(i);
        }

        union.update(&sketch1);
        union.update(&sketch2);

        let result = union.get_result(HllType::Hll6);
        let estimate = result.estimate();

        // Should estimate ~15,000 unique values (0-14,999)
        // Both sketches are Hll6, so we expect the full union
        assert!(
            estimate > 13_000.0 && estimate < 17_000.0,
            "Expected estimate around 15000, got {}. This suggests sketch2 overwrote sketch1 instead of merging.",
            estimate
        );
    }
}
