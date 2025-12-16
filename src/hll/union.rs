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
//!
//! # Example
//!
//! ```ignore
//! use datasketches::hll::{HllSketch, HllUnion, HllType};
//!
//! // Create a union with lg_max_k = 12
//! let mut union = HllUnion::new(12);
//!
//! // Create and update some sketches
//! let mut sketch1 = HllSketch::new(12, HllType::Hll8);
//! sketch1.update("foo");
//! sketch1.update("bar");
//!
//! let mut sketch2 = HllSketch::new(12, HllType::Hll8);
//! sketch2.update("bar");
//! sketch2.update("baz");
//!
//! // Union the sketches
//! union.update(&sketch1);
//! union.update(&sketch2);
//!
//! // Get the result (should estimate ~3 unique items)
//! let result = union.get_result(HllType::Hll8);
//! println!("Union estimate: {}", result.estimate());
//! ```

use crate::hll::{HllSketch, HllType};

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
    ///
    /// # Example
    ///
    /// ```ignore
    /// let union = HllUnion::new(12); // Can handle sketches up to lg_k=12
    /// ```
    pub fn new(lg_max_k: u8) -> Self {
        assert!(
            lg_max_k >= 4 && lg_max_k <= 21,
            "lg_max_k must be in [4, 21], got {}",
            lg_max_k
        );

        // Start with an empty gadget at lg_max_k using Hll8 (default)
        // We'll use Hll8 as the default target type for the gadget
        let gadget = HllSketch::new(lg_max_k, HllType::Hll8);

        Self { lg_max_k, gadget }
    }

    /// Update the union with another sketch
    ///
    /// This merges the input sketch into the union's internal gadget.
    /// The method handles:
    /// - Sketches with different lg_k values (resizes as needed)
    /// - Sketches in different modes (List, Set, Array)
    /// - Sketches with different target HLL types
    ///
    /// # Arguments
    ///
    /// * `sketch` - The sketch to merge into the union
    ///
    /// # Algorithm
    ///
    /// 1. If sketch lg_k > union lg_max_k: down-sample the sketch data
    /// 2. If sketch lg_k < gadget lg_k: keep gadget (higher precision)
    /// 3. If sketch lg_k > gadget lg_k: upsize gadget to match
    /// 4. Merge the data based on modes:
    ///    - List/Set mode: iterate coupons and update gadget
    ///    - Array mode: merge registers (take max of each pair)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut union = HllUnion::new(12);
    /// let sketch = HllSketch::new(10, HllType::Hll6);
    /// union.update(&sketch);
    /// ```
    pub fn update(&mut self, sketch: &HllSketch) {
        use crate::hll::mode::Mode;

        // Early return if source is empty
        if sketch.is_empty() {
            return;
        }

        let src_lg_k = sketch.lg_config_k();
        let dst_lg_k = self.gadget.lg_config_k();
        let src_mode = sketch.mode();

        // Match on source mode to determine strategy
        match src_mode {
            // Case 1: Source is List or Set - iterate coupons into gadget
            Mode::List { .. } | Mode::Set { .. } => {
                merge_coupons_into_gadget(&mut self.gadget, src_mode);
            }

            // Case 2: Source is Array - merge into gadget's Array8
            Mode::Array4(_) | Mode::Array6(_) | Mode::Array8(_) => {
                // Check gadget mode
                let is_gadget_array = matches!(self.gadget.mode(), Mode::Array8(_));

                if is_gadget_array {
                    // Both arrays - need to handle downsizing if necessary
                    if src_lg_k < dst_lg_k {
                        // Source has lower precision - must downsize gadget
                        // This mirrors C++ HllUnion-internal.hpp lines 252-260

                        // Step 1: Create new Array8 at src_lg_k
                        let mut new_array = crate::hll::array8::Array8::new(src_lg_k);

                        // Step 2: Downsample current gadget into new array
                        if let Mode::Array8(old_gadget) = self.gadget.mode() {
                            merge_array_with_downsample(&mut new_array, src_lg_k, &Mode::Array8(old_gadget.clone()), dst_lg_k);
                        }

                        // Step 3: Merge source into new array
                        merge_array_same_lgk(&mut new_array, src_mode);

                        // Step 4: Replace gadget
                        self.gadget = HllSketch::from_mode(src_lg_k, Mode::Array8(new_array));
                    } else {
                        // Standard merge: src_lg_k >= dst_lg_k
                        let dst_mode = self.gadget.mode_mut();
                        if let Mode::Array8(dst_array) = dst_mode {
                            merge_array_into_array8(dst_array, dst_lg_k, src_mode, src_lg_k);
                        }
                    }
                } else {
                    // Gadget is List/Set, source is Array - promote gadget
                    // This mirrors C++ union_impl lines 243-250

                    // Step 1: Copy/downsample source to create new Array8
                    let mut new_array = copy_or_downsample(src_mode, src_lg_k, self.lg_max_k);

                    // Step 2: Merge gadget's coupons into the new array
                    let old_gadget_mode = self.gadget.mode();
                    merge_coupons_into_mode(&mut new_array, old_gadget_mode);

                    // Step 3: Replace gadget with new Array8
                    let final_lg_k = new_array.num_registers().trailing_zeros() as u8;
                    self.gadget = HllSketch::from_mode(
                        final_lg_k,
                        Mode::Array8(new_array),
                    );
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
    ///
    /// # Returns
    ///
    /// A new HllSketch containing the union of all input sketches, converted to the
    /// requested HLL type if necessary.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut union = HllUnion::new(12);
    /// // ... update with sketches ...
    /// let result = union.get_result(HllType::Hll6); // Get result as Hll6
    /// ```
    pub fn get_result(&self, hll_type: HllType) -> HllSketch {
        use crate::hll::mode::Mode;

        let gadget_type = self.gadget.target_type();

        // If requested type matches gadget type, just clone
        if hll_type == gadget_type {
            return self.gadget.clone();
        }

        // Type conversion needed
        match self.gadget.mode() {
            // List/Set modes: just change the target type
            Mode::List { list, .. } => {
                HllSketch::from_mode(
                    self.gadget.lg_config_k(),
                    Mode::List {
                        list: list.clone(),
                        hll_type,
                    },
                )
            }
            Mode::Set { set, .. } => {
                HllSketch::from_mode(
                    self.gadget.lg_config_k(),
                    Mode::Set {
                        set: set.clone(),
                        hll_type,
                    },
                )
            }
            // Array8 mode: convert to requested array type
            Mode::Array8(array8) => {
                convert_array8_to_type(array8, self.gadget.lg_config_k(), hll_type)
            }
            // Array4/6 should never occur in gadget (always Hll8)
            Mode::Array4(_) | Mode::Array6(_) => {
                // Shouldn't happen, but handle gracefully
                self.gadget.clone()
            }
        }
    }

    /// Reset the union to its initial empty state
    ///
    /// Clears all data from the internal gadget, allowing the union to be reused
    /// for a new set of operations.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut union = HllUnion::new(12);
    /// // ... update with sketches ...
    /// union.reset(); // Clear everything and start fresh
    /// ```
    pub fn reset(&mut self) {
        // Recreate the gadget as empty
        self.gadget = HllSketch::new(self.lg_max_k, HllType::Hll8);
    }

    /// Check if the union is empty (no sketches have been added)
    ///
    /// # Returns
    ///
    /// `true` if no sketches have been added to the union, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.gadget.is_empty()
    }

    /// Get the current cardinality estimate of the union
    ///
    /// # Returns
    ///
    /// The estimated number of unique elements across all unioned sketches
    pub fn estimate(&self) -> f64 {
        self.gadget.estimate()
    }

    /// Get the current lg_config_k of the internal gadget
    ///
    /// # Returns
    ///
    /// The log2 of the number of buckets in the internal gadget
    pub fn lg_config_k(&self) -> u8 {
        self.gadget.lg_config_k()
    }

    /// Get the maximum lg_k this union can handle
    ///
    /// # Returns
    ///
    /// The lg_max_k value specified when creating the union
    pub fn lg_max_k(&self) -> u8 {
        self.lg_max_k
    }
}

/// Merge coupons from a List or Set mode sketch into the gadget
///
/// This iterates over all coupons in the source container and updates
/// the gadget sketch with each one. The gadget handles mode transitions
/// automatically (List → Set → Array).
///
/// This mirrors the C++ implementation's coupon iteration approach.
fn merge_coupons_into_gadget(gadget: &mut HllSketch, src_mode: &crate::hll::mode::Mode) {
    use crate::hll::mode::Mode;

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
        _ => {
            // Array modes don't have coupons to iterate
            // This shouldn't be called for array modes
        }
    }
}

/// Merge coupons from a List or Set mode directly into an Array8
///
/// Similar to merge_coupons_into_gadget, but works directly with an Array8
/// instead of going through an HllSketch.
fn merge_coupons_into_mode(dst: &mut crate::hll::array8::Array8, src_mode: &crate::hll::mode::Mode) {
    use crate::hll::mode::Mode;

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
        _ => {
            // Array modes don't have coupons
        }
    }
}

/// Merge an HLL array into an Array8 gadget
///
/// This handles merging from Array4, Array6, or Array8 sources into an
/// Array8 destination. It dispatches based on lg_k relationship:
/// - Same lg_k: optimized bulk merge
/// - src lg_k > dst lg_k: downsample src into dst
///
/// Note: The case where src_lg_k < dst_lg_k is handled by the caller
/// (requires gadget replacement, not in-place merge).
///
/// This mirrors the C++ Hll8Array::mergeHll() implementation.
fn merge_array_into_array8(
    dst_array8: &mut crate::hll::array8::Array8,
    dst_lg_k: u8,
    src_mode: &crate::hll::mode::Mode,
    src_lg_k: u8,
) {
    assert!(
        src_lg_k >= dst_lg_k,
        "merge_array_into_array8 requires src_lg_k >= dst_lg_k (got src={}, dst={})",
        src_lg_k,
        dst_lg_k
    );

    if dst_lg_k == src_lg_k {
        // Same lg_k: use optimized bulk merge
        merge_array_same_lgk(dst_array8, src_mode);
    } else {
        // src_lg_k > dst_lg_k: downsample from src to dst
        merge_array_with_downsample(dst_array8, dst_lg_k, src_mode, src_lg_k);
    }
}

/// Merge arrays with same lg_k
///
/// For same lg_k, we can use the optimized merge methods that directly
/// take the max of corresponding registers. Also combines HIP accumulators.
fn merge_array_same_lgk(dst: &mut crate::hll::array8::Array8, src_mode: &crate::hll::mode::Mode) {
    use crate::hll::mode::Mode;

    // Get source HIP accumulator
    let src_hip = match src_mode {
        Mode::Array8(src) => src.hip_accum(),
        Mode::Array6(src) => src.hip_accum(),
        Mode::Array4(src) => src.hip_accum(),
        _ => unreachable!("Only array modes should be passed to merge_array_same_lgk"),
    };

    let dst_hip = dst.hip_accum();

    match src_mode {
        Mode::Array8(src) => {
            // Array8 → Array8: use optimized bulk merge
            dst.merge_array_same_lgk(src.values());
        }
        Mode::Array6(src) => {
            // Array6 → Array8: read and merge slot by slot
            // Use direct register modification to avoid estimator inconsistency
            for slot in 0..src.num_registers() {
                let val = src.get(slot as u32);
                let current = dst.values()[slot];
                if val > current {
                    dst.set_register(slot, val);
                }
            }
            // Rebuild estimator state from the modified registers
            dst.rebuild_estimator_from_registers();
        }
        Mode::Array4(src) => {
            // Array4 → Array8: read adjusted values and merge
            // Use direct register modification to avoid estimator inconsistency
            for slot in 0..src.num_registers() {
                let val = src.get(slot as u32);
                let current = dst.values()[slot];
                if val > current {
                    dst.set_register(slot, val);
                }
            }
            // Rebuild estimator state from the modified registers
            dst.rebuild_estimator_from_registers();
        }
        _ => unreachable!("Only array modes should be passed to merge_array_same_lgk"),
    }

    // Combine HIP accumulators: take max
    // This mirrors C++ HllUnion-internal.hpp line ~225
    if src_hip > dst_hip {
        dst.set_hip_accum(src_hip);
    }
}

/// Merge arrays with downsampling (src lg_k > dst lg_k)
///
/// When source has higher precision, multiple source registers map to
/// each destination register via masking: dst_slot = src_slot & dst_mask
/// Also combines HIP accumulators.
fn merge_array_with_downsample(
    dst: &mut crate::hll::array8::Array8,
    dst_lg_k: u8,
    src_mode: &crate::hll::mode::Mode,
    src_lg_k: u8,
) {
    use crate::hll::mode::Mode;

    assert!(src_lg_k > dst_lg_k, "This function requires src_lg_k > dst_lg_k");

    // Get source HIP accumulator
    let src_hip = match src_mode {
        Mode::Array8(src) => src.hip_accum(),
        Mode::Array6(src) => src.hip_accum(),
        Mode::Array4(src) => src.hip_accum(),
        _ => unreachable!("Only array modes should be passed to merge_array_with_downsample"),
    };

    let dst_hip = dst.hip_accum();

    match src_mode {
        Mode::Array8(src) => {
            // Array8 → Array8 with downsampling: use optimized method
            dst.merge_array_with_downsample(src.values(), src_lg_k);
        }
        Mode::Array6(src) => {
            // Array6 → Array8 with downsampling
            // Use direct register modification to avoid estimator inconsistency
            let dst_mask = (1 << dst_lg_k) - 1;
            for src_slot in 0..src.num_registers() {
                let val = src.get(src_slot as u32);
                if val > 0 {
                    let dst_slot = (src_slot as u32 & dst_mask) as usize;
                    let current = dst.values()[dst_slot];
                    if val > current {
                        dst.set_register(dst_slot, val);
                    }
                }
            }
            // Rebuild estimator state from the modified registers
            dst.rebuild_estimator_from_registers();
        }
        Mode::Array4(src) => {
            // Array4 → Array8 with downsampling
            // Use direct register modification to avoid estimator inconsistency
            let dst_mask = (1 << dst_lg_k) - 1;
            for src_slot in 0..src.num_registers() {
                let val = src.get(src_slot as u32);
                if val > 0 {
                    let dst_slot = (src_slot as u32 & dst_mask) as usize;
                    let current = dst.values()[dst_slot];
                    if val > current {
                        dst.set_register(dst_slot, val);
                    }
                }
            }
            // Rebuild estimator state from the modified registers
            dst.rebuild_estimator_from_registers();
        }
        _ => unreachable!("Only array modes should be passed to merge_array_with_downsample"),
    }

    // Combine HIP accumulators: take max
    if src_hip > dst_hip {
        dst.set_hip_accum(src_hip);
    }
}

/// Convert Array8 to a different HLL type
///
/// Creates a new sketch with the requested type by copying register values
/// from the Array8 source. Preserves the HIP accumulator.
fn convert_array8_to_type(
    src: &crate::hll::array8::Array8,
    lg_config_k: u8,
    target_type: HllType,
) -> HllSketch {
    use crate::hll::mode::Mode;

    match target_type {
        HllType::Hll8 => {
            // Just clone as Array8
            HllSketch::from_mode(lg_config_k, Mode::Array8(src.clone()))
        }
        HllType::Hll6 => {
            // Convert Array8 → Array6
            // Simply copy all registers - Array6 uses same byte-per-register but with 6-bit packing
            let mut array6 = crate::hll::array6::Array6::new(lg_config_k);

            // Copy all register values by simulating a merge
            for slot in 0..src.num_registers() {
                let val = src.values()[slot];
                if val > 0 {
                    let clamped_val = val.min(63); // Array6 max value is 63
                    let coupon = crate::hll::pack_coupon(slot as u32, clamped_val);
                    array6.update(coupon);
                }
            }

            // Now the array6 has all the register values and its estimator is properly computed
            // But we want to preserve the source's estimate for accuracy
            // Take the max of the two estimates
            let src_est = src.estimate();
            let arr6_est = array6.estimate();
            if src_est > arr6_est {
                array6.set_hip_accum(src_est);
            }

            HllSketch::from_mode(lg_config_k, Mode::Array6(array6))
        }
        HllType::Hll4 => {
            // Convert Array8 → Array4
            let mut array4 = crate::hll::array4::Array4::new(lg_config_k);

            // Copy all register values
            for slot in 0..src.num_registers() {
                let val = src.values()[slot];
                if val > 0 {
                    let coupon = crate::hll::pack_coupon(slot as u32, val);
                    array4.update(coupon);
                }
            }

            // Preserve the source's estimate for accuracy
            let src_est = src.estimate();
            let arr4_est = array4.estimate();
            if src_est > arr4_est {
                array4.set_hip_accum(src_est);
            }

            HllSketch::from_mode(lg_config_k, Mode::Array4(array4))
        }
    }
}

/// Copy or downsample a source array to create a new Array8
///
/// If src_lg_k <= tgt_lg_k: direct copy
/// If src_lg_k > tgt_lg_k: downsample to tgt_lg_k
///
/// This mirrors the C++ copy_or_downsample function. The result is always
/// marked as out-of-order and HIP accumulator is preserved from source.
fn copy_or_downsample(
    src_mode: &crate::hll::mode::Mode,
    src_lg_k: u8,
    tgt_lg_k: u8,
) -> crate::hll::array8::Array8 {
    use crate::hll::mode::Mode;

    if src_lg_k <= tgt_lg_k {
        // Direct copy - no downsampling needed
        let mut result = crate::hll::array8::Array8::new(src_lg_k);

        // Get the source's HIP accumulator value to preserve
        let src_hip = match src_mode {
            Mode::Array8(src) => src.hip_accum(),
            Mode::Array6(src) => src.hip_accum(),
            Mode::Array4(src) => src.hip_accum(),
            _ => unreachable!("Only array modes should be passed"),
        };

        match src_mode {
            Mode::Array8(src) => {
                result.merge_array_same_lgk(src.values());
            }
            Mode::Array6(src) => {
                for slot in 0..src.num_registers() {
                    let val = src.get(slot as u32);
                    if val > 0 {
                        let coupon = crate::hll::pack_coupon(slot as u32, val);
                        result.update(coupon);
                    }
                }
            }
            Mode::Array4(src) => {
                for slot in 0..src.num_registers() {
                    let val = src.get(slot as u32);
                    if val > 0 {
                        let coupon = crate::hll::pack_coupon(slot as u32, val);
                        result.update(coupon);
                    }
                }
            }
            _ => unreachable!("Only array modes should be passed"),
        }

        // Preserve HIP accumulator from source
        result.set_hip_accum(src_hip);
        result
    } else {
        // Downsample from src to tgt
        let mut result = crate::hll::array8::Array8::new(tgt_lg_k);

        // merge_array_with_downsample will handle HIP accumulator combination
        merge_array_with_downsample(&mut result, tgt_lg_k, src_mode, src_lg_k);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_new() {
        let union = HllUnion::new(12);
        assert_eq!(union.lg_max_k(), 12);
        assert_eq!(union.lg_config_k(), 12);
        assert!(union.is_empty());
    }

    #[test]
    #[should_panic(expected = "lg_max_k must be in [4, 21]")]
    fn test_union_new_invalid_lg_k_low() {
        let _union = HllUnion::new(3);
    }

    #[test]
    #[should_panic(expected = "lg_max_k must be in [4, 21]")]
    fn test_union_new_invalid_lg_k_high() {
        let _union = HllUnion::new(22);
    }

    #[test]
    fn test_union_reset() {
        let mut union = HllUnion::new(10);
        // Even without updates, reset should work
        union.reset();
        assert!(union.is_empty());
        assert_eq!(union.lg_config_k(), 10);
    }

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
            estimate >= 4.0 && estimate <= 6.0,
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
        assert!(matches!(sketch1.mode(), crate::hll::mode::Mode::Array8(_)));
        assert!(matches!(sketch2.mode(), crate::hll::mode::Mode::Array8(_)));

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
        assert!(matches!(sketch1.mode(), crate::hll::mode::Mode::List { .. }));

        // Second sketch: large (Array mode)
        let mut sketch2 = HllSketch::new(12, HllType::Hll8);
        for i in 0..10_000 {
            sketch2.update(i);
        }
        assert!(matches!(sketch2.mode(), crate::hll::mode::Mode::Array8(_)));

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
        assert!(matches!(sketch1.mode(), crate::hll::mode::Mode::Array8(_)));

        // Second sketch: small (List mode)
        let mut sketch2 = HllSketch::new(12, HllType::Hll8);
        sketch2.update("a");
        sketch2.update("b");
        sketch2.update("c");
        assert!(matches!(sketch2.mode(), crate::hll::mode::Mode::List { .. }));

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
        assert!(matches!(result.mode(), crate::hll::mode::Mode::List { .. }));

        let estimate = result.estimate();
        assert!(
            estimate >= 3.0 && estimate <= 5.0,
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