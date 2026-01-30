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

use crate::cpc::count_bits_set_in_matrix;
use crate::cpc::pair_table::PairTable;
use crate::cpc::{CpcSketch, determine_correct_offset};
use crate::cpc::{DEFAULT_LG_K, Flavor};
use crate::hash::DEFAULT_UPDATE_SEED;

/// The union (merge) operation for the CPC sketches.
#[derive(Debug, Clone)]
pub struct CpcUnion {
    // immutable config variables
    lg_k: u8,
    seed: u64,

    // union state
    state: UnionState,
}

impl Default for CpcUnion {
    fn default() -> Self {
        Self::new(DEFAULT_LG_K)
    }
}

impl CpcUnion {
    /// Creates a new `CpcUnion` with the given `lg_k` and default seed.
    pub fn new(lg_k: u8) -> Self {
        Self::with_seed(lg_k, DEFAULT_UPDATE_SEED)
    }

    /// Creates a new `CpcUnion` with the given `lg_k` and `seed`.
    pub fn with_seed(lg_k: u8, seed: u64) -> Self {
        // We begin with the accumulator holding an EMPTY_MERGED sketch object.
        let sketch = CpcSketch::with_seed(lg_k, seed);
        let state = UnionState::Accumulator(sketch);
        Self { lg_k, seed, state }
    }

    /// Return the parameter lg_k.
    ///
    /// Note that due to merging with source sketches that may have a lower value of LgK, this value
    /// can be less than what the union object was configured with.
    pub fn lg_k(&self) -> u8 {
        self.lg_k
    }

    /// Returns the result of union operations as a CPC sketch.
    pub fn get_result(&self) -> CpcSketch {
        match &self.state {
            UnionState::Accumulator(sketch) => {
                if sketch.is_empty() {
                    CpcSketch::with_seed(self.lg_k, self.seed)
                } else {
                    let mut sketch = sketch.clone();
                    assert_eq!(sketch.flavor(), Flavor::SPARSE);
                    sketch.merge_flag = true;
                    sketch
                }
            }
            UnionState::BitMatrix(matrix) => {
                let lg_k = self.lg_k;

                let mut sketch = CpcSketch::with_seed(lg_k, self.seed);
                let num_coupons = count_bits_set_in_matrix(matrix);
                sketch.num_coupons = num_coupons;
                let offset = determine_correct_offset(lg_k, num_coupons);
                sketch.window_offset = offset;

                // Build the window and pair table
                let k = 1 << lg_k;
                let mut sliding_window = vec![0u8; k];

                // LgSize = K/16; in some cases this will end up being oversized
                let new_table_lg_size = (lg_k - 4).max(2);
                let mut table = PairTable::new(new_table_lg_size, 6 + lg_k);

                // the following should work even when the offset is zero
                let mask_for_clearing_window = (0xFFu64 << offset) ^ u64::MAX;
                let mask_for_flipping_early_zone = (1u64 << offset) - 1;
                let mut all_surprises_ored = 0;

                // The snowplow effect was caused by processing the rows in order,
                // but we have fixed it by using a sufficiently large hash table.
                for i in 0..k {
                    let mut pattern = matrix[i];
                    sliding_window[i] = ((pattern >> offset) & 0xFF) as u8;
                    pattern &= mask_for_clearing_window;
                    pattern ^= mask_for_flipping_early_zone; // this flipping converts surprising 0's to 1's
                    all_surprises_ored |= pattern;
                    while pattern != 0 {
                        let col = pattern.trailing_zeros();
                        pattern = pattern ^ (1u64 << col); // erase the 1
                        let row_col = ((i as u32) << 6) | col;
                        let is_novel = table.maybe_insert(row_col);
                        assert!(is_novel);
                    }
                }

                // at this point we could shrink an oversized hash table, but the relative waste isn't very big
                sketch.first_interesting_column = all_surprises_ored.trailing_zeros() as u8;
                if sketch.first_interesting_column > offset {
                    sketch.first_interesting_column = offset; // corner case
                }

                // HIP-related fields will contain zeros, and that is okay since merge_flag is true
                sketch.sliding_window = sliding_window;
                sketch.surprising_value_table = Some(table);
                sketch.merge_flag = true;

                sketch
            }
        }
    }

    /// Update this union with a CpcSketch.
    pub fn update(&mut self, sketch: &CpcSketch) {
        assert_eq!(self.seed, sketch.seed());

        let flavor = sketch.flavor();
        if flavor == Flavor::EMPTY {
            return;
        }

        if sketch.lg_k() < self.lg_k {
            self.reduce_k(sketch.lg_k());
        }

        // if source is past SPARSE mode, make sure that union is a bitMatrix.
        if flavor > Flavor::SPARSE {
            if let UnionState::Accumulator(old_sketch) = &self.state {
                // convert the accumulator to a bit matrix
                let bit_matrix = old_sketch.build_bit_matrix();
                self.state = UnionState::BitMatrix(bit_matrix);
            }
        }

        match &mut self.state {
            UnionState::Accumulator(old_sketch) => {
                // [Case A] Sparse, bitMatrix == null, accumulator valid
                if flavor == Flavor::SPARSE {
                    let old_flavor = old_sketch.flavor();
                    if old_flavor != Flavor::SPARSE && old_flavor != Flavor::EMPTY {
                        unreachable!(
                            "unexpected old flavor in union accumulator: {:?}",
                            old_flavor
                        );
                    }

                    // The following partially fixes the snowplow problem provided that the K's
                    // are equal.
                    if old_flavor == Flavor::EMPTY && self.lg_k == sketch.lg_k() {
                        *old_sketch = sketch.clone();
                        return;
                    }

                    walk_table_updating_sketch(old_sketch, sketch.surprising_value_table());
                    let final_flavor = old_sketch.flavor();

                    // if the accumulator has graduated beyond sparse, switch to a bit matrix
                    // representation
                    if final_flavor > Flavor::SPARSE {
                        let bit_matrix = old_sketch.build_bit_matrix();
                        self.state = UnionState::BitMatrix(bit_matrix);
                    }

                    return;
                }

                // If flavor is past SPARSE mode, the state must have been converted to bitMatrix.
                // Otherwise, the EMPTY case was handled at the start of this method, and the
                // SPARSE case was handled above.
                unreachable!("unexpected flavor in union accumulator: {:?}", flavor);
            }
            UnionState::BitMatrix(old_matrix) => {
                if flavor == Flavor::SPARSE {
                    // [Case B] Sparse, bitMatrix valid, accumulator == null
                    or_table_into_matrix(old_matrix, self.lg_k, sketch.surprising_value_table());
                    return;
                }

                if matches!(flavor, Flavor::HYBRID | Flavor::PINNED) {
                    // [Case C] Hybrid, bitMatrix valid, accumulator == null
                    // [Case C] Pinned, bitMatrix valid, accumulator == null
                    or_window_into_matrix(
                        old_matrix,
                        self.lg_k,
                        &sketch.sliding_window,
                        sketch.window_offset,
                        sketch.lg_k(),
                    );
                    or_table_into_matrix(old_matrix, self.lg_k, sketch.surprising_value_table());
                    return;
                }

                // [Case D] Sliding, bitMatrix valid, accumulator == null
                // SLIDING mode involves inverted logic, so we cannot just walk the source sketch.
                // Instead, we convert it to a bitMatrix that can be ORed into the destination.
                assert_eq!(flavor, Flavor::SLIDING);
                let src_matrix = sketch.build_bit_matrix();
                or_matrix_into_matrix(old_matrix, self.lg_k, &src_matrix, sketch.lg_k());
                return;
            }
        }
    }

    fn reduce_k(&mut self, new_lg_k: u8) {
        match &mut self.state {
            UnionState::Accumulator(sketch) => {
                if sketch.is_empty() {
                    self.lg_k = new_lg_k;
                    self.state = UnionState::Accumulator(CpcSketch::with_seed(new_lg_k, self.seed));
                    return;
                }

                let mut new_sketch = CpcSketch::with_seed(new_lg_k, self.seed);
                walk_table_updating_sketch(&mut new_sketch, sketch.surprising_value_table());

                let final_new_flavor = new_sketch.flavor();
                // SV table had to have something in it
                assert_ne!(final_new_flavor, Flavor::EMPTY);
                if final_new_flavor == Flavor::SPARSE {
                    self.lg_k = new_lg_k;
                    self.state = UnionState::Accumulator(new_sketch);
                    return;
                }

                // the new sketch has graduated beyond sparse, so convert to bitMatrix
                self.lg_k = new_lg_k;
                self.state = UnionState::BitMatrix(new_sketch.build_bit_matrix());
            }
            UnionState::BitMatrix(matrix) => {
                let new_k = 1 << new_lg_k;
                let mut new_matrix = vec![0; new_k];
                or_matrix_into_matrix(&mut new_matrix, new_lg_k, &matrix, self.lg_k);
                self.lg_k = new_lg_k;
                self.state = UnionState::BitMatrix(new_matrix);
            }
        }
    }
}

// testing methods
impl CpcUnion {
    /// Returns the number of coupons in the union.
    ///
    /// This is primarily for testing and validation purposes.
    pub fn num_coupons(&self) -> u32 {
        match &self.state {
            UnionState::Accumulator(sketch) => sketch.num_coupons,
            UnionState::BitMatrix(matrix) => count_bits_set_in_matrix(matrix),
        }
    }
}

fn or_window_into_matrix(
    dst_matrix: &mut [u64],
    dst_lg_k: u8,
    src_window: &[u8],
    src_offset: u8,
    src_lg_k: u8,
) {
    assert!(dst_lg_k <= src_lg_k);
    let dst_mask = (1 << dst_lg_k) - 1; // downsamples when dst_lg_k < src_lg_k
    let src_k = 1 << src_lg_k;
    for src_row in 0..src_k {
        dst_matrix[src_row & dst_mask] |= (src_window[src_row] as u64) << src_offset;
    }
}

fn or_table_into_matrix(dst_matrix: &mut [u64], dst_lg_k: u8, src_table: &PairTable) {
    let dst_mask = (1 << dst_lg_k) - 1; // downsamples when dst_lg_k < src_lg_k
    let slots = src_table.slots();
    for &row_col in slots.iter() {
        if row_col != u32::MAX {
            let src_row = (row_col >> 6) as usize;
            let src_col = (row_col & 63) as usize;
            let dst_row = src_row & dst_mask;
            dst_matrix[dst_row] |= 1u64 << src_col;
        }
    }
}

fn or_matrix_into_matrix(dst_matrix: &mut [u64], dst_lg_k: u8, src_matrix: &[u64], src_lg_k: u8) {
    assert!(dst_lg_k <= src_lg_k);
    let dst_mask = (1 << dst_lg_k) - 1; // downsamples when dst_lg_k < src_lg_k
    let src_k = 1 << src_lg_k;
    for src_row in 0..src_k {
        let dst_row = src_row & dst_mask;
        dst_matrix[dst_row] |= src_matrix[src_row];
    }
}

fn walk_table_updating_sketch(sketch: &mut CpcSketch, table: &PairTable) {
    assert!(sketch.lg_k() <= 26);

    let slots = table.slots();
    let num_slots = slots.len() as u32;

    // downsamples when dst lgK < src LgK
    let dst_mask = (((1u64 << sketch.lg_k()) - 1) << 6) | 63;
    // Using a golden ratio stride fixes the snowplow effect.
    let mut stride = (0.6180339887498949025 * (num_slots as f64)) as u32;
    assert!(stride >= 2);
    if stride == ((stride >> 1) << 1) {
        // force the stride to be odd
        stride += 1;
    }
    assert!((stride >= 3) && (stride < num_slots));

    let mut k = 0;
    for _ in 0..num_slots {
        k &= num_slots - 1;
        let row_col = slots[k as usize];
        if row_col != u32::MAX {
            sketch.row_col_update(row_col & (dst_mask as u32));
        }
        k += stride;
    }
}

/// The internal State of the Union operation.
///
/// At most one of BitMatrix and accumulator will be non-null at any given moment. Accumulator is a
/// sketch object that is employed until it graduates out of Sparse mode.
///
/// At that point, it is converted into a full-sized bitMatrix, which is mathematically a sketch,
/// but doesn't maintain any of the "extra" fields of our sketch objects.
#[derive(Debug, Clone)]
enum UnionState {
    Accumulator(CpcSketch),
    BitMatrix(Vec<u64>),
}
