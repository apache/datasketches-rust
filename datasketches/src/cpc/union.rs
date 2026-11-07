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

//! The merging logic is somewhat involved, so it will be summarized here.
//!
//! First, we compare the K values of the union and the source sketch.
//!
//! If `source.K < union.K`, we reduce the union's K to match, which requires downsampling the
//! union's internal sketch.
//!
//! Here is how to perform the downsampling:
//!
//! If the union contains a bitMatrix, downsample it by row-wise ORing.
//!
//! If the union contains a sparse sketch, then create a new empty sketch, and walk the old target
//! sketch updating the new one (with modulo). At the end, check whether the new target sketch is
//! still in sparse mode (it might not be, because downsampling densifies the set of collected
//! coupons). If it is NOT in sparse mode, immediately convert it to a bitMatrix.
//!
//! At this point, we have `source.K >= union.K`. (We won't keep mentioning this, but in all the
//! following the source's row indices are used mod union.K while updating the union's sketch.
//! That takes care of the situation where `source.K < union.K`.)
//!
//! Case A: union is Sparse and source is Sparse. We walk the source sketch updating the union's
//! sketch. At the end, if the union's sketch is no longer in sparse mode, we convert it to a
//! bitMatrix.
//!
//! Case B: union is bitMatrix and source is Sparse. We walk the source sketch, setting bits in the
//! bitMatrix.
//!
//! In the remaining cases, we have flavor(source) > Sparse, so we immediately convert the union's
//! sketch to a bitMatrix (even if the union contains very few coupons). Then:
//!
//! Case C: union is bitMatrix and source is Hybrid or Pinned. Then we OR the source's sliding
//! window into the bitMatrix, and walk the source's table, setting bits in the bitMatrix.
//!
//! Case D: union is bitMatrix, and source is Sliding. Then we convert the source into a bitMatrix,
//! and OR it into the union's bitMatrix. (Important note; merely walking the source wouldn't work
//! because of the partially inverted Logic in the Sliding flavor, where the presence of coupons
//! is sometimes indicated by the ABSENCE of row_col pairs in the surprises table.)
//!
//! Result extraction and serialization:
//!
//! An accumulator is already a result sketch: [`CpcUnion::to_sketch`] clones it, while
//! [`CpcUnion::serialize`] borrows it directly. Its estimator state must therefore be maintained
//! during merging, including input adoption and downsampling, rather than repaired on export.
//!
//! A bit matrix has no sliding window or surprise table. Both export paths reconstruct these,
//! along with the coupon count, window offset, and first interesting column, through
//! [`CpcUnion::to_sketch`]. The matrix does not preserve the stream history needed by HIP, so
//! the reconstructed sketch uses the merged estimator.

use std::fmt;

use crate::cpc::CpcSketch;
use crate::cpc::DEFAULT_LG_K;
use crate::cpc::Flavor;
use crate::cpc::count_bits_set_in_matrix;
use crate::cpc::determine_correct_offset;
use crate::cpc::pair_table::PairTable;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;

/// Union operator for CPC sketches.
#[derive(Clone)]
pub struct CpcUnion {
    // Effective precision can decrease when merging a smaller sketch.
    lg_k: u8,
    seed: u64,
    state: UnionState,
}

/// Working representation of the union's coupon set.
#[derive(Debug, Clone)]
enum UnionState {
    /// An empty or sparse sketch that can be exported directly.
    ///
    /// Every nonempty accumulator has `merge_flag = true`, including one adopted from a single
    /// input. Union results use the merged estimator consistently, regardless of whether the
    /// input's HIP history was retained by cloning or lost during table traversal.
    /// Empty accumulators keep `merge_flag = false` to preserve the existing empty-result encoding.
    Accumulator(CpcSketch),
    /// A full coupon matrix, used once an input or the accumulator outgrows sparse mode.
    ///
    /// Row-wise OR combines coupon sets without maintaining a sketch's compressed layout or
    /// estimator fields. Export must reconstruct that layout before it can use the sketch encoder.
    BitMatrix(Vec<u64>),
}

impl Default for CpcUnion {
    fn default() -> Self {
        Self::new(DEFAULT_LG_K).unwrap()
    }
}

impl CpcUnion {
    /// Creates a new `CpcUnion` with the given `lg_k` and default seed.
    ///
    /// # Errors
    ///
    /// Returns an error if `lg_k` is not in the range `[4, 26]`.
    pub fn new(lg_k: u8) -> Result<Self, Error> {
        Self::with_seed(lg_k, DEFAULT_UPDATE_SEED)
    }

    /// Creates a new `CpcUnion` with the given `lg_k` and `seed`.
    ///
    /// # Errors
    ///
    /// Returns an error if `lg_k` is not in the range `[4, 26]`, or the computed seed hash is zero.
    pub fn with_seed(lg_k: u8, seed: u64) -> Result<Self, Error> {
        let sketch = CpcSketch::with_seed(lg_k, seed)?;
        let state = UnionState::Accumulator(sketch);
        Ok(Self { lg_k, seed, state })
    }

    /// Returns the current `lg_k`.
    ///
    /// Note that due to merging with source sketches that may have a lower `lg_k`, this
    /// value can be less than what the union object was configured with.
    pub fn lg_k(&self) -> u8 {
        self.lg_k
    }

    /// Returns the union result as a new sketch.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::cpc::CpcSketch;
    /// use datasketches::cpc::CpcUnion;
    ///
    /// let mut s1 = CpcSketch::new(12).unwrap();
    /// s1.update(&"apple");
    ///
    /// let mut s2 = CpcSketch::new(12).unwrap();
    /// s2.update(&"apple");
    /// s2.update(&"banana");
    ///
    /// let mut union = CpcUnion::new(12).unwrap();
    /// union.update(&s1).unwrap();
    /// union.update(&s2).unwrap();
    ///
    /// let result = union.to_sketch();
    /// assert_eq!(result.estimate().trunc(), 2.0);
    /// ```
    pub fn to_sketch(&self) -> CpcSketch {
        match &self.state {
            UnionState::Accumulator(sketch) => sketch.clone(),
            UnionState::BitMatrix(matrix) => {
                let lg_k = self.lg_k;

                let mut sketch = CpcSketch::with_seed(lg_k, self.seed).unwrap();
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
                let clear_mask = (0xFFu64 << offset) ^ u64::MAX;
                let flip_mask = (1u64 << offset) - 1;
                let mut surprises = 0;

                // The snowplow effect was caused by processing the rows in order,
                // but we have fixed it by using a sufficiently large hash table.
                for i in 0..k {
                    let mut pattern = matrix[i];
                    sliding_window[i] = ((pattern >> offset) & 0xFF) as u8;
                    pattern &= clear_mask;
                    pattern ^= flip_mask; // this flipping converts surprising 0's to 1's
                    surprises |= pattern;
                    while pattern != 0 {
                        let col = pattern.trailing_zeros();
                        pattern ^= 1u64 << col; // erase the 1
                        let row_col = ((i as u32) << 6) | col;
                        let is_novel = table.maybe_insert(row_col);
                        assert!(is_novel);
                    }
                }

                // at this point we could shrink an oversized hash table, but the relative waste
                // isn't very big
                sketch.first_interesting_column = surprises.trailing_zeros() as u8;
                if sketch.first_interesting_column > offset {
                    sketch.first_interesting_column = offset; // corner case
                }

                // The matrix has no stream history, so the constructor's HIP fields cannot
                // estimate this result. The merged estimator uses only lg_k and num_coupons.
                sketch.sliding_window = sliding_window;
                sketch.surprising_value_table = Some(table);
                sketch.merge_flag = true;

                sketch
            }
        }
    }

    /// Serializes the union's result as a `CpcSketch`.
    ///
    /// Produces the same bytes as [`Self::to_sketch`] followed by [`CpcSketch::serialize`],
    /// and can be faster.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::cpc::CpcSketch;
    /// use datasketches::cpc::CpcUnion;
    ///
    /// let mut sketch = CpcSketch::new(12).unwrap();
    /// sketch.update("apple");
    ///
    /// let mut union = CpcUnion::new(12).unwrap();
    /// union.update(&sketch).unwrap();
    ///
    /// let result = CpcSketch::deserialize(&union.serialize()).unwrap();
    /// assert_eq!(result.estimate(), union.to_sketch().estimate());
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        match &self.state {
            UnionState::Accumulator(sketch) => sketch.serialize(),
            UnionState::BitMatrix(_) => self.to_sketch().serialize(),
        }
    }

    /// Updates this union with a `CpcSketch`.
    ///
    /// # Errors
    ///
    /// Returns an error if the seed of the provided sketch does not match the seed of this union.
    pub fn update(&mut self, sketch: &CpcSketch) -> Result<(), Error> {
        if self.seed != sketch.seed() {
            return Err(Error::invalid_argument(format!(
                "CPC sketch seed must match union seed: expected {}, got {}",
                self.seed,
                sketch.seed()
            )));
        }

        let flavor = sketch.flavor();
        if flavor == Flavor::Empty {
            return Ok(());
        }

        if sketch.lg_k() < self.lg_k {
            self.reduce_k(sketch.lg_k());
        }

        // if source is past SPARSE mode, make sure that union is a bitMatrix.
        if flavor > Flavor::Sparse {
            if let UnionState::Accumulator(old_sketch) = &self.state {
                // convert the accumulator to a bit matrix
                let bit_matrix = old_sketch.build_bit_matrix();
                self.state = UnionState::BitMatrix(bit_matrix);
            }
        }

        match &mut self.state {
            UnionState::Accumulator(old_sketch) => {
                // [Case A] Sparse, bitMatrix == null, accumulator valid
                if flavor == Flavor::Sparse {
                    let old_flavor = old_sketch.flavor();
                    if old_flavor != Flavor::Sparse && old_flavor != Flavor::Empty {
                        unreachable!(
                            "unexpected old flavor in union accumulator: {:?}",
                            old_flavor
                        );
                    }

                    // Matching precision lets us reuse the table layout without reinserting
                    // coupons. Adoption still produces a union result, so it must use the merged
                    // estimator even though the cloned input may have a valid HIP history.
                    if old_flavor == Flavor::Empty && self.lg_k == sketch.lg_k() {
                        *old_sketch = sketch.clone();
                        old_sketch.merge_flag = true;
                        return Ok(());
                    }

                    merge_table_into_sketch(old_sketch, sketch.surprising_value_table());
                    let final_flavor = old_sketch.flavor();

                    // if the accumulator has graduated beyond sparse, switch to a bit matrix
                    // representation
                    if final_flavor > Flavor::Sparse {
                        let bit_matrix = old_sketch.build_bit_matrix();
                        self.state = UnionState::BitMatrix(bit_matrix);
                    }

                    return Ok(());
                }

                // If flavor is past SPARSE mode, the state must have been converted to bitMatrix.
                // Otherwise, the EMPTY case was handled at the start of this method, and the
                // SPARSE case was handled above.
                unreachable!("unexpected flavor in union accumulator: {:?}", flavor);
            }
            UnionState::BitMatrix(old_matrix) => {
                if flavor == Flavor::Sparse {
                    // [Case B] Sparse, bitMatrix valid, accumulator == null
                    or_table_into_matrix(old_matrix, self.lg_k, sketch.surprising_value_table());
                    return Ok(());
                }

                if matches!(flavor, Flavor::Hybrid | Flavor::Pinned) {
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
                    return Ok(());
                }

                // [Case D] Sliding, bitMatrix valid, accumulator == null
                // SLIDING mode involves inverted logic, so we cannot just walk the source sketch.
                // Instead, we convert it to a bitMatrix that can be ORed into the destination.
                assert_eq!(flavor, Flavor::Sliding);
                let src_matrix = sketch.build_bit_matrix();
                or_matrix_into_matrix(old_matrix, self.lg_k, &src_matrix, sketch.lg_k());
            }
        }
        Ok(())
    }

    fn reduce_k(&mut self, new_lg_k: u8) {
        match &mut self.state {
            UnionState::Accumulator(sketch) => {
                if sketch.is_empty() {
                    self.lg_k = new_lg_k;
                    self.state =
                        UnionState::Accumulator(CpcSketch::with_seed(new_lg_k, self.seed).unwrap());
                    return;
                }

                let mut new_sketch = CpcSketch::with_seed(new_lg_k, self.seed).unwrap();
                merge_table_into_sketch(&mut new_sketch, sketch.surprising_value_table());

                let final_new_flavor = new_sketch.flavor();
                // SV table had to have something in it
                assert_ne!(final_new_flavor, Flavor::Empty);
                if final_new_flavor == Flavor::Sparse {
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
                or_matrix_into_matrix(&mut new_matrix, new_lg_k, matrix, self.lg_k);
                self.lg_k = new_lg_k;
                self.state = UnionState::BitMatrix(new_matrix);
            }
        }
    }

    /// Returns an estimate of the union's memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        match &self.state {
            UnionState::Accumulator(sketch) => sketch.estimated_size(),
            UnionState::BitMatrix(matrix) => matrix.capacity() * size_of::<u64>(),
        }
    }
}

impl fmt::Debug for CpcUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (state, num_coupons) = match &self.state {
            UnionState::Accumulator(sketch) => ("Accumulator", sketch.num_coupons),
            UnionState::BitMatrix(matrix) => ("BitMatrix", count_bits_set_in_matrix(matrix)),
        };

        f.debug_struct("CpcUnion")
            .field("lg_k", &self.lg_k())
            .field("seed", &self.seed)
            .field("state", &state)
            .field("is_empty", &(num_coupons == 0))
            .field("num_coupons", &num_coupons)
            .finish()
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

fn merge_table_into_sketch(sketch: &mut CpcSketch, table: &PairTable) {
    assert!(sketch.lg_k() <= 26);
    // Table traversal cannot preserve the stream order required by the HIP estimator.
    sketch.merge_flag = true;

    let slots = table.slots();
    let num_slots = slots.len() as u32;

    // downsamples when dst lgK < src LgK
    let dst_mask = (((1u64 << sketch.lg_k()) - 1) << 6) | 63;
    // Using a golden ratio stride fixes the snowplow effect.
    let mut stride = (0.6180339887498949 * (num_slots as f64)) as u32;
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
