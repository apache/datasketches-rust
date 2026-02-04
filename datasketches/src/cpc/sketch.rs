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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::common::NumStdDev;
use crate::common::canonical_double;
use crate::common::inv_pow2_table::INVERSE_POWERS_OF_2;
use crate::cpc::DEFAULT_LG_K;
use crate::cpc::Flavor;
use crate::cpc::MAX_LG_K;
use crate::cpc::MIN_LG_K;
use crate::cpc::compression::CompressedState;
use crate::cpc::count_bits_set_in_matrix;
use crate::cpc::determine_correct_offset;
use crate::cpc::determine_flavor;
use crate::cpc::estimator::hip_confidence_lb;
use crate::cpc::estimator::hip_confidence_ub;
use crate::cpc::estimator::icon_confidence_lb;
use crate::cpc::estimator::icon_confidence_ub;
use crate::cpc::estimator::icon_estimate;
use crate::cpc::kxp_byte_lookup::KXP_BYTE_TABLE;
use crate::cpc::pair_table::PairTable;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::MurmurHash3X64128;
use crate::hash::compute_seed_hash;

/// A Compressed Probabilistic Counting sketch.
///
/// See the [module level documentation](super) for more.
#[derive(Debug, Clone)]
pub struct CpcSketch {
    // immutable config variables
    lg_k: u8,
    seed: u64,

    // sketch state
    /// Part of a speed optimization.
    pub(super) first_interesting_column: u8,
    /// The number of coupons collected so far.
    pub(super) num_coupons: u32,
    /// Sparse and surprising values.
    pub(super) surprising_value_table: Option<PairTable>,
    /// Derivable from num_coupons, but made explicit for speed.
    pub(super) window_offset: u8,
    /// Size K bytes in dense mode (flavor >= HYBRID).
    pub(super) sliding_window: Vec<u8>,

    // estimator state
    /// Whether the sketch is a result of merging.
    ///
    /// If `false`, the HIP (Historical Inverse Probability) estimator is used.
    /// If `true`, the ICON (Inter-Column Optimal) Estimator is fallback in use.
    pub(super) merge_flag: bool,
    // the following variables are only valid in HIP estimator
    /// A pre-calculated probability factor (`k * p`) used to compute the increment delta.
    kxp: f64,
    /// The accumulated cardinality estimate.
    hip_est_accum: f64,
}

impl Default for CpcSketch {
    fn default() -> Self {
        Self::new(DEFAULT_LG_K)
    }
}

impl CpcSketch {
    /// Creates a new `CpcSketch` with the given `lg_k` and default seed.
    ///
    /// # Panics
    ///
    /// Panics if `lg_k` is not in the range `[4, 16]`.
    pub fn new(lg_k: u8) -> Self {
        Self::with_seed(lg_k, DEFAULT_UPDATE_SEED)
    }

    /// Creates a new `CpcSketch` with the given `lg_k` and `seed`.
    ///
    /// # Panics
    ///
    /// Panics if `lg_k` is not in the range `[4, 16]`.
    pub fn with_seed(lg_k: u8, seed: u64) -> Self {
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&(lg_k as usize)),
            "lg_k out of range; got {lg_k}",
        );

        Self {
            lg_k,
            seed,
            first_interesting_column: 0,
            num_coupons: 0,
            surprising_value_table: None,
            window_offset: 0,
            sliding_window: vec![],
            merge_flag: false,
            kxp: (1 << lg_k) as f64,
            hip_est_accum: 0.0,
        }
    }

    /// Return the parameter lg_k.
    pub fn lg_k(&self) -> u8 {
        self.lg_k
    }

    /// Returns the best estimate of the cardinality of the sketch.
    pub fn estimate(&self) -> f64 {
        if !self.merge_flag {
            self.hip_est_accum
        } else {
            icon_estimate(self.lg_k, self.num_coupons)
        }
    }

    /// Returns the best estimate of the lower bound of the confidence interval given `kappa`.
    pub fn lower_bound(&self, kappa: NumStdDev) -> f64 {
        if !self.merge_flag {
            hip_confidence_lb(self.lg_k, self.num_coupons, self.hip_est_accum, kappa)
        } else {
            icon_confidence_lb(self.lg_k, self.num_coupons, kappa)
        }
    }

    /// Returns the best estimate of the upper bound of the confidence interval given `kappa`.
    pub fn upper_bound(&self, kappa: NumStdDev) -> f64 {
        if !self.merge_flag {
            hip_confidence_ub(self.lg_k, self.num_coupons, self.hip_est_accum, kappa)
        } else {
            icon_confidence_ub(self.lg_k, self.num_coupons, kappa)
        }
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_coupons == 0
    }

    /// Update the sketch with a hashable value.
    ///
    /// For `f32`/`f64` values, use `update_f32`/`update_f64` instead.
    pub fn update<T: Hash>(&mut self, value: T) {
        let mut hasher = MurmurHash3X64128::with_seed(self.seed);
        value.hash(&mut hasher);
        let (h1, h2) = hasher.finish128();

        let k = 1 << self.lg_k;
        let col = h2.leading_zeros(); // 0 <= col <= 64
        let col = if col > 63 { 63 } else { col as u8 }; // clip so that 0 <= col <= 63
        let row = (h1 & (k - 1)) as u32;
        let mut row_col = (row << 6) | (col as u32);
        // To avoid the hash table's "empty" value, we change the row of the following pair.
        // This case is extremely unlikely, but we might as well handle it.
        if row_col == u32::MAX {
            row_col ^= 1 << 6;
        }
        self.row_col_update(row_col);
    }

    /// Update the sketch with a f64 value.
    pub fn update_f64(&mut self, value: f64) {
        // Canonicalize double for compatibility with Java
        let canonical = canonical_double(value);
        self.update(canonical);
    }

    /// Update the sketch with a f32 value.
    pub fn update_f32(&mut self, value: f32) {
        self.update_f64(value as f64);
    }

    pub(super) fn flavor(&self) -> Flavor {
        determine_flavor(self.lg_k, self.num_coupons)
    }

    pub(super) fn row_col_update(&mut self, row_col: u32) {
        let col = (row_col & 63) as u8;
        if col < self.first_interesting_column {
            // important speed optimization
            return;
        }

        if self.num_coupons == 0 {
            // promote EMPTY to SPARSE
            self.surprising_value_table = Some(PairTable::new(2, 6 + self.lg_k));
        }

        if self.sliding_window.is_empty() {
            self.update_sparse(row_col);
        } else {
            self.update_windowed(row_col);
        }
    }

    pub(super) fn seed(&self) -> u64 {
        self.seed
    }

    pub(super) fn surprising_value_table(&self) -> &PairTable {
        self.surprising_value_table
            .as_ref()
            .expect("surprising value table must be initialized")
    }

    pub(super) fn mut_surprising_value_table(&mut self) -> &mut PairTable {
        self.surprising_value_table
            .as_mut()
            .expect("surprising value table must be initialized")
    }

    fn update_hip(&mut self, row_col: u32) {
        let k = 1 << self.lg_k;
        let col = (row_col & 63) as usize;
        let one_over_p = (k as f64) / self.kxp;
        self.hip_est_accum += one_over_p;
        self.kxp -= INVERSE_POWERS_OF_2[col + 1] // notice the "+1"
    }

    fn update_sparse(&mut self, row_col: u32) {
        let k = 1 << self.lg_k;
        let c32pre = (self.num_coupons as u64) << 5;
        debug_assert!(c32pre < 3 * k); // C < 3K/32, in other words, flavor == SPARSE
        let is_novel = self.mut_surprising_value_table().maybe_insert(row_col);
        if is_novel {
            self.num_coupons += 1;
            self.update_hip(row_col);
            let c32post = (self.num_coupons as u64) << 5;
            if c32post >= 3 * k {
                self.promote_sparse_to_windowed();
            }
        }
    }

    fn promote_sparse_to_windowed(&mut self) {
        debug_assert_eq!(self.window_offset, 0);

        let k = 1 << self.lg_k;
        let c32 = (self.num_coupons as u64) << 5;
        debug_assert!((c32 == (3 * k)) || ((self.lg_k == 4) && (c32 > (3 * k))));

        self.sliding_window.resize(k as usize, 0);

        let old_table = self
            .surprising_value_table
            .replace(PairTable::new(2, 6 + self.lg_k))
            .expect("surprising value table must be initialized");
        let old_slots = old_table.slots();
        for &row_col in old_slots {
            if row_col != u32::MAX {
                let col = (row_col & 63) as u8;
                if col < 8 {
                    let row = (row_col >> 6) as usize;
                    self.sliding_window[row] |= 1 << col;
                } else {
                    // cannot use must_insert(), because it doesn't provide for growth
                    let is_novel = self.mut_surprising_value_table().maybe_insert(row_col);
                    debug_assert!(is_novel);
                }
            }
        }
    }

    fn update_windowed(&mut self, row_col: u32) {
        debug_assert!(self.window_offset <= 56);
        let k = 1 << self.lg_k;
        let c32pre = (self.num_coupons as u64) << 5;
        debug_assert!(c32pre >= 3 * k); // C >= 3K/32, in other words flavor >= HYBRID
        let c8pre = (self.num_coupons as u64) << 3;
        let w8pre = (self.window_offset as u64) << 3;
        debug_assert!(c8pre < (27 + w8pre) * k); // C < (K * 27/8) + (K * windowOffset)

        let mut is_novel = false; // novel if new coupon;
        let col = (row_col & 63) as u8;
        if col < self.window_offset {
            // track the surprising 0's "before" the window
            is_novel = self.mut_surprising_value_table().maybe_delete(row_col); // inverted logic
        } else if col < self.window_offset + 8 {
            // track the 8 bits inside the window
            let row = (row_col >> 6) as usize;
            let old_bits = self.sliding_window[row];
            let new_bits = old_bits | (1 << (col - self.window_offset));
            if old_bits != new_bits {
                self.sliding_window[row] = new_bits;
                is_novel = true;
            }
        } else {
            // track the surprising 1's "after" the window
            is_novel = self.mut_surprising_value_table().maybe_insert(row_col); // normal logic
        }

        if is_novel {
            self.num_coupons += 1;
            self.update_hip(row_col);
            let c8post = (self.num_coupons as u64) << 3;
            if c8post >= (27 + w8pre) * k {
                self.move_window();
                debug_assert!((1..=56).contains(&self.window_offset));
                let w8post = (self.window_offset as u64) << 3;
                debug_assert!(c8post < ((27 + w8post) * k)); // C < (K * 27/8) + (K * windowOffset)
            }
        }
    }

    fn move_window(&mut self) {
        let new_offset = self.window_offset + 1;
        debug_assert!(new_offset <= 56);
        debug_assert_eq!(
            new_offset,
            determine_correct_offset(self.lg_k, self.num_coupons)
        );

        let k = 1 << self.lg_k;

        // Construct the full-sized bit matrix that corresponds to the sketch
        let bit_matrix = self.build_bit_matrix();

        // refresh the KXP register on every 8th window shift.
        if (new_offset & 0x7) == 0 {
            self.refresh_kxp(&bit_matrix);
        }

        self.mut_surprising_value_table().clear(); // the new number of surprises will be about the same

        let mask_for_clearing_window = (0xFF << new_offset) ^ u64::MAX;
        let mask_for_flipping_early_zone = (1u64 << new_offset) - 1;

        let mut all_surprises_ored = 0u64;
        for i in 0..k {
            let mut pattern = bit_matrix[i];
            self.sliding_window[i] = ((pattern >> new_offset) & 0xff) as u8;
            pattern &= mask_for_clearing_window;
            // The following line converts surprising 0's to 1's in the "early zone",
            // (and vice versa, which is essential for this procedure's O(k) time cost).
            pattern ^= mask_for_flipping_early_zone;
            all_surprises_ored |= pattern; // a cheap way to recalculate first_interesting_column
            while pattern != 0 {
                let col = pattern.trailing_zeros();
                pattern ^= 1 << col; // erase the 1
                let row_col = ((i as u32) << 6) | col;
                let is_novel = self.mut_surprising_value_table().maybe_insert(row_col);
                debug_assert!(is_novel);
            }
        }

        self.window_offset = new_offset;
        self.first_interesting_column = all_surprises_ored.trailing_zeros() as u8;
        if self.first_interesting_column > new_offset {
            self.first_interesting_column = new_offset; // corner case
        }
    }

    /// The KXP register is a double with roughly 50 bits of precision, but it might need roughly
    /// 90 bits to track the value with perfect accuracy.
    ///
    /// Therefore, we recalculate KXP occasionally from the sketch's full bit_matrix so that it
    /// will reflect changes that were previously outside the mantissa.
    fn refresh_kxp(&mut self, bit_matrix: &[u64]) {
        // for improved numerical accuracy, we separately sum the bytes of the u64's
        let mut byte_sums = [0.0; 8];
        for &bit in bit_matrix {
            let mut word = bit;
            for sum in byte_sums.iter_mut() {
                let byte = (word & 0xFF) as usize;
                *sum += KXP_BYTE_TABLE[byte];
                word >>= 8;
            }
        }

        let mut total = 0.0;
        for i in (0..8).rev() {
            // the reverse order is important
            let factor = INVERSE_POWERS_OF_2[i * 8]; // pow (256.0, (-1.0 * ((double) j)))
            total += factor * byte_sums[i];
        }

        self.kxp = total;
    }

    pub(super) fn build_bit_matrix(&self) -> Vec<u64> {
        let k = 1 << self.lg_k;
        let offset = self.window_offset;
        debug_assert!(offset <= 56);

        // Fill the matrix with default rows in which the "early zone" is filled with ones.
        // This is essential for the routine's O(k) time cost (as opposed to O(C)).
        let default_row = (1u64 << offset) - 1;

        let mut matrix = vec![default_row; k];
        if self.num_coupons == 0 {
            return matrix;
        }

        if !self.sliding_window.is_empty() {
            // In other words, we are in window mode, not sparse mode
            for i in 0..k {
                // set the window bits, trusting the sketch's current offset
                matrix[i] |= (self.sliding_window[i] as u64) << offset;
            }
        }

        for &row_col in self.surprising_value_table().slots() {
            if row_col != u32::MAX {
                let col = (row_col & 63) as u8;
                let row = (row_col >> 6) as usize;
                // Flip the specified matrix bit from its default value.
                // In the "early" zone the bit changes from 1 to 0.
                // In the "late" zone the bit changes from 0 to 1.
                matrix[row] ^= 1 << col;
            }
        }

        matrix
    }
}

const SERIAL_VERSION: u8 = 1;
const CPC_FAMILY_ID: u8 = 16;

#[repr(u8)]
enum Flags {
    IsCompressed = 1,
    HasHip = 2,
    HasTable = 3,
    HasWindow = 4,
}

impl CpcSketch {
    /// Serializes this CpcSketch to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = SketchBytes::with_capacity(256);

        let mut compressed = CompressedState::default();
        compressed.compress(self);

        let has_hip = !self.merge_flag;
        let has_table = !compressed.table_data.is_empty();
        let has_window = !compressed.window_data.is_empty();
        let preamble_ints = make_preamble_ints(self.num_coupons, has_hip, has_table, has_window);
        bytes.write_u8(preamble_ints);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(CPC_FAMILY_ID);
        bytes.write_u8(self.lg_k);
        bytes.write_u8(self.first_interesting_column);
        let flags = (1 << Flags::IsCompressed as u8)
            | (if has_hip { 1 } else { 0 } << Flags::HasHip as u8)
            | (if has_table { 1 } else { 0 } << Flags::HasTable as u8)
            | (if has_window { 1 } else { 0 } << Flags::HasWindow as u8);
        bytes.write_u8(flags);
        let seed_hash = compute_seed_hash(self.seed);
        bytes.write_u16_le(seed_hash);
        if !self.is_empty() {
            bytes.write_u32_le(self.num_coupons);
            if has_table && has_window {
                // if there is no window it is the same as number of coupons
                bytes.write_u32_le(compressed.table_num_entries);
                // HIP values can be in two different places in the sequence of fields
                // this is the first HIP decision point
                if has_hip {
                    self.write_hip(&mut bytes);
                }
            }
            if has_table {
                debug_assert!(compressed.table_data_words <= u32::MAX as usize);
                bytes.write_u32_le(compressed.table_data_words as u32);
            }
            if has_window {
                debug_assert!(compressed.window_data_words <= u32::MAX as usize);
                bytes.write_u32_le(compressed.window_data_words as u32);
            }
            // this is the second HIP decision point
            if has_hip && !(has_table && has_window) {
                self.write_hip(&mut bytes);
            }
            if has_window {
                for i in 0..compressed.window_data_words {
                    bytes.write_u32_le(compressed.window_data[i]);
                }
            }
            if has_table {
                for i in 0..compressed.table_data_words {
                    bytes.write_u32_le(compressed.table_data[i]);
                }
            }
        }
        bytes.into_bytes()
    }

    /// Deserializes a CpcSketch from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_seed(bytes, DEFAULT_UPDATE_SEED)
    }

    /// Deserializes a CpcSketch from bytes with the provided seed.
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }

        let mut cursor = SketchSlice::new(bytes);
        let preamble_ints = cursor.read_u8().map_err(make_error("preamble_ints"))?;
        let serial_version = cursor.read_u8().map_err(make_error("serial_version"))?;
        let family_id = cursor.read_u8().map_err(make_error("family_id"))?;
        if family_id != CPC_FAMILY_ID {
            return Err(Error::invalid_family(CPC_FAMILY_ID, family_id, "TDigest"));
        }
        if serial_version != SERIAL_VERSION {
            return Err(Error::unsupported_serial_version(
                SERIAL_VERSION,
                serial_version,
            ));
        }

        let lg_k = cursor.read_u8().map_err(make_error("lg_k"))?;
        let first_interesting_column = cursor
            .read_u8()
            .map_err(make_error("first_interesting_column"))?;

        let flags = cursor.read_u8().map_err(make_error("flags"))?;
        let seed_hash = cursor.read_u16_le().map_err(make_error("seed_hash"))?;
        let has_hip = flags & (1 << Flags::HasHip as u8) != 0;
        let has_table = flags & (1 << Flags::HasTable as u8) != 0;
        let has_window = flags & (1 << Flags::HasWindow as u8) != 0;

        let mut compressed = CompressedState::default();
        let mut num_coupons = 0;
        let mut kxp = 0.0;
        let mut hip_est_accum = 0.0;

        if has_table || has_window {
            num_coupons = cursor.read_u32_le().map_err(make_error("num_coupons"))?;
            if has_table && has_window {
                compressed.table_num_entries = cursor
                    .read_u32_le()
                    .map_err(make_error("table_num_entries"))?;
                if has_hip {
                    kxp = cursor.read_f64_le().map_err(make_error("kxp"))?;
                    hip_est_accum = cursor.read_f64_le().map_err(make_error("hip_est_accum"))?;
                }
            }
            if has_table {
                compressed.table_data_words = cursor
                    .read_u32_le()
                    .map_err(make_error("table_data_words"))?
                    as usize;
            }
            if has_window {
                compressed.window_data_words = cursor
                    .read_u32_le()
                    .map_err(make_error("window_data_words"))?
                    as usize;
            }
            if has_hip && !(has_table && has_window) {
                kxp = cursor.read_f64_le().map_err(make_error("kxp"))?;
                hip_est_accum = cursor.read_f64_le().map_err(make_error("hip_est_accum"))?;
            }
            if has_window {
                for _ in 0..compressed.window_data_words {
                    let word = cursor.read_u32_le().map_err(make_error("window_data"))?;
                    compressed.window_data.push(word);
                }
            }
            if has_table {
                for _ in 0..compressed.table_data_words {
                    let word = cursor.read_u32_le().map_err(make_error("table_data"))?;
                    compressed.table_data.push(word);
                }
            }
            if !has_window {
                compressed.table_num_entries = num_coupons;
            }
        }

        let expected_preamble_ints =
            make_preamble_ints(num_coupons, has_hip, has_table, has_window);
        if preamble_ints != expected_preamble_ints {
            return Err(Error::invalid_preamble_longs(
                expected_preamble_ints,
                preamble_ints,
            ));
        }
        if seed_hash != compute_seed_hash(seed) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "seed hash mismatch: expected {}, got {}",
                    compute_seed_hash(seed),
                    seed_hash
                ),
            ));
        }

        let uncompressed = compressed.uncompress(lg_k, num_coupons);
        Ok(CpcSketch {
            lg_k,
            seed,
            first_interesting_column,
            num_coupons,
            surprising_value_table: Some(uncompressed.table),
            window_offset: determine_correct_offset(lg_k, num_coupons),
            sliding_window: uncompressed.window,
            merge_flag: !has_hip,
            kxp,
            hip_est_accum,
        })
    }

    fn write_hip(&self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(self.kxp);
        bytes.write_f64_le(self.hip_est_accum);
    }
}

fn make_preamble_ints(num_coupons: u32, has_hip: bool, has_table: bool, has_window: bool) -> u8 {
    let mut preamble_ints = 2;
    if num_coupons > 0 {
        preamble_ints += 1; // number of coupons
        if has_hip {
            preamble_ints += 4; // HIP
        }
        if has_table {
            preamble_ints += 1; // table data length
            // number of values (if there is no window it is the same as number of coupons)
            if has_window {
                preamble_ints += 1;
            }
        }
        if has_window {
            preamble_ints += 1; // window length
        }
    }
    preamble_ints
}

impl CpcSketch {
    /// Returns the estimated maximum compressed serialized size of a sketch.
    ///
    /// The actual size of a compressed CPC sketch has a small random variance, but the following
    /// empirically measured size should be large enough for at least 99.9 percent of sketches.
    ///
    /// For small values of `n` the size can be much smaller.
    ///
    /// # Panics
    ///
    /// Panics if `lg_k` is not in the range `[4, 26]`.
    pub fn max_serialized_bytes(lg_k: u8) -> usize {
        let lg_k = lg_k as usize;
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&lg_k),
            "lg_k out of range; got {lg_k}",
        );

        // These empirical values for the 99.9th percentile of size in bytes were measured using
        // 100,000 trials. The value for each trial is the maximum of 5*16=80 measurements
        // that were equally spaced over values of the quantity C/K between 3.0 and 8.0.
        // This table does not include the worst-case space for the preamble, which is added
        // by the function.
        const MAX_PREAMBLE_SIZE_BYTES: usize = 40;
        const EMPIRICAL_SIZE_MAX_LGK: usize = 19;
        const EMPIRICAL_MAX_SIZE_FACTOR: f64 = 0.6; // 0.6 = 4.8 / 8.0
        static EMPIRICAL_MAX_SIZE_BYTES: [usize; 16] = [
            24,     // lg_k = 4
            36,     // lg_k = 5
            56,     // lg_k = 6
            100,    // lg_k = 7
            180,    // lg_k = 8
            344,    // lg_k = 9
            660,    // lg_k = 10
            1292,   // lg_k = 11
            2540,   // lg_k = 12
            5020,   // lg_k = 13
            9968,   // lg_k = 14
            19836,  // lg_k = 15
            39532,  // lg_k = 16
            78880,  // lg_k = 17
            157516, // lg_k = 18
            314656, // lg_k = 19
        ];

        if lg_k <= EMPIRICAL_SIZE_MAX_LGK {
            return EMPIRICAL_MAX_SIZE_BYTES[lg_k - MIN_LG_K] + MAX_PREAMBLE_SIZE_BYTES;
        }
        let k = 1 << lg_k;
        ((EMPIRICAL_MAX_SIZE_FACTOR * k as f64) as usize) + MAX_PREAMBLE_SIZE_BYTES
    }
}

// testing methods
impl CpcSketch {
    /// Validate this sketch is valid.
    ///
    /// This is primarily for testing and validation purposes.
    pub fn validate(&self) -> bool {
        let bit_matrix = self.build_bit_matrix();
        let num_bits_set = count_bits_set_in_matrix(&bit_matrix);
        num_bits_set == self.num_coupons
    }

    /// Returns the number of coupons in the sketch.
    ///
    /// This is primarily for testing and validation purposes.
    pub fn num_coupons(&self) -> u32 {
        self.num_coupons
    }
}
