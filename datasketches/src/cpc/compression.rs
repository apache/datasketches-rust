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

use std::cmp::Ordering;

use crate::cpc::CpcSketch;
use crate::cpc::Flavor;
use crate::cpc::compression_data::COLUMN_PERMUTATIONS_FOR_DECODING;
use crate::cpc::compression_data::COLUMN_PERMUTATIONS_FOR_ENCODING;
use crate::cpc::compression_data::DECODING_TABLES_FOR_HIGH_ENTROPY_BYTE;
use crate::cpc::compression_data::ENCODING_TABLES_FOR_HIGH_ENTROPY_BYTE;
use crate::cpc::compression_data::LENGTH_LIMITED_UNARY_DECODING_TABLE65;
use crate::cpc::compression_data::LENGTH_LIMITED_UNARY_ENCODING_TABLE65;
use crate::cpc::determine_correct_offset;
use crate::cpc::determine_flavor;
use crate::cpc::pair_table::PairTable;

#[derive(Default)]
pub(super) struct CompressedState {
    pub(super) table_data: Vec<u32>,
    pub(super) table_data_words: usize,
    // can be different from the number of entries in the sketch in hybrid mode
    pub(super) table_num_entries: u32,
    pub(super) window_data: Vec<u32>,
    pub(super) window_data_words: usize,
}

impl CompressedState {
    pub fn compress(&mut self, source: &CpcSketch) {
        match source.flavor() {
            Flavor::Empty => {
                // do nothing
            }
            Flavor::Sparse => {
                self.compress_sparse_flavor(source);
                debug_assert!(self.window_data.is_empty(), "window is not expected");
                debug_assert!(!self.table_data.is_empty(), "table is expected");
            }
            Flavor::Hybrid => {
                self.compress_hybrid_flavor(source);
                debug_assert!(self.window_data.is_empty(), "window is not expected");
                debug_assert!(!self.table_data.is_empty(), "table is expected");
            }
            Flavor::Pinned => {
                self.compress_pinned_flavor(source);
                debug_assert!(!self.window_data.is_empty(), "window is expected");
            }
            Flavor::Sliding => {
                self.compress_sliding_flavor(source);
                debug_assert!(!self.window_data.is_empty(), "window is expected");
            }
        }
    }

    fn compress_sparse_flavor(&mut self, source: &CpcSketch) {
        debug_assert!(source.sliding_window.is_empty());
        let mut pairs = source.surprising_value_table().unwrapping_get_items();
        pairs.sort_unstable();
        self.compress_surprising_values(&pairs, source.lg_k());
    }

    fn compress_hybrid_flavor(&mut self, source: &CpcSketch) {
        debug_assert!(!source.sliding_window.is_empty());
        debug_assert_eq!(source.window_offset, 0);

        let k = 1 << source.lg_k();
        let mut pairs = source.surprising_value_table().unwrapping_get_items();
        pairs.sort_unstable();
        let num_pairs_from_table = pairs.len();
        let num_pairs_from_window = (source.num_coupons() as usize) - num_pairs_from_table;

        let all_pairs_len = num_pairs_from_table + num_pairs_from_window;
        let mut all_pairs = vec![0; all_pairs_len];
        // tricky read pairs from sliding_window
        {
            // The empty space that this leaves at the beginning of the output array will be filled
            // later.
            let mut idx = num_pairs_from_table;
            for row_index in 0..k {
                let mut byte = source.sliding_window[row_index];
                while byte != 0 {
                    let col_index = byte.trailing_zeros();
                    byte ^= 1 << col_index; // erase the 1
                    all_pairs[idx] = ((row_index << 6) as u32) | col_index;
                    idx += 1;
                }
            }
            assert_eq!(idx, all_pairs_len);
        }
        // two-way merge of pairs_from_table and pairs_from_window into all_pairs
        {
            let mut final_idx = 0;
            let mut table_idx = 0;
            let mut window_idx = num_pairs_from_table;

            while final_idx < all_pairs_len {
                if table_idx < num_pairs_from_table
                    && (window_idx >= all_pairs_len || pairs[table_idx] <= all_pairs[window_idx])
                {
                    all_pairs[final_idx] = pairs[table_idx];
                    table_idx += 1;
                } else {
                    all_pairs[final_idx] = all_pairs[window_idx];
                    window_idx += 1;
                }
                final_idx += 1;
            }
        }

        self.compress_surprising_values(&all_pairs, source.lg_k());
    }

    fn compress_pinned_flavor(&mut self, source: &CpcSketch) {
        self.compress_sliding_window(&source.sliding_window, source.lg_k(), source.num_coupons());
        let mut pairs = source.surprising_value_table().unwrapping_get_items();
        if !pairs.is_empty() {
            // Here we subtract 8 from the column indices. Because they are stored in the low 6 bits
            // of each row_col pair, and because no column index is less than 8 for a "Pinned"
            // sketch, we can simply subtract 8 from the pairs themselves.

            // shift the columns over by 8 positions before compressing (because of the window)
            for pair in &mut pairs {
                assert!(*pair & 63 >= 8, "pair column index is less than 8: {pair}");
                *pair -= 8;
            }

            pairs.sort_unstable();
            self.compress_surprising_values(&pairs, source.lg_k());
        }
    }

    // Complicated by the existence of both a left fringe and a right fringe.
    fn compress_sliding_flavor(&mut self, source: &CpcSketch) {
        self.compress_sliding_window(&source.sliding_window, source.lg_k(), source.num_coupons());
        let mut pairs = source.surprising_value_table().unwrapping_get_items();
        if !pairs.is_empty() {
            // Here we apply a complicated transformation to the column indices, which
            // changes the implied ordering of the pairs, so we must do it before sorting.

            let pseudo_phase = determine_pseudo_phase(source.lg_k(), source.num_coupons());
            let permutation = &COLUMN_PERMUTATIONS_FOR_ENCODING[pseudo_phase as usize];
            let offset = source.window_offset;
            debug_assert!(offset <= 56);
            for pair in &mut pairs {
                let row_col = *pair;
                let row = row_col >> 6;
                let mut col = (row_col & 63) as u8;
                // first rotate the columns into a canonical configuration:
                //  new = ((old - (offset+8)) + 64) mod 64
                col = (col + 56 - offset) & 63;
                debug_assert!(col < 56);
                // then apply the permutation
                col = permutation[col as usize];
                *pair = (row << 6) | (col as u32);
            }

            pairs.sort_unstable();
            self.compress_surprising_values(&pairs, source.lg_k());
        }
    }

    fn compress_surprising_values(&mut self, pairs: &[u32], lg_k: u8) {
        let k = 1 << lg_k;
        let num_pairs = pairs.len() as u32;
        let num_base_bits = golomb_choose_number_of_base_bits(k + num_pairs, num_pairs as u64);
        let table_len = safe_length_for_compressed_pair_buf(k, num_pairs, num_base_bits);
        self.table_data.resize(table_len, 0);

        let compressed_surprising_values = self.low_level_compress_pairs(pairs, num_base_bits);

        // At this point we could free the unused portion of the compression output buffer,
        // but it is not necessary if it is temporary
        // Note: realloc caused strange timing spikes for lgK = 11 and 12.

        self.table_data_words = compressed_surprising_values;
        self.table_num_entries = num_pairs;
    }

    fn compress_sliding_window(&mut self, window: &[u8], lg_k: u8, num_coupons: u32) {
        let k = 1 << lg_k;
        let window_buf_len = safe_length_for_compressed_window_buf(k);
        self.window_data.resize(window_buf_len, 0);
        let pseudo_phase = determine_pseudo_phase(lg_k, num_coupons);
        let data_words = self.low_level_compress_bytes(
            window,
            k,
            &ENCODING_TABLES_FOR_HIGH_ENTROPY_BYTE[pseudo_phase as usize],
        );

        // At this point we could free the unused portion of the compression output buffer,
        // but it is not necessary if it is temporary
        // Note: realloc caused strange timing spikes for lgK = 11 and 12.

        self.window_data_words = data_words;
    }

    /// Returns the number of compressed words that were actually used.
    ///
    /// It is the caller's responsibility to ensure that the window_data is long enough.
    fn low_level_compress_bytes(
        &mut self,
        byte_array: &[u8],
        num_bytes_to_encode: u32,
        encoding_table: &[u16],
    ) -> usize {
        // bits are packed into this first, then are flushed to window_data
        let mut bitbuf = 0;
        // number of bits currently in bitbuf; must be between 0 and 31
        let mut bufbits = 0;
        let mut next_word_index = 0;

        for byte_index in 0..num_bytes_to_encode {
            let code_info = encoding_table[byte_array[byte_index as usize] as usize];
            let code_val = (code_info & 0xfff) as u64;
            let code_len = (code_info >> 12) as u8;
            bitbuf |= code_val << bufbits;
            bufbits += code_len;
            maybe_flush_bitbuf(
                &mut bitbuf,
                &mut bufbits,
                &mut self.window_data,
                &mut next_word_index,
            );
        }

        // Pad the bitstream with 11 zero-bits so that the decompressor's 12-bit peek can't overrun
        // its input.
        bufbits += 11;
        maybe_flush_bitbuf(
            &mut bitbuf,
            &mut bufbits,
            &mut self.window_data,
            &mut next_word_index,
        );

        if bufbits > 0 {
            // We are done encoding now, so we flush the bit buffer.
            debug_assert!(bufbits < 32);
            self.window_data[next_word_index] = (bitbuf & 0xffffffff) as u32;
            next_word_index += 1;

            // not really necessary unset since no more use
            //bitbuf = 0;
            //bufbits = 0;
        }

        next_word_index
    }

    /// Returns the number of table_data actually used.
    ///
    /// Here "pairs" refers to row/column pairs that specify the positions of surprising values in
    /// the bit matrix.
    fn low_level_compress_pairs(&mut self, pairs: &[u32], num_base_bits: u8) -> usize {
        let mut bitbuf = 0;
        let mut bufbits = 0;
        let mut next_word_index = 0;
        let golomb_lo_mask = ((1 << num_base_bits) - 1) as u64;
        let mut predicted_row_index = 0;
        let mut predicted_col_index = 0;

        for pair_index in 0..pairs.len() {
            let row_col = pairs[pair_index];
            let row_index = row_col >> 6;
            let col_index = row_col & 63;

            if row_index != predicted_row_index {
                predicted_col_index = 0;
            }

            assert!(row_index >= predicted_row_index);
            assert!(col_index >= predicted_col_index);

            let y_delta = row_index - predicted_row_index;
            let x_delta = col_index - predicted_col_index;

            predicted_row_index = row_index;
            predicted_col_index = col_index + 1;

            let code_info = LENGTH_LIMITED_UNARY_ENCODING_TABLE65[x_delta as usize];
            let code_val = (code_info & 0xfff) as u64;
            let code_len = (code_info >> 12) as u8;
            bitbuf |= code_val << bufbits;
            bufbits += code_len;

            maybe_flush_bitbuf(
                &mut bitbuf,
                &mut bufbits,
                &mut self.table_data,
                &mut next_word_index,
            );

            let golomb_lo = (y_delta as u64) & golomb_lo_mask;
            let golomb_hi = (y_delta as u64) >> num_base_bits;
            write_unary(
                &mut self.table_data,
                &mut next_word_index,
                &mut bitbuf,
                &mut bufbits,
                golomb_hi,
            );

            bitbuf |= golomb_lo << bufbits;
            bufbits += num_base_bits;
            maybe_flush_bitbuf(
                &mut bitbuf,
                &mut bufbits,
                &mut self.table_data,
                &mut next_word_index,
            );
        }

        // Pad the bitstream so that the decompressor's 12-bit peek can't overrun its input.
        let padding = 10u8.saturating_sub(num_base_bits);
        bufbits += padding;
        maybe_flush_bitbuf(
            &mut bitbuf,
            &mut bufbits,
            &mut self.table_data,
            &mut next_word_index,
        );

        if bufbits > 0 {
            // We are done encoding now, so we flush the bit buffer
            assert!(bufbits < 32);
            self.table_data[next_word_index] = (bitbuf & 0xffffffff) as u32;
            next_word_index += 1;

            // not really necessary unset since no more use
            //bitbuf = 0;
            //bufbits = 0;
        }

        next_word_index
    }
}

pub(super) struct UncompressedState {
    pub(super) table: PairTable,
    pub(super) window: Vec<u8>,
}

impl CompressedState {
    pub fn uncompress(&self, lg_k: u8, num_coupons: u32) -> UncompressedState {
        match determine_flavor(lg_k, num_coupons) {
            Flavor::Empty => UncompressedState {
                table: PairTable::new(2, lg_k + 6),
                window: vec![],
            },
            Flavor::Sparse => self.uncompress_sparse_flavor(lg_k),
            Flavor::Hybrid => self.uncompress_hybrid_flavor(lg_k),
            Flavor::Pinned => self.uncompress_pinned_flavor(lg_k, num_coupons),
            Flavor::Sliding => self.uncompress_sliding_flavor(lg_k, num_coupons),
        }
    }

    fn uncompress_sparse_flavor(&self, lg_k: u8) -> UncompressedState {
        debug_assert!(self.window_data.is_empty(), "window is not expected");
        debug_assert!(!self.table_data.is_empty(), "table is expected");

        let pairs = uncompress_surprising_values(
            &self.table_data,
            self.table_data_words,
            self.table_num_entries,
            lg_k,
        );

        UncompressedState {
            table: PairTable::from_slots(lg_k, self.table_num_entries, pairs),
            window: vec![],
        }
    }

    fn uncompress_hybrid_flavor(&self, lg_k: u8) -> UncompressedState {
        debug_assert!(self.window_data.is_empty(), "window is not expected");
        debug_assert!(!self.table_data.is_empty(), "table is expected");

        let mut pairs = uncompress_surprising_values(
            &self.table_data,
            self.table_data_words,
            self.table_num_entries,
            lg_k,
        );

        // In the hybrid flavor, some of these pairs actually belong in the window, so we will
        // separate them out, moving the "true" pairs to the bottom of the array.
        let k = 1 << lg_k;
        let mut window = vec![0u8; k]; // important: zero the memory
        let mut next_true_pair = 0;
        for i in 0..self.table_num_entries {
            let row_col = pairs[i as usize];
            assert_ne!(row_col, u32::MAX);
            let col = row_col & 63;
            if col < 8 {
                let row = row_col >> 6;
                window[row as usize] |= 1 << col; // set the window bit
            } else {
                pairs[next_true_pair as usize] = row_col;
                next_true_pair += 1;
            }
        }

        UncompressedState {
            table: PairTable::from_slots(lg_k, next_true_pair, pairs),
            window,
        }
    }

    fn uncompress_pinned_flavor(&self, lg_k: u8, num_coupons: u32) -> UncompressedState {
        debug_assert!(!self.window_data.is_empty(), "window is expected");

        let mut window = vec![];
        uncompress_sliding_window(
            &self.window_data,
            self.window_data_words,
            &mut window,
            lg_k,
            num_coupons,
        );
        let num_pairs = self.table_num_entries;
        let table = if num_pairs == 0 {
            PairTable::new(2, lg_k + 6)
        } else {
            debug_assert!(!self.table_data.is_empty(), "table is expected");
            let mut pairs = uncompress_surprising_values(
                &self.table_data,
                self.table_data_words,
                num_pairs,
                lg_k,
            );
            // undo the compressor's 8-column shift
            for i in 0..num_pairs {
                let i = i as usize;
                assert!(
                    (pairs[i] & 63) < 56,
                    "pair column index is invalid: {}",
                    pairs[i]
                );
                pairs[i] += 8;
            }
            PairTable::from_slots(lg_k, num_pairs, pairs)
        };
        UncompressedState { table, window }
    }

    fn uncompress_sliding_flavor(&self, lg_k: u8, num_coupons: u32) -> UncompressedState {
        debug_assert!(!self.window_data.is_empty(), "window is expected");

        let mut window = vec![];
        uncompress_sliding_window(
            &self.window_data,
            self.window_data_words,
            &mut window,
            lg_k,
            num_coupons,
        );
        let num_pairs = self.table_num_entries;
        let table = if num_pairs == 0 {
            PairTable::new(2, lg_k + 6)
        } else {
            debug_assert!(!self.table_data.is_empty(), "table is expected");
            let mut pairs = uncompress_surprising_values(
                &self.table_data,
                self.table_data_words,
                num_pairs,
                lg_k,
            );
            let pseudo_phase = determine_pseudo_phase(lg_k, num_coupons);
            let permutation = &COLUMN_PERMUTATIONS_FOR_DECODING[pseudo_phase as usize];
            let offset = determine_correct_offset(lg_k, num_coupons);
            assert!(offset <= 56, "offset is invalid: {offset}");

            for i in 0..num_pairs {
                let i = i as usize;
                let row_col = pairs[i];
                let row = row_col >> 6;
                let mut col = (row_col & 63) as u8;
                // first undo the permutation
                col = permutation[col as usize];
                // then undo the rotation: old = (new + (offset+8)) mod 64
                col = (col + (offset + 8)) & 63;
                pairs[i] = (row << 6) | (col as u32);
            }

            PairTable::from_slots(lg_k, num_pairs, pairs)
        };
        UncompressedState { table, window }
    }
}

fn uncompress_surprising_values(
    data: &[u32],
    data_words: usize,
    num_pairs: u32,
    lg_k: u8,
) -> Vec<u32> {
    let k = 1 << lg_k;
    let mut pairs = vec![0; num_pairs as usize];
    let num_base_bits = golomb_choose_number_of_base_bits(k + num_pairs, num_pairs as u64);
    low_level_uncompress_pairs(&mut pairs, num_pairs, num_base_bits, data, data_words);
    pairs
}

fn uncompress_sliding_window(
    data: &[u32],
    data_words: usize,
    window: &mut Vec<u8>,
    lg_k: u8,
    num_coupons: u32,
) {
    let k = 1 << lg_k;
    window.resize(k, 0);
    let pseudo_phase = determine_pseudo_phase(lg_k, num_coupons);
    low_level_uncompress_bytes(
        window,
        k as u32,
        data,
        data_words,
        &DECODING_TABLES_FOR_HIGH_ENTROPY_BYTE[pseudo_phase as usize],
    );
}

fn low_level_uncompress_pairs(
    pairs: &mut [u32],
    num_pairs_to_decode: u32,
    num_base_bits: u8,
    compressed_words: &[u32],
    num_compressed_words: usize,
) {
    let mut word_index = 0;
    let mut bitbuf = 0;
    let mut bufbits = 0;
    let golomb_lo_mask = (1 << num_base_bits) - 1;
    let mut predicted_row_index = 0u32;
    let mut predicted_col_index = 0u8;

    // for each pair we need to read:
    // x_delta (12-bit length-limited unary)
    // y_delta_hi (unary)
    // y_delta_lo (basebits)

    for pair_index in 0..num_pairs_to_decode {
        // ensure 12 bits in bit buffer
        maybe_fill_bitbuf(
            &mut bitbuf,
            &mut bufbits,
            compressed_words,
            &mut word_index,
            12,
        );
        let peek12 = bitbuf & 0xfff;
        let lookup = LENGTH_LIMITED_UNARY_DECODING_TABLE65[peek12 as usize];
        let code_word_length = (lookup >> 8) as u8;
        let x_delta = (lookup & 0xff) as u8;
        bitbuf >>= code_word_length;
        bufbits -= code_word_length;

        let golomb_hi = read_unary(compressed_words, &mut word_index, &mut bitbuf, &mut bufbits);
        // ensure num_base_bits in the bit buffer
        maybe_fill_bitbuf(
            &mut bitbuf,
            &mut bufbits,
            compressed_words,
            &mut word_index,
            num_base_bits,
        );
        let golomb_lo = bitbuf & golomb_lo_mask;
        bitbuf >>= num_base_bits;
        bufbits -= num_base_bits;
        let y_delta = ((golomb_hi << num_base_bits) | golomb_lo) as u32;

        // Now that we have x_delta and y_delta, we can compute the pair's row and column
        if y_delta > 0 {
            predicted_col_index = 0;
        }
        let row_index = predicted_row_index + y_delta;
        let col_index = predicted_col_index + x_delta;
        let row_col = (row_index << 6) | (col_index as u32);
        pairs[pair_index as usize] = row_col;
        predicted_row_index = row_index;
        predicted_col_index = col_index + 1;
    }

    debug_assert!(
        word_index <= num_compressed_words,
        "word_index: {word_index}, num_compressed_words: {num_compressed_words}",
    );
}

fn low_level_uncompress_bytes(
    byte_array: &mut [u8],
    num_bytes_to_decode: u32,
    compressed_words: &[u32],
    num_compressed_words: usize,
    decoding_table: &[u16],
) {
    let mut word_index = 0;
    let mut bitbuf = 0;
    let mut bufbits = 0;

    for byte_index in 0..num_bytes_to_decode {
        // ensure 12 bits in bit buffer
        maybe_fill_bitbuf(
            &mut bitbuf,
            &mut bufbits,
            compressed_words,
            &mut word_index,
            12,
        );
        // These 12 bits will include an entire Huffman codeword.
        let peek12 = bitbuf & 0xfff;
        let lookup = decoding_table[peek12 as usize];
        let code_word_length = (lookup >> 8) as u8;
        let decoded_byte = (lookup & 0xff) as u8;
        byte_array[byte_index as usize] = decoded_byte;
        bitbuf >>= code_word_length;
        bufbits -= code_word_length;
    }

    // Buffer over-run should be impossible unless there is a bug.
    debug_assert!(
        word_index <= num_compressed_words,
        "word_index: {word_index}, num_compressed_words: {num_compressed_words}",
    );
}

fn determine_pseudo_phase(lg_k: u8, num_coupons: u32) -> u8 {
    let k = 1 << lg_k;
    // This mid-range logic produces pseudo-phases. They are used to select encoding tables.
    // The thresholds were chosen by hand after looking at plots of measured compression.
    if 1000 * num_coupons < 2375 * k {
        if 4 * num_coupons < 3 * k {
            // mid-range table
            16
        } else if 10 * num_coupons < 11 * k {
            // mid-range table
            16 + 1
        } else if 100 * num_coupons < 132 * k {
            // mid-range table
            16 + 2
        } else if 3 * num_coupons < 5 * k {
            // mid-range table
            16 + 3
        } else if 1000 * num_coupons < 1965 * k {
            // mid-range table
            16 + 4
        } else if 1000 * num_coupons < 2275 * k {
            // mid-range table
            16 + 5
        } else {
            // steady-state table employed before its actual phase
            6
        }
    } else {
        // This steady-state logic produces true phases. They are used to select
        // encoding tables, and also column permutations for the "Sliding" flavor.
        debug_assert!(lg_k >= 4);
        let tmp = num_coupons >> (lg_k - 4);
        (tmp & 15) as u8 // phase
    }
}

fn write_unary(
    compressed_words: &mut [u32],
    next_word_index: &mut usize,
    bitbuf: &mut u64,
    bufbits: &mut u8,
    value: u64,
) {
    assert!(*bufbits <= 31);

    let mut remaining = value;
    while remaining >= 16 {
        remaining -= 16;
        // Here we output 16 zeros, but we don't need to physically write them into bitbuf
        // because it already contains zeros in that region.
        *bufbits += 16; // Record the fact that 16 bits of output have occurred.
        maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);
    }

    let the_unary_code = 1 << remaining;
    *bitbuf |= the_unary_code << *bufbits;
    *bufbits += (remaining + 1) as u8;
    maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);
}

fn read_unary(
    compressed_words: &[u32],
    next_word_index: &mut usize,
    bitbuf: &mut u64,
    bufbits: &mut u8,
) -> u64 {
    let mut subtotal = 0u64;
    loop {
        // ensure 8 bits in bit buffer
        maybe_fill_bitbuf(bitbuf, bufbits, compressed_words, next_word_index, 8);
        // These 8 bits include either all or part of the Unary codeword
        let peek8 = *bitbuf & 0xff;
        let trailing_zeros = peek8.trailing_zeros() as u8;
        if trailing_zeros < 8 {
            *bufbits -= 1 + trailing_zeros;
            *bitbuf >>= 1 + trailing_zeros;
            return subtotal + trailing_zeros as u64;
        }
        // The codeword was partial, so read some more
        subtotal += 8;
        *bufbits -= 8;
        *bitbuf >>= 8;
    }
}

fn maybe_flush_bitbuf(
    bitbuf: &mut u64,
    bufbits: &mut u8,
    word: &mut [u32],
    word_index: &mut usize,
) {
    if *bufbits >= 32 {
        word[*word_index] = (*bitbuf & 0xffffffff) as u32;
        *word_index += 1;
        *bitbuf >>= 32;
        *bufbits -= 32;
    }
}

fn maybe_fill_bitbuf(
    bitbuf: &mut u64,
    bufbits: &mut u8,
    words: &[u32],
    word_index: &mut usize,
    minbits: u8,
) {
    if *bufbits < minbits {
        *bitbuf |= (words[*word_index] as u64) << *bufbits;
        *word_index += 1;
        *bufbits += 32;
    }
}

// Explanation of padding: we write
// 1) xdelta (huffman, provides at least 1 bit, requires 12-bit lookahead)
// 2) ydeltaGolombHi (unary, provides at least 1 bit, requires 8-bit lookahead)
// 3) ydeltaGolombLo (straight B bits).
// So the 12-bit lookahead is the tight constraint, but there are at least (2 + B) bits emitted,
// so we would be safe with max (0, 10 - B) bits of padding at the end of the bitstream.
fn safe_length_for_compressed_window_buf(k: u32) -> usize {
    // 11 bits of padding, due to 12-bit lookahead, with 1 bit certainly present.
    let bits = 12 * k + 11;
    divide_longs_rounding_up(bits as usize, 32)
}

fn safe_length_for_compressed_pair_buf(k: u32, num_pairs: u32, num_base_bits: u8) -> usize {
    // Long ybits = k + numPairs; // simpler and safer UB
    // The following tighter UB on ybits is based on page 198
    // of the textbook "Managing Gigabytes" by Witten, Moffat, and Bell.
    // Notice that if numBaseBits == 0 it coincides with (k + numPairs).

    let k = k as usize;
    let num_pairs = num_pairs as usize;
    let num_base_bits = num_base_bits as usize;

    let ybits = num_pairs * (1 + num_base_bits) + (k >> num_base_bits);
    let xbits = 12 * (num_pairs);
    let padding = 10usize.saturating_sub(num_base_bits);
    divide_longs_rounding_up(xbits + ybits + padding, 32)
}

fn divide_longs_rounding_up(x: usize, y: usize) -> usize {
    debug_assert_ne!(y, 0);
    let quotient = x / y;
    if quotient * y == x {
        quotient
    } else {
        quotient + 1
    }
}

/// Returns an integer that is between zero and ceil(log_2(k)) - 1, inclusive.
fn golomb_choose_number_of_base_bits(k: u32, count: u64) -> u8 {
    debug_assert!(k > 0);
    debug_assert!(count > 0);
    let quotient = ((k as u64) - count) / count; // integer division
    if quotient == 0 {
        0
    } else {
        floor_log2_of_long(quotient)
    }
}

fn floor_log2_of_long(x: u64) -> u8 {
    debug_assert!(x > 0);
    let mut p = 0u8;
    let mut y = 1u64;
    loop {
        match u64::cmp(&y, &x) {
            Ordering::Equal => return p,
            Ordering::Greater => return p - 1,
            Ordering::Less => {
                p += 1;
                y <<= 1;
            }
        }
    }
}
