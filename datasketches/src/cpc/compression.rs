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

use crate::cpc::compression_data::LENGTH_LIMITED_UNARY_ENCODING_TABLE65;
use crate::cpc::pair_table::{PairTable, introspective_insertion_sort};
use crate::cpc::{CpcSketch, Flavor};
use std::cmp::Ordering;

pub(super) struct CompressedState {
    table_data: Vec<u32>,
    table_data_words: usize,
    // can be different from the number of entries in the sketch in hybrid mode
    table_num_entries: u32,
    window_data: Vec<u32>,
    window_data_words: usize,
}

impl CompressedState {
    pub fn compress(&mut self, source: &CpcSketch) {
        match source.flavor() {
            Flavor::EMPTY => {
                // do nothing
                return;
            }
            Flavor::SPARSE => {
                self.compress_sparse_flavor(source);
                debug_assert!(self.window_data.is_empty(), "window is not expected");
                debug_assert!(!self.table_data.is_empty(), "table is expected");
            }
            Flavor::HYBRID => {
                self.compress_hybrid_flavor(source);
                debug_assert!(self.window_data.is_empty(), "window is not expected");
                debug_assert!(!self.table_data.is_empty(), "table is expected");
            }
            Flavor::PINNED => {
                self.compress_pinned_flavor(source);
                debug_assert!(!self.window_data.is_empty(), "window is expected");
            }
            Flavor::SLIDING => {
                self.compress_sliding_flavor(source);
                debug_assert!(!self.window_data.is_empty(), "window is expected");
            }
        }
    }

    fn compress_sparse_flavor(&mut self, source: &CpcSketch) {
        debug_assert!(source.sliding_window.is_empty());
        let mut pairs = source.surprising_value_table().unwrapping_get_items();
        introspective_insertion_sort(&mut pairs);
        self.compress_surprising_values(&pairs, source.lg_k());
    }

    fn compress_hybrid_flavor(&mut self, source: &CpcSketch) {
        debug_assert!(!source.sliding_window.is_empty());
        debug_assert_eq!(source.window_offset, 0);

        let k = 1 << source.lg_k();
        let mut pairs = source.surprising_value_table().unwrapping_get_items();
        if !pairs.is_empty() {
            introspective_insertion_sort(&mut pairs);
        }
        let num_pairs_from_table = pairs.len() as u32;
        let num_pairs_from_window = source.num_coupons() - num_pairs_from_table;

        let mut all_pairs = tricky_get_pairs_from_window(
            &source.sliding_window,
            k,
            num_pairs_from_window,
            num_pairs_from_table,
        );
        // u32_table<A>::merge(
        //     pairs_from_table.data(), 0, pairs_from_table.size(),
        //     all_pairs.data(), num_pairs_from_table, num_pairs_from_window,
        //     all_pairs.data(), 0
        // );  // note the overlapping subarray trick

        self.compress_surprising_values(&all_pairs, source.lg_k());
    }

    fn compress_pinned_flavor(&mut self, source: &CpcSketch) {}

    fn compress_sliding_flavor(&mut self, source: &CpcSketch) {}

    fn compress_surprising_values(&mut self, pairs: &[u32], lg_k: u8) {
        let k = 1 << lg_k;
        let num_pairs = pairs.len() as u32;
        let num_base_bits = golomb_choose_number_of_base_bits(k + num_pairs, num_pairs as u64);
        let table_len = safe_length_for_compressed_pair_buf(k, num_pairs, num_base_bits);
        self.table_data.truncate(table_len);

        let compressed_surprising_values = self.low_level_compress_pairs(&pairs, num_base_bits);

        // At this point we could free the unused portion of the compression output buffer,
        // but it is not necessary if it is temporary
        // Note: realloc caused strange timing spikes for lgK = 11 and 12.

        self.table_data_words = compressed_surprising_values;
        self.table_num_entries = num_pairs;
    }

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
            let code_val = code_info & 0xfff;
            let code_len = (code_info >> 12) as u8;
            bitbuf |= (code_val << bufbits) as u64;
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

            // not really necessary unset
            //bitbuf = 0;
            //bufbits = 0;
        }

        next_word_index
    }
}

pub(super) struct UncompressedState {
    table: PairTable,
    window: Vec<u8>,
}

/// The empty space that this leaves at the beginning of the output array will be filled in later
/// by the caller.
fn tricky_get_pairs_from_window(
    window: &[u8],
    k: usize,
    num_pairs_to_get: u32,
    empty_space: u32,
) -> Vec<u32> {
    let output_length = empty_space + num_pairs_to_get;
    let mut pairs = vec![0; output_length as usize];
    let mut pair_index = empty_space;
    for row_index in 0..k {
        let mut byte = window[row_index];
        while byte != 0 {
            let col_index = byte.trailing_zeros();
            byte = byte ^ (1 << col_index); // erase the 1
            pairs[pair_index as usize] = ((row_index << 6) as u32) | col_index;
            pair_index += 1;
        }
    }
    assert_eq!(pair_index, output_length);
    pairs
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
