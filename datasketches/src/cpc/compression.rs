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

use crate::codec::SketchBytes;
use crate::cpc::compression_data::DECODING_TABLES_FOR_HIGH_ENTROPY_BYTE;
use crate::cpc::compression_data::ENCODING_TABLES_FOR_HIGH_ENTROPY_BYTE;
use crate::cpc::compression_data::LENGTH_LIMITED_UNARY_DECODING_TABLE65;
use crate::cpc::compression_data::LENGTH_LIMITED_UNARY_ENCODING_TABLE65;
use crate::error::Error;

pub(super) fn encode_pairs(pairs: &[u32], lg_k: u8, output: &mut SketchBytes) -> usize {
    let num_pairs = pairs.len() as u32;
    let num_base_bits =
        golomb_choose_number_of_base_bits((1 << lg_k) + num_pairs, u64::from(num_pairs));
    let mut bits = BitWriter::new(output);
    let golomb_lo_mask = (1 << num_base_bits) - 1;
    let mut predicted_row_index = 0;
    let mut predicted_col_index = 0;

    for &row_col in pairs {
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
        bits.write(u64::from(code_info & 0xfff), (code_info >> 12) as u8);
        bits.write_unary(u64::from(y_delta >> num_base_bits));
        bits.write(u64::from(y_delta & golomb_lo_mask), num_base_bits);
    }

    bits.pad(10u8.saturating_sub(num_base_bits));
    bits.finish()
}

pub(super) fn encode_window(
    window: &[u8],
    lg_k: u8,
    num_coupons: u32,
    output: &mut SketchBytes,
) -> usize {
    let pseudo_phase = determine_pseudo_phase(lg_k, num_coupons);
    let encoding_table = &ENCODING_TABLES_FOR_HIGH_ENTROPY_BYTE[pseudo_phase as usize];
    let mut bits = BitWriter::new(output);
    for &byte in window {
        let code_info = encoding_table[byte as usize];
        bits.write(u64::from(code_info & 0xfff), (code_info >> 12) as u8);
    }
    bits.pad(11);
    bits.finish()
}

struct BitWriter<'a> {
    output: &'a mut SketchBytes,
    buffer: u64,
    buffered_bits: u8,
    words_written: usize,
}

impl<'a> BitWriter<'a> {
    fn new(output: &'a mut SketchBytes) -> Self {
        Self {
            output,
            buffer: 0,
            buffered_bits: 0,
            words_written: 0,
        }
    }

    fn write(&mut self, value: u64, count: u8) {
        self.buffer |= value << self.buffered_bits;
        self.buffered_bits += count;
        self.flush_full_word();
    }

    fn write_unary(&mut self, value: u64) {
        let mut remaining = value;
        while remaining >= 16 {
            self.pad(16);
            remaining -= 16;
        }
        self.write(1 << remaining, (remaining + 1) as u8);
    }

    fn pad(&mut self, count: u8) {
        self.buffered_bits += count;
        self.flush_full_word();
    }

    fn flush_full_word(&mut self) {
        if self.buffered_bits >= 32 {
            self.output.write_u32_le(self.buffer as u32);
            self.words_written += 1;
            self.buffer >>= 32;
            self.buffered_bits -= 32;
        }
    }

    fn finish(mut self) -> usize {
        if self.buffered_bits > 0 {
            debug_assert!(self.buffered_bits < 32);
            self.output.write_u32_le(self.buffer as u32);
            self.words_written += 1;
        }
        self.words_written
    }
}

pub(super) fn decode_pairs(data: &[u8], num_pairs: u32, lg_k: u8) -> Result<Vec<u32>, Error> {
    if num_pairs == 0 {
        return Ok(vec![]);
    }
    let k = 1 << lg_k;
    let mut pairs = vec![0; num_pairs as usize];
    let num_base_bits = golomb_choose_number_of_base_bits(k + num_pairs, num_pairs as u64);
    let mut bits = BitReader::new(data);
    let golomb_lo_mask = (1 << num_base_bits) - 1;
    let mut predicted_row_index = 0u32;
    let mut predicted_col_index = 0u32;

    // for each pair we need to read:
    // x_delta (12-bit length-limited unary)
    // y_delta_hi (unary)
    // y_delta_lo (basebits)

    for pair_index in 0..num_pairs {
        let peek12 = bits.peek(12)?;
        let lookup = LENGTH_LIMITED_UNARY_DECODING_TABLE65[peek12 as usize];
        let code_word_length = (lookup >> 8) as u8;
        let x_delta = u32::from((lookup & 0xff) as u8);
        bits.consume(code_word_length);

        let golomb_hi = bits.read_unary()?;
        let golomb_lo = bits.read(num_base_bits)? & golomb_lo_mask;
        let y_delta = golomb_hi
            .checked_shl(u32::from(num_base_bits))
            .and_then(|high| high.checked_add(golomb_lo))
            .and_then(|delta| u32::try_from(delta).ok())
            .ok_or_else(|| Error::deserial("CPC pair row delta overflows"))?;

        // Now that we have x_delta and y_delta, we can compute the pair's row and column
        if y_delta > 0 {
            predicted_col_index = 0;
        }
        let row_index = predicted_row_index
            .checked_add(y_delta)
            .filter(|&row| row < k)
            .ok_or_else(|| Error::deserial("CPC pair row index is out of range"))?;
        let col_index = predicted_col_index
            .checked_add(x_delta)
            .filter(|&column| column < 64)
            .ok_or_else(|| Error::deserial("CPC pair column index is out of range"))?;
        let row_col = (row_index << 6) | col_index;
        if row_col == u32::MAX {
            return Err(Error::deserial(
                "CPC pair uses the reserved empty-table sentinel",
            ));
        }
        pairs[pair_index as usize] = row_col;
        predicted_row_index = row_index;
        predicted_col_index = col_index + 1;
    }
    Ok(pairs)
}

pub(super) fn decode_window(data: &[u8], lg_k: u8, num_coupons: u32) -> Result<Vec<u8>, Error> {
    let mut window = vec![0; 1 << lg_k];
    let pseudo_phase = determine_pseudo_phase(lg_k, num_coupons);
    let decoding_table = &DECODING_TABLES_FOR_HIGH_ENTROPY_BYTE[pseudo_phase as usize];
    let mut bits = BitReader::new(data);

    for byte in &mut window {
        // These 12 bits will include an entire Huffman codeword.
        let peek12 = bits.peek(12)?;
        let lookup = decoding_table[peek12 as usize];
        let code_word_length = (lookup >> 8) as u8;
        *byte = (lookup & 0xff) as u8;
        bits.consume(code_word_length);
    }
    Ok(window)
}

struct BitReader<'a> {
    bytes: &'a [u8],
    next_byte: usize,
    buffer: u64,
    buffered_bits: u8,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            next_byte: 0,
            buffer: 0,
            buffered_bits: 0,
        }
    }

    fn fill(&mut self, needed: u8) -> Result<(), Error> {
        if self.buffered_bits < needed {
            let word = self
                .bytes
                .get(self.next_byte..self.next_byte + size_of::<u32>())
                .ok_or_else(|| Error::deserial("CPC compressed stream is truncated"))?;
            let word = u32::from_le_bytes(word.try_into().unwrap());
            self.buffer |= u64::from(word) << self.buffered_bits;
            self.next_byte += size_of::<u32>();
            self.buffered_bits += 32;
        }
        Ok(())
    }

    fn peek(&mut self, count: u8) -> Result<u64, Error> {
        self.fill(count)?;
        Ok(self.buffer & ((1u64 << count) - 1))
    }

    fn consume(&mut self, count: u8) {
        debug_assert!(count <= self.buffered_bits);
        self.buffer >>= count;
        self.buffered_bits -= count;
    }

    fn read(&mut self, count: u8) -> Result<u64, Error> {
        if count == 0 {
            return Ok(0);
        }
        let value = self.peek(count)?;
        self.consume(count);
        Ok(value)
    }

    fn read_unary(&mut self) -> Result<u64, Error> {
        let mut value = 0u64;
        loop {
            let byte = self.peek(8)?;
            let zeros = byte.trailing_zeros() as u8;
            if zeros < 8 {
                self.consume(zeros + 1);
                return value
                    .checked_add(u64::from(zeros))
                    .ok_or_else(|| Error::deserial("CPC unary value overflows"));
            }
            value = value
                .checked_add(8)
                .ok_or_else(|| Error::deserial("CPC unary value overflows"))?;
            self.consume(8);
        }
    }
}

pub(super) fn determine_pseudo_phase(lg_k: u8, num_coupons: u32) -> u8 {
    let k = 1u64 << lg_k;
    let num_coupons = u64::from(num_coupons);
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

#[cfg(test)]
mod tests {
    use super::decode_pairs;
    use super::determine_pseudo_phase;
    use super::encode_pairs;
    use crate::codec::SketchBytes;

    #[test]
    fn pseudo_phase_handles_maximum_lg_k() {
        assert!(determine_pseudo_phase(26, 1 << 25) < 22);
        assert!(determine_pseudo_phase(26, u32::MAX) < 22);
    }

    #[test]
    fn pair_decoder_rejects_empty_table_sentinel() {
        let mut compressed = SketchBytes::with_capacity(16);
        encode_pairs(&[u32::MAX], 26, &mut compressed);
        let compressed = compressed.into_bytes();

        let error = decode_pairs(&compressed, 1, 26);
        assert_eq!(
            error.unwrap_err().message(),
            "CPC pair uses the reserved empty-table sentinel"
        );
    }
}
