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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::error::ErrorKind;

pub(super) const MAX_WORDS: u64 = i32::MAX as u64 - Family::BLOOMFILTER.max_pre_longs as u64;
pub(super) const MAX_NUM_BITS: u64 = MAX_WORDS * 64;
const MAX_ALLOCATABLE_WORDS: u64 = (isize::MAX as usize / size_of::<u64>()) as u64;

// Scalar fields precede `words` so derived `PartialEq` short-circuits on `bits_used`.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct BitArray {
    bits_used: u64,
    multiplier: u64,
    shift: u32,
    words: Box<[u64]>,
}

impl BitArray {
    pub(super) fn new(num_bits: u64) -> Result<Self, Error> {
        let num_words = num_bits.div_ceil(64);
        if !(1..=MAX_WORDS).contains(&num_words) {
            return Err(Error::invalid_argument(format!(
                "num_bits must be within 1..={MAX_NUM_BITS}, got {num_bits}"
            )));
        }
        let word_len = allocatable_len(num_words, ErrorKind::InvalidArgument)?;
        let words = vec![0_u64; word_len].into_boxed_slice();
        Ok(Self::from_words(0, words))
    }

    pub(super) fn deserialize(mut cursor: SketchSlice<'_>, is_empty: bool) -> Result<Self, Error> {
        let num_longs = cursor
            .read_i32_le()
            .map_err(insufficient_data("num_longs"))?;
        let _unused = cursor.read_u32_le().map_err(insufficient_data("unused"))?;

        if !(1..=MAX_WORDS as i32).contains(&num_longs) {
            return Err(Error::deserial(format!(
                "invalid num_longs: expected [1, {MAX_WORDS}], got {num_longs}"
            )));
        }
        let num_words = num_longs as u64;
        let word_len = allocatable_len(num_words, ErrorKind::InvalidData)?;

        if !is_empty {
            let payload_bytes = num_words
                .checked_add(1)
                .and_then(|words| words.checked_mul(size_of::<u64>() as u64))
                .and_then(|bytes| usize::try_from(bytes).ok())
                .ok_or_else(|| Error::deserial("Bloom filter payload length overflows"))?;
            let available_bytes = cursor.remaining().len();
            if available_bytes < payload_bytes {
                return Err(Error::insufficient_data_of(
                    "Bloom filter payload",
                    format_args!("expected {payload_bytes} bytes, got {available_bytes}"),
                ));
            }
        }

        let mut words = vec![0_u64; word_len].into_boxed_slice();
        let bits_used = if is_empty {
            0
        } else {
            let serialized_num_bits_set = cursor
                .read_u64_le()
                .map_err(insufficient_data("num_bits_set"))?;
            let mut ones = 0_u64;
            for word in &mut words {
                *word = cursor
                    .read_u64_le()
                    .map_err(insufficient_data("bit_array"))?;
                ones += u64::from(word.count_ones());
            }
            if serialized_num_bits_set != u64::MAX && serialized_num_bits_set != ones {
                return Err(Error::deserial(format!(
                    "invalid num_bits_set: expected {ones}, got {serialized_num_bits_set}"
                )));
            }
            ones
        };

        Ok(Self::from_words(bits_used, words))
    }

    fn from_words(bits_used: u64, words: Box<[u64]>) -> Self {
        let capacity = (words.len() as u64) << 6;
        // `capacity` is a positive multiple of 64, so `2^(l - 1) < capacity <= 2^l`
        // and `ceil(2^(63 + l) / capacity)` fits in `u64`.
        let l = 64 - (capacity - 1).leading_zeros();
        let multiplier = (1_u128 << (63 + l)).div_ceil(u128::from(capacity)) as u64;
        Self {
            bits_used,
            multiplier,
            shift: l - 1,
            words,
        }
    }

    #[inline(always)]
    pub(super) fn contains(&self, h0: u64, h1: u64, num_hashes: u16) -> bool {
        let capacity = self.capacity();
        let multiplier = self.multiplier;
        let shift = self.shift;
        let words = &*self.words;
        for i in 1..=num_hashes {
            let (word_index, mask) = probe_slot(h0, h1, i, capacity, multiplier, shift);
            if (words[word_index] & mask) == 0 {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    pub(super) fn insert(&mut self, h0: u64, h1: u64, num_hashes: u16) {
        let capacity = self.capacity();
        let multiplier = self.multiplier;
        let shift = self.shift;
        let words = &mut *self.words;
        let mut bits_added = 0_u64;
        for i in 1..=num_hashes {
            let (word_index, mask) = probe_slot(h0, h1, i, capacity, multiplier, shift);
            let word = &mut words[word_index];
            let was_set = (*word & mask) != 0;
            *word |= mask;
            bits_added += u64::from(!was_set);
        }
        self.bits_used += bits_added;
    }

    #[inline(always)]
    pub(super) fn contains_and_insert(&mut self, h0: u64, h1: u64, num_hashes: u16) -> bool {
        let capacity = self.capacity();
        let multiplier = self.multiplier;
        let shift = self.shift;
        let words = &mut *self.words;
        let mut bits_added = 0_u64;
        for i in 1..=num_hashes {
            let (word_index, mask) = probe_slot(h0, h1, i, capacity, multiplier, shift);
            let word = &mut words[word_index];
            if (*word & mask) == 0 {
                *word |= mask;
                bits_added += 1;
            }
        }
        if bits_added != 0 {
            self.bits_used += bits_added;
            return false;
        }
        true
    }

    pub(super) fn reset(&mut self) {
        self.words.fill(0);
        self.bits_used = 0;
    }

    fn combine(&mut self, other: &Self, op: impl Fn(&mut u64, u64)) {
        debug_assert_eq!(self.words.len(), other.words.len());
        let mut bits_used = 0_u64;
        for (word, &other_word) in self.words.iter_mut().zip(&other.words) {
            op(word, other_word);
            bits_used += u64::from(word.count_ones());
        }
        self.bits_used = bits_used;
    }

    pub(super) fn union(&mut self, other: &Self) {
        self.combine(other, |word, other_word| *word |= other_word);
    }

    pub(super) fn intersect(&mut self, other: &Self) {
        self.combine(other, |word, other_word| *word &= other_word);
    }

    pub(super) fn difference(&mut self, other: &Self) {
        self.combine(other, |word, other_word| *word &= !other_word);
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.bits_used == 0
    }

    #[inline]
    pub(super) fn bits_used(&self) -> u64 {
        self.bits_used
    }

    #[inline]
    pub(super) fn word_len(&self) -> usize {
        self.words.len()
    }

    #[inline]
    pub(super) fn capacity(&self) -> u64 {
        (self.words.len() as u64) << 6
    }

    #[inline]
    pub(super) fn byte_len(&self) -> usize {
        size_of_val::<[u64]>(&self.words)
    }

    pub(super) fn serialize(&self, bytes: &mut SketchBytes) {
        bytes.write_i32_le(self.words.len() as i32);
        bytes.write_u32_le(0);
        if !self.is_empty() {
            bytes.write_u64_le(self.bits_used);
            for &word in &self.words {
                bytes.write_u64_le(word);
            }
        }
    }
}

fn allocatable_len(num_words: u64, kind: ErrorKind) -> Result<usize, Error> {
    if num_words > MAX_ALLOCATABLE_WORDS {
        return Err(Error::new(
            kind,
            format!(
                "requested {num_words} words exceeds maximum addressable allocation ({MAX_ALLOCATABLE_WORDS})"
            ),
        ));
    }
    Ok(num_words as usize)
}

#[inline(always)]
fn probe_slot(
    h0: u64,
    h1: u64,
    i: u16,
    capacity: u64,
    multiplier: u64,
    shift: u32,
) -> (usize, u64) {
    let shifted = h0.wrapping_add(u64::from(i).wrapping_mul(h1)) >> 1;
    let bit_index = bit_index(shifted, capacity, multiplier, shift);
    let word_index = (bit_index >> 6) as usize;
    let mask = 1_u64 << (bit_index & 63);
    (word_index, mask)
}

#[inline(always)]
fn bit_index(shifted: u64, capacity: u64, multiplier: u64, shift: u32) -> u64 {
    debug_assert!(shifted < 1 << 63);
    // Granlund-Montgomery invariant division: (multiplier * shifted) >> (63 + l).
    let quotient = ((u128::from(shifted) * u128::from(multiplier)) >> 64) as u64 >> shift;
    shifted - quotient * capacity
}
