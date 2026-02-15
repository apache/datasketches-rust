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

const BLOCK_WIDTH: usize = 8;

#[inline]
fn low_bit_to_byte_mask(bits: u8) -> u8 {
    if bits >= u8::BITS as u8 {
        u8::MAX
    } else {
        (1u8 << bits) - 1
    }
}

/// Packs values into a byte buffer with arbitrary bit widths.
///
/// # Panics
///
/// Panics if the buffer is too small to hold the packed values.
/// The caller must ensure that `bytes` has enough capacity for
/// the total number of bits to be packed.
pub(crate) struct BitPacker<'a> {
    bytes: &'a mut [u8],
    byte_index: usize,
    byte_bit_used: u8,
}

impl<'a> BitPacker<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Self {
        BitPacker {
            bytes,
            byte_index: 0,
            byte_bit_used: 0,
        }
    }

    /// Return used number of byte.
    pub fn byte_used(&self) -> usize {
        if self.byte_bit_used == 0 {
            self.byte_index
        } else {
            self.byte_index + 1
        }
    }

    /// Packs `value` represent as `bits` of bit into `bytes`.
    ///
    /// # Panics
    ///
    /// Panics if packing the value would exceed the buffer bounds.
    pub fn pack_value(&mut self, value: u64, mut bits: u8) {
        debug_assert!(self.byte_bit_used < 8, "offset must be in [0, 7]");

        if self.byte_bit_used > 0 {
            let remain_bits = 8 - self.byte_bit_used;
            let remain_mask = low_bit_to_byte_mask(remain_bits);

            // Fast path: there is enough space for remain byte to pack whole value.
            if bits < remain_bits {
                self.bytes[self.byte_index] |=
                    ((value << (remain_bits - bits)) as u8) & remain_mask;
                self.byte_bit_used += bits;
                return;
            }

            // Pack highest remain_bits bit first.
            self.bytes[self.byte_index] |= ((value >> (bits - remain_bits)) as u8) & remain_mask;
            bits -= remain_bits;
            self.byte_bit_used = 0;
            self.byte_index += 1;
        }

        while bits >= 8 {
            self.bytes[self.byte_index] = (value >> (bits - 8)) as u8;
            self.byte_index += 1;
            bits -= 8;
        }

        if bits > 0 {
            self.bytes[self.byte_index] = (value << (8 - bits)) as u8;
            self.byte_bit_used = bits;
        }
    }
}

/// Unpacks values from a byte buffer with arbitrary bit widths.
///
/// # Panics
///
/// Panics if the buffer is too small to provide the requested bits.
/// The caller must ensure that `bytes` has enough capacity for
/// the total number of bits to be unpacked.
pub(crate) struct BitUnpacker<'a> {
    bytes: &'a [u8],
    byte_index: usize,
    byte_bit_used: u8,
}

impl<'a> BitUnpacker<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            byte_index: 0,
            byte_bit_used: 0,
        }
    }

    /// Unpack `value` represent as `bits` of bit from `bytes`.
    ///
    /// # Panics
    ///
    /// Panics if unpacking the value would exceed the buffer bounds.
    pub fn unpack_value(&mut self, mut bits: u8) -> u64 {
        if bits == 0 {
            return 0;
        }

        let avail_bits = 8 - self.byte_bit_used;
        let chunk_bits = avail_bits.min(bits);
        let chunk_mask = low_bit_to_byte_mask(chunk_bits);

        let mut value =
            ((self.bytes[self.byte_index] >> (avail_bits - chunk_bits)) & chunk_mask) as u64;
        // Use all remain bits for current byte.
        if chunk_bits == avail_bits {
            self.byte_index += 1;
        }
        self.byte_bit_used = (self.byte_bit_used + chunk_bits) & 7;
        bits -= chunk_bits;

        while bits >= 8 {
            value = (value << 8) | self.bytes[self.byte_index] as u64;
            self.byte_index += 1;
            bits -= 8;
        }

        if bits > 0 {
            value = (value << bits) | (self.bytes[self.byte_index] >> (8 - bits)) as u64;
            self.byte_bit_used = bits;
        }

        value
    }
}

#[inline]
fn pack_bits_1(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(!bytes.is_empty());
    bytes[0] = ((((values[0]) & 0x1) << 7)
        | (((values[1]) & 0x1) << 6)
        | (((values[2]) & 0x1) << 5)
        | (((values[3]) & 0x1) << 4)
        | (((values[4]) & 0x1) << 3)
        | (((values[5]) & 0x1) << 2)
        | (((values[6]) & 0x1) << 1)
        | ((values[7]) & 0x1)) as u8;
}

#[inline]
fn pack_bits_2(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 2);
    bytes[0] = ((((values[0]) & 0x3) << 6)
        | (((values[1]) & 0x3) << 4)
        | (((values[2]) & 0x3) << 2)
        | ((values[3]) & 0x3)) as u8;
    bytes[1] = ((((values[4]) & 0x3) << 6)
        | (((values[5]) & 0x3) << 4)
        | (((values[6]) & 0x3) << 2)
        | ((values[7]) & 0x3)) as u8;
}

#[inline]
fn pack_bits_3(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 3);
    bytes[0] =
        ((((values[0]) & 0x7) << 5) | (((values[1]) & 0x7) << 2) | ((values[2] >> 1) & 0x3)) as u8;
    bytes[1] = ((((values[2]) & 0x1) << 7)
        | (((values[3]) & 0x7) << 4)
        | (((values[4]) & 0x7) << 1)
        | ((values[5] >> 2) & 0x1)) as u8;
    bytes[2] =
        ((((values[5]) & 0x3) << 6) | (((values[6]) & 0x7) << 3) | ((values[7]) & 0x7)) as u8;
}

#[inline]
fn pack_bits_4(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 4);
    bytes[0] = ((((values[0]) & 0xf) << 4) | ((values[1]) & 0xf)) as u8;
    bytes[1] = ((((values[2]) & 0xf) << 4) | ((values[3]) & 0xf)) as u8;
    bytes[2] = ((((values[4]) & 0xf) << 4) | ((values[5]) & 0xf)) as u8;
    bytes[3] = ((((values[6]) & 0xf) << 4) | ((values[7]) & 0xf)) as u8;
}

#[inline]
fn pack_bits_5(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 5);
    bytes[0] = ((((values[0]) & 0x1f) << 3) | ((values[1] >> 2) & 0x7)) as u8;
    bytes[1] =
        ((((values[1]) & 0x3) << 6) | (((values[2]) & 0x1f) << 1) | ((values[3] >> 4) & 0x1)) as u8;
    bytes[2] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 1) & 0xf)) as u8;
    bytes[3] =
        ((((values[4]) & 0x1) << 7) | (((values[5]) & 0x1f) << 2) | ((values[6] >> 3) & 0x3)) as u8;
    bytes[4] = ((((values[6]) & 0x7) << 5) | ((values[7]) & 0x1f)) as u8;
}

#[inline]
fn pack_bits_6(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 6);
    bytes[0] = ((((values[0]) & 0x3f) << 2) | ((values[1] >> 4) & 0x3)) as u8;
    bytes[1] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 2) & 0xf)) as u8;
    bytes[2] = ((((values[2]) & 0x3) << 6) | ((values[3]) & 0x3f)) as u8;
    bytes[3] = ((((values[4]) & 0x3f) << 2) | ((values[5] >> 4) & 0x3)) as u8;
    bytes[4] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 2) & 0xf)) as u8;
    bytes[5] = ((((values[6]) & 0x3) << 6) | ((values[7]) & 0x3f)) as u8;
}

#[inline]
fn pack_bits_7(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 7);
    bytes[0] = ((((values[0]) & 0x7f) << 1) | ((values[1] >> 6) & 0x1)) as u8;
    bytes[1] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 5) & 0x3)) as u8;
    bytes[2] = ((((values[2]) & 0x1f) << 3) | ((values[3] >> 4) & 0x7)) as u8;
    bytes[3] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 3) & 0xf)) as u8;
    bytes[4] = ((((values[4]) & 0x7) << 5) | ((values[5] >> 2) & 0x1f)) as u8;
    bytes[5] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 1) & 0x3f)) as u8;
    bytes[6] = ((((values[6]) & 0x1) << 7) | ((values[7]) & 0x7f)) as u8;
}

#[inline]
fn pack_bits_8(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 8);
    bytes[0] = ((values[0]) & 0xff) as u8;
    bytes[1] = ((values[1]) & 0xff) as u8;
    bytes[2] = ((values[2]) & 0xff) as u8;
    bytes[3] = ((values[3]) & 0xff) as u8;
    bytes[4] = ((values[4]) & 0xff) as u8;
    bytes[5] = ((values[5]) & 0xff) as u8;
    bytes[6] = ((values[6]) & 0xff) as u8;
    bytes[7] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_9(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 9);
    bytes[0] = ((values[0] >> 1) & 0xff) as u8;
    bytes[1] = ((((values[0]) & 0x1) << 7) | ((values[1] >> 2) & 0x7f)) as u8;
    bytes[2] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 3) & 0x3f)) as u8;
    bytes[3] = ((((values[2]) & 0x7) << 5) | ((values[3] >> 4) & 0x1f)) as u8;
    bytes[4] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 5) & 0xf)) as u8;
    bytes[5] = ((((values[4]) & 0x1f) << 3) | ((values[5] >> 6) & 0x7)) as u8;
    bytes[6] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 7) & 0x3)) as u8;
    bytes[7] = ((((values[6]) & 0x7f) << 1) | ((values[7] >> 8) & 0x1)) as u8;
    bytes[8] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_10(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 10);
    bytes[0] = ((values[0] >> 2) & 0xff) as u8;
    bytes[1] = ((((values[0]) & 0x3) << 6) | ((values[1] >> 4) & 0x3f)) as u8;
    bytes[2] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 6) & 0xf)) as u8;
    bytes[3] = ((((values[2]) & 0x3f) << 2) | ((values[3] >> 8) & 0x3)) as u8;
    bytes[4] = ((values[3]) & 0xff) as u8;
    bytes[5] = ((values[4] >> 2) & 0xff) as u8;
    bytes[6] = ((((values[4]) & 0x3) << 6) | ((values[5] >> 4) & 0x3f)) as u8;
    bytes[7] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 6) & 0xf)) as u8;
    bytes[8] = ((((values[6]) & 0x3f) << 2) | ((values[7] >> 8) & 0x3)) as u8;
    bytes[9] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_11(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 11);
    bytes[0] = ((values[0] >> 3) & 0xff) as u8;
    bytes[1] = ((((values[0]) & 0x7) << 5) | ((values[1] >> 6) & 0x1f)) as u8;
    bytes[2] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 9) & 0x3)) as u8;
    bytes[3] = ((values[2] >> 1) & 0xff) as u8;
    bytes[4] = ((((values[2]) & 0x1) << 7) | ((values[3] >> 4) & 0x7f)) as u8;
    bytes[5] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 7) & 0xf)) as u8;
    bytes[6] = ((((values[4]) & 0x7f) << 1) | ((values[5] >> 10) & 0x1)) as u8;
    bytes[7] = ((values[5] >> 2) & 0xff) as u8;
    bytes[8] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 5) & 0x3f)) as u8;
    bytes[9] = ((((values[6]) & 0x1f) << 3) | ((values[7] >> 8) & 0x7)) as u8;
    bytes[10] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_12(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 12);
    bytes[0] = ((values[0] >> 4) & 0xff) as u8;
    bytes[1] = ((((values[0]) & 0xf) << 4) | ((values[1] >> 8) & 0xf)) as u8;
    bytes[2] = ((values[1]) & 0xff) as u8;
    bytes[3] = ((values[2] >> 4) & 0xff) as u8;
    bytes[4] = ((((values[2]) & 0xf) << 4) | ((values[3] >> 8) & 0xf)) as u8;
    bytes[5] = ((values[3]) & 0xff) as u8;
    bytes[6] = ((values[4] >> 4) & 0xff) as u8;
    bytes[7] = ((((values[4]) & 0xf) << 4) | ((values[5] >> 8) & 0xf)) as u8;
    bytes[8] = ((values[5]) & 0xff) as u8;
    bytes[9] = ((values[6] >> 4) & 0xff) as u8;
    bytes[10] = ((((values[6]) & 0xf) << 4) | ((values[7] >> 8) & 0xf)) as u8;
    bytes[11] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_13(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 13);
    bytes[0] = ((values[0] >> 5) & 0xff) as u8;
    bytes[1] = ((((values[0]) & 0x1f) << 3) | ((values[1] >> 10) & 0x7)) as u8;
    bytes[2] = ((values[1] >> 2) & 0xff) as u8;
    bytes[3] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 7) & 0x3f)) as u8;
    bytes[4] = ((((values[2]) & 0x7f) << 1) | ((values[3] >> 12) & 0x1)) as u8;
    bytes[5] = ((values[3] >> 4) & 0xff) as u8;
    bytes[6] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 9) & 0xf)) as u8;
    bytes[7] = ((values[4] >> 1) & 0xff) as u8;
    bytes[8] = ((((values[4]) & 0x1) << 7) | ((values[5] >> 6) & 0x7f)) as u8;
    bytes[9] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 11) & 0x3)) as u8;
    bytes[10] = ((values[6] >> 3) & 0xff) as u8;
    bytes[11] = ((((values[6]) & 0x7) << 5) | ((values[7] >> 8) & 0x1f)) as u8;
    bytes[12] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_14(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 14);
    bytes[0] = ((values[0] >> 6) & 0xff) as u8;
    bytes[1] = ((((values[0]) & 0x3f) << 2) | ((values[1] >> 12) & 0x3)) as u8;
    bytes[2] = ((values[1] >> 4) & 0xff) as u8;
    bytes[3] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 10) & 0xf)) as u8;
    bytes[4] = ((values[2] >> 2) & 0xff) as u8;
    bytes[5] = ((((values[2]) & 0x3) << 6) | ((values[3] >> 8) & 0x3f)) as u8;
    bytes[6] = ((values[3]) & 0xff) as u8;
    bytes[7] = ((values[4] >> 6) & 0xff) as u8;
    bytes[8] = ((((values[4]) & 0x3f) << 2) | ((values[5] >> 12) & 0x3)) as u8;
    bytes[9] = ((values[5] >> 4) & 0xff) as u8;
    bytes[10] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 10) & 0xf)) as u8;
    bytes[11] = ((values[6] >> 2) & 0xff) as u8;
    bytes[12] = ((((values[6]) & 0x3) << 6) | ((values[7] >> 8) & 0x3f)) as u8;
    bytes[13] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_15(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 15);
    bytes[0] = ((values[0] >> 7) & 0xff) as u8;
    bytes[1] = ((((values[0]) & 0x7f) << 1) | ((values[1] >> 14) & 0x1)) as u8;
    bytes[2] = ((values[1] >> 6) & 0xff) as u8;
    bytes[3] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 13) & 0x3)) as u8;
    bytes[4] = ((values[2] >> 5) & 0xff) as u8;
    bytes[5] = ((((values[2]) & 0x1f) << 3) | ((values[3] >> 12) & 0x7)) as u8;
    bytes[6] = ((values[3] >> 4) & 0xff) as u8;
    bytes[7] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 11) & 0xf)) as u8;
    bytes[8] = ((values[4] >> 3) & 0xff) as u8;
    bytes[9] = ((((values[4]) & 0x7) << 5) | ((values[5] >> 10) & 0x1f)) as u8;
    bytes[10] = ((values[5] >> 2) & 0xff) as u8;
    bytes[11] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 9) & 0x3f)) as u8;
    bytes[12] = ((values[6] >> 1) & 0xff) as u8;
    bytes[13] = ((((values[6]) & 0x1) << 7) | ((values[7] >> 8) & 0x7f)) as u8;
    bytes[14] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_16(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 16);
    bytes[0] = ((values[0] >> 8) & 0xff) as u8;
    bytes[1] = ((values[0]) & 0xff) as u8;
    bytes[2] = ((values[1] >> 8) & 0xff) as u8;
    bytes[3] = ((values[1]) & 0xff) as u8;
    bytes[4] = ((values[2] >> 8) & 0xff) as u8;
    bytes[5] = ((values[2]) & 0xff) as u8;
    bytes[6] = ((values[3] >> 8) & 0xff) as u8;
    bytes[7] = ((values[3]) & 0xff) as u8;
    bytes[8] = ((values[4] >> 8) & 0xff) as u8;
    bytes[9] = ((values[4]) & 0xff) as u8;
    bytes[10] = ((values[5] >> 8) & 0xff) as u8;
    bytes[11] = ((values[5]) & 0xff) as u8;
    bytes[12] = ((values[6] >> 8) & 0xff) as u8;
    bytes[13] = ((values[6]) & 0xff) as u8;
    bytes[14] = ((values[7] >> 8) & 0xff) as u8;
    bytes[15] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_17(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 17);
    bytes[0] = ((values[0] >> 9) & 0xff) as u8;
    bytes[1] = ((values[0] >> 1) & 0xff) as u8;
    bytes[2] = ((((values[0]) & 0x1) << 7) | ((values[1] >> 10) & 0x7f)) as u8;
    bytes[3] = ((values[1] >> 2) & 0xff) as u8;
    bytes[4] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 11) & 0x3f)) as u8;
    bytes[5] = ((values[2] >> 3) & 0xff) as u8;
    bytes[6] = ((((values[2]) & 0x7) << 5) | ((values[3] >> 12) & 0x1f)) as u8;
    bytes[7] = ((values[3] >> 4) & 0xff) as u8;
    bytes[8] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 13) & 0xf)) as u8;
    bytes[9] = ((values[4] >> 5) & 0xff) as u8;
    bytes[10] = ((((values[4]) & 0x1f) << 3) | ((values[5] >> 14) & 0x7)) as u8;
    bytes[11] = ((values[5] >> 6) & 0xff) as u8;
    bytes[12] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 15) & 0x3)) as u8;
    bytes[13] = ((values[6] >> 7) & 0xff) as u8;
    bytes[14] = ((((values[6]) & 0x7f) << 1) | ((values[7] >> 16) & 0x1)) as u8;
    bytes[15] = ((values[7] >> 8) & 0xff) as u8;
    bytes[16] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_18(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 18);
    bytes[0] = ((values[0] >> 10) & 0xff) as u8;
    bytes[1] = ((values[0] >> 2) & 0xff) as u8;
    bytes[2] = ((((values[0]) & 0x3) << 6) | ((values[1] >> 12) & 0x3f)) as u8;
    bytes[3] = ((values[1] >> 4) & 0xff) as u8;
    bytes[4] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 14) & 0xf)) as u8;
    bytes[5] = ((values[2] >> 6) & 0xff) as u8;
    bytes[6] = ((((values[2]) & 0x3f) << 2) | ((values[3] >> 16) & 0x3)) as u8;
    bytes[7] = ((values[3] >> 8) & 0xff) as u8;
    bytes[8] = ((values[3]) & 0xff) as u8;
    bytes[9] = ((values[4] >> 10) & 0xff) as u8;
    bytes[10] = ((values[4] >> 2) & 0xff) as u8;
    bytes[11] = ((((values[4]) & 0x3) << 6) | ((values[5] >> 12) & 0x3f)) as u8;
    bytes[12] = ((values[5] >> 4) & 0xff) as u8;
    bytes[13] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 14) & 0xf)) as u8;
    bytes[14] = ((values[6] >> 6) & 0xff) as u8;
    bytes[15] = ((((values[6]) & 0x3f) << 2) | ((values[7] >> 16) & 0x3)) as u8;
    bytes[16] = ((values[7] >> 8) & 0xff) as u8;
    bytes[17] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_19(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 19);
    bytes[0] = ((values[0] >> 11) & 0xff) as u8;
    bytes[1] = ((values[0] >> 3) & 0xff) as u8;
    bytes[2] = ((((values[0]) & 0x7) << 5) | ((values[1] >> 14) & 0x1f)) as u8;
    bytes[3] = ((values[1] >> 6) & 0xff) as u8;
    bytes[4] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 17) & 0x3)) as u8;
    bytes[5] = ((values[2] >> 9) & 0xff) as u8;
    bytes[6] = ((values[2] >> 1) & 0xff) as u8;
    bytes[7] = ((((values[2]) & 0x1) << 7) | ((values[3] >> 12) & 0x7f)) as u8;
    bytes[8] = ((values[3] >> 4) & 0xff) as u8;
    bytes[9] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 15) & 0xf)) as u8;
    bytes[10] = ((values[4] >> 7) & 0xff) as u8;
    bytes[11] = ((((values[4]) & 0x7f) << 1) | ((values[5] >> 18) & 0x1)) as u8;
    bytes[12] = ((values[5] >> 10) & 0xff) as u8;
    bytes[13] = ((values[5] >> 2) & 0xff) as u8;
    bytes[14] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 13) & 0x3f)) as u8;
    bytes[15] = ((values[6] >> 5) & 0xff) as u8;
    bytes[16] = ((((values[6]) & 0x1f) << 3) | ((values[7] >> 16) & 0x7)) as u8;
    bytes[17] = ((values[7] >> 8) & 0xff) as u8;
    bytes[18] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_20(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 20);
    bytes[0] = ((values[0] >> 12) & 0xff) as u8;
    bytes[1] = ((values[0] >> 4) & 0xff) as u8;
    bytes[2] = ((((values[0]) & 0xf) << 4) | ((values[1] >> 16) & 0xf)) as u8;
    bytes[3] = ((values[1] >> 8) & 0xff) as u8;
    bytes[4] = ((values[1]) & 0xff) as u8;
    bytes[5] = ((values[2] >> 12) & 0xff) as u8;
    bytes[6] = ((values[2] >> 4) & 0xff) as u8;
    bytes[7] = ((((values[2]) & 0xf) << 4) | ((values[3] >> 16) & 0xf)) as u8;
    bytes[8] = ((values[3] >> 8) & 0xff) as u8;
    bytes[9] = ((values[3]) & 0xff) as u8;
    bytes[10] = ((values[4] >> 12) & 0xff) as u8;
    bytes[11] = ((values[4] >> 4) & 0xff) as u8;
    bytes[12] = ((((values[4]) & 0xf) << 4) | ((values[5] >> 16) & 0xf)) as u8;
    bytes[13] = ((values[5] >> 8) & 0xff) as u8;
    bytes[14] = ((values[5]) & 0xff) as u8;
    bytes[15] = ((values[6] >> 12) & 0xff) as u8;
    bytes[16] = ((values[6] >> 4) & 0xff) as u8;
    bytes[17] = ((((values[6]) & 0xf) << 4) | ((values[7] >> 16) & 0xf)) as u8;
    bytes[18] = ((values[7] >> 8) & 0xff) as u8;
    bytes[19] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_21(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 21);
    bytes[0] = ((values[0] >> 13) & 0xff) as u8;
    bytes[1] = ((values[0] >> 5) & 0xff) as u8;
    bytes[2] = ((((values[0]) & 0x1f) << 3) | ((values[1] >> 18) & 0x7)) as u8;
    bytes[3] = ((values[1] >> 10) & 0xff) as u8;
    bytes[4] = ((values[1] >> 2) & 0xff) as u8;
    bytes[5] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 15) & 0x3f)) as u8;
    bytes[6] = ((values[2] >> 7) & 0xff) as u8;
    bytes[7] = ((((values[2]) & 0x7f) << 1) | ((values[3] >> 20) & 0x1)) as u8;
    bytes[8] = ((values[3] >> 12) & 0xff) as u8;
    bytes[9] = ((values[3] >> 4) & 0xff) as u8;
    bytes[10] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 17) & 0xf)) as u8;
    bytes[11] = ((values[4] >> 9) & 0xff) as u8;
    bytes[12] = ((values[4] >> 1) & 0xff) as u8;
    bytes[13] = ((((values[4]) & 0x1) << 7) | ((values[5] >> 14) & 0x7f)) as u8;
    bytes[14] = ((values[5] >> 6) & 0xff) as u8;
    bytes[15] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 19) & 0x3)) as u8;
    bytes[16] = ((values[6] >> 11) & 0xff) as u8;
    bytes[17] = ((values[6] >> 3) & 0xff) as u8;
    bytes[18] = ((((values[6]) & 0x7) << 5) | ((values[7] >> 16) & 0x1f)) as u8;
    bytes[19] = ((values[7] >> 8) & 0xff) as u8;
    bytes[20] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_22(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 22);
    bytes[0] = ((values[0] >> 14) & 0xff) as u8;
    bytes[1] = ((values[0] >> 6) & 0xff) as u8;
    bytes[2] = ((((values[0]) & 0x3f) << 2) | ((values[1] >> 20) & 0x3)) as u8;
    bytes[3] = ((values[1] >> 12) & 0xff) as u8;
    bytes[4] = ((values[1] >> 4) & 0xff) as u8;
    bytes[5] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 18) & 0xf)) as u8;
    bytes[6] = ((values[2] >> 10) & 0xff) as u8;
    bytes[7] = ((values[2] >> 2) & 0xff) as u8;
    bytes[8] = ((((values[2]) & 0x3) << 6) | ((values[3] >> 16) & 0x3f)) as u8;
    bytes[9] = ((values[3] >> 8) & 0xff) as u8;
    bytes[10] = ((values[3]) & 0xff) as u8;
    bytes[11] = ((values[4] >> 14) & 0xff) as u8;
    bytes[12] = ((values[4] >> 6) & 0xff) as u8;
    bytes[13] = ((((values[4]) & 0x3f) << 2) | ((values[5] >> 20) & 0x3)) as u8;
    bytes[14] = ((values[5] >> 12) & 0xff) as u8;
    bytes[15] = ((values[5] >> 4) & 0xff) as u8;
    bytes[16] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 18) & 0xf)) as u8;
    bytes[17] = ((values[6] >> 10) & 0xff) as u8;
    bytes[18] = ((values[6] >> 2) & 0xff) as u8;
    bytes[19] = ((((values[6]) & 0x3) << 6) | ((values[7] >> 16) & 0x3f)) as u8;
    bytes[20] = ((values[7] >> 8) & 0xff) as u8;
    bytes[21] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_23(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 23);
    bytes[0] = ((values[0] >> 15) & 0xff) as u8;
    bytes[1] = ((values[0] >> 7) & 0xff) as u8;
    bytes[2] = ((((values[0]) & 0x7f) << 1) | ((values[1] >> 22) & 0x1)) as u8;
    bytes[3] = ((values[1] >> 14) & 0xff) as u8;
    bytes[4] = ((values[1] >> 6) & 0xff) as u8;
    bytes[5] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 21) & 0x3)) as u8;
    bytes[6] = ((values[2] >> 13) & 0xff) as u8;
    bytes[7] = ((values[2] >> 5) & 0xff) as u8;
    bytes[8] = ((((values[2]) & 0x1f) << 3) | ((values[3] >> 20) & 0x7)) as u8;
    bytes[9] = ((values[3] >> 12) & 0xff) as u8;
    bytes[10] = ((values[3] >> 4) & 0xff) as u8;
    bytes[11] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 19) & 0xf)) as u8;
    bytes[12] = ((values[4] >> 11) & 0xff) as u8;
    bytes[13] = ((values[4] >> 3) & 0xff) as u8;
    bytes[14] = ((((values[4]) & 0x7) << 5) | ((values[5] >> 18) & 0x1f)) as u8;
    bytes[15] = ((values[5] >> 10) & 0xff) as u8;
    bytes[16] = ((values[5] >> 2) & 0xff) as u8;
    bytes[17] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 17) & 0x3f)) as u8;
    bytes[18] = ((values[6] >> 9) & 0xff) as u8;
    bytes[19] = ((values[6] >> 1) & 0xff) as u8;
    bytes[20] = ((((values[6]) & 0x1) << 7) | ((values[7] >> 16) & 0x7f)) as u8;
    bytes[21] = ((values[7] >> 8) & 0xff) as u8;
    bytes[22] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_24(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 24);
    bytes[0] = ((values[0] >> 16) & 0xff) as u8;
    bytes[1] = ((values[0] >> 8) & 0xff) as u8;
    bytes[2] = ((values[0]) & 0xff) as u8;
    bytes[3] = ((values[1] >> 16) & 0xff) as u8;
    bytes[4] = ((values[1] >> 8) & 0xff) as u8;
    bytes[5] = ((values[1]) & 0xff) as u8;
    bytes[6] = ((values[2] >> 16) & 0xff) as u8;
    bytes[7] = ((values[2] >> 8) & 0xff) as u8;
    bytes[8] = ((values[2]) & 0xff) as u8;
    bytes[9] = ((values[3] >> 16) & 0xff) as u8;
    bytes[10] = ((values[3] >> 8) & 0xff) as u8;
    bytes[11] = ((values[3]) & 0xff) as u8;
    bytes[12] = ((values[4] >> 16) & 0xff) as u8;
    bytes[13] = ((values[4] >> 8) & 0xff) as u8;
    bytes[14] = ((values[4]) & 0xff) as u8;
    bytes[15] = ((values[5] >> 16) & 0xff) as u8;
    bytes[16] = ((values[5] >> 8) & 0xff) as u8;
    bytes[17] = ((values[5]) & 0xff) as u8;
    bytes[18] = ((values[6] >> 16) & 0xff) as u8;
    bytes[19] = ((values[6] >> 8) & 0xff) as u8;
    bytes[20] = ((values[6]) & 0xff) as u8;
    bytes[21] = ((values[7] >> 16) & 0xff) as u8;
    bytes[22] = ((values[7] >> 8) & 0xff) as u8;
    bytes[23] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_25(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 25);
    bytes[0] = ((values[0] >> 17) & 0xff) as u8;
    bytes[1] = ((values[0] >> 9) & 0xff) as u8;
    bytes[2] = ((values[0] >> 1) & 0xff) as u8;
    bytes[3] = ((((values[0]) & 0x1) << 7) | ((values[1] >> 18) & 0x7f)) as u8;
    bytes[4] = ((values[1] >> 10) & 0xff) as u8;
    bytes[5] = ((values[1] >> 2) & 0xff) as u8;
    bytes[6] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 19) & 0x3f)) as u8;
    bytes[7] = ((values[2] >> 11) & 0xff) as u8;
    bytes[8] = ((values[2] >> 3) & 0xff) as u8;
    bytes[9] = ((((values[2]) & 0x7) << 5) | ((values[3] >> 20) & 0x1f)) as u8;
    bytes[10] = ((values[3] >> 12) & 0xff) as u8;
    bytes[11] = ((values[3] >> 4) & 0xff) as u8;
    bytes[12] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 21) & 0xf)) as u8;
    bytes[13] = ((values[4] >> 13) & 0xff) as u8;
    bytes[14] = ((values[4] >> 5) & 0xff) as u8;
    bytes[15] = ((((values[4]) & 0x1f) << 3) | ((values[5] >> 22) & 0x7)) as u8;
    bytes[16] = ((values[5] >> 14) & 0xff) as u8;
    bytes[17] = ((values[5] >> 6) & 0xff) as u8;
    bytes[18] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 23) & 0x3)) as u8;
    bytes[19] = ((values[6] >> 15) & 0xff) as u8;
    bytes[20] = ((values[6] >> 7) & 0xff) as u8;
    bytes[21] = ((((values[6]) & 0x7f) << 1) | ((values[7] >> 24) & 0x1)) as u8;
    bytes[22] = ((values[7] >> 16) & 0xff) as u8;
    bytes[23] = ((values[7] >> 8) & 0xff) as u8;
    bytes[24] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_26(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 26);
    bytes[0] = ((values[0] >> 18) & 0xff) as u8;
    bytes[1] = ((values[0] >> 10) & 0xff) as u8;
    bytes[2] = ((values[0] >> 2) & 0xff) as u8;
    bytes[3] = ((((values[0]) & 0x3) << 6) | ((values[1] >> 20) & 0x3f)) as u8;
    bytes[4] = ((values[1] >> 12) & 0xff) as u8;
    bytes[5] = ((values[1] >> 4) & 0xff) as u8;
    bytes[6] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 22) & 0xf)) as u8;
    bytes[7] = ((values[2] >> 14) & 0xff) as u8;
    bytes[8] = ((values[2] >> 6) & 0xff) as u8;
    bytes[9] = ((((values[2]) & 0x3f) << 2) | ((values[3] >> 24) & 0x3)) as u8;
    bytes[10] = ((values[3] >> 16) & 0xff) as u8;
    bytes[11] = ((values[3] >> 8) & 0xff) as u8;
    bytes[12] = ((values[3]) & 0xff) as u8;
    bytes[13] = ((values[4] >> 18) & 0xff) as u8;
    bytes[14] = ((values[4] >> 10) & 0xff) as u8;
    bytes[15] = ((values[4] >> 2) & 0xff) as u8;
    bytes[16] = ((((values[4]) & 0x3) << 6) | ((values[5] >> 20) & 0x3f)) as u8;
    bytes[17] = ((values[5] >> 12) & 0xff) as u8;
    bytes[18] = ((values[5] >> 4) & 0xff) as u8;
    bytes[19] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 22) & 0xf)) as u8;
    bytes[20] = ((values[6] >> 14) & 0xff) as u8;
    bytes[21] = ((values[6] >> 6) & 0xff) as u8;
    bytes[22] = ((((values[6]) & 0x3f) << 2) | ((values[7] >> 24) & 0x3)) as u8;
    bytes[23] = ((values[7] >> 16) & 0xff) as u8;
    bytes[24] = ((values[7] >> 8) & 0xff) as u8;
    bytes[25] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_27(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 27);
    bytes[0] = ((values[0] >> 19) & 0xff) as u8;
    bytes[1] = ((values[0] >> 11) & 0xff) as u8;
    bytes[2] = ((values[0] >> 3) & 0xff) as u8;
    bytes[3] = ((((values[0]) & 0x7) << 5) | ((values[1] >> 22) & 0x1f)) as u8;
    bytes[4] = ((values[1] >> 14) & 0xff) as u8;
    bytes[5] = ((values[1] >> 6) & 0xff) as u8;
    bytes[6] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 25) & 0x3)) as u8;
    bytes[7] = ((values[2] >> 17) & 0xff) as u8;
    bytes[8] = ((values[2] >> 9) & 0xff) as u8;
    bytes[9] = ((values[2] >> 1) & 0xff) as u8;
    bytes[10] = ((((values[2]) & 0x1) << 7) | ((values[3] >> 20) & 0x7f)) as u8;
    bytes[11] = ((values[3] >> 12) & 0xff) as u8;
    bytes[12] = ((values[3] >> 4) & 0xff) as u8;
    bytes[13] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 23) & 0xf)) as u8;
    bytes[14] = ((values[4] >> 15) & 0xff) as u8;
    bytes[15] = ((values[4] >> 7) & 0xff) as u8;
    bytes[16] = ((((values[4]) & 0x7f) << 1) | ((values[5] >> 26) & 0x1)) as u8;
    bytes[17] = ((values[5] >> 18) & 0xff) as u8;
    bytes[18] = ((values[5] >> 10) & 0xff) as u8;
    bytes[19] = ((values[5] >> 2) & 0xff) as u8;
    bytes[20] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 21) & 0x3f)) as u8;
    bytes[21] = ((values[6] >> 13) & 0xff) as u8;
    bytes[22] = ((values[6] >> 5) & 0xff) as u8;
    bytes[23] = ((((values[6]) & 0x1f) << 3) | ((values[7] >> 24) & 0x7)) as u8;
    bytes[24] = ((values[7] >> 16) & 0xff) as u8;
    bytes[25] = ((values[7] >> 8) & 0xff) as u8;
    bytes[26] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_28(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 28);
    bytes[0] = ((values[0] >> 20) & 0xff) as u8;
    bytes[1] = ((values[0] >> 12) & 0xff) as u8;
    bytes[2] = ((values[0] >> 4) & 0xff) as u8;
    bytes[3] = ((((values[0]) & 0xf) << 4) | ((values[1] >> 24) & 0xf)) as u8;
    bytes[4] = ((values[1] >> 16) & 0xff) as u8;
    bytes[5] = ((values[1] >> 8) & 0xff) as u8;
    bytes[6] = ((values[1]) & 0xff) as u8;
    bytes[7] = ((values[2] >> 20) & 0xff) as u8;
    bytes[8] = ((values[2] >> 12) & 0xff) as u8;
    bytes[9] = ((values[2] >> 4) & 0xff) as u8;
    bytes[10] = ((((values[2]) & 0xf) << 4) | ((values[3] >> 24) & 0xf)) as u8;
    bytes[11] = ((values[3] >> 16) & 0xff) as u8;
    bytes[12] = ((values[3] >> 8) & 0xff) as u8;
    bytes[13] = ((values[3]) & 0xff) as u8;
    bytes[14] = ((values[4] >> 20) & 0xff) as u8;
    bytes[15] = ((values[4] >> 12) & 0xff) as u8;
    bytes[16] = ((values[4] >> 4) & 0xff) as u8;
    bytes[17] = ((((values[4]) & 0xf) << 4) | ((values[5] >> 24) & 0xf)) as u8;
    bytes[18] = ((values[5] >> 16) & 0xff) as u8;
    bytes[19] = ((values[5] >> 8) & 0xff) as u8;
    bytes[20] = ((values[5]) & 0xff) as u8;
    bytes[21] = ((values[6] >> 20) & 0xff) as u8;
    bytes[22] = ((values[6] >> 12) & 0xff) as u8;
    bytes[23] = ((values[6] >> 4) & 0xff) as u8;
    bytes[24] = ((((values[6]) & 0xf) << 4) | ((values[7] >> 24) & 0xf)) as u8;
    bytes[25] = ((values[7] >> 16) & 0xff) as u8;
    bytes[26] = ((values[7] >> 8) & 0xff) as u8;
    bytes[27] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_29(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 29);
    bytes[0] = ((values[0] >> 21) & 0xff) as u8;
    bytes[1] = ((values[0] >> 13) & 0xff) as u8;
    bytes[2] = ((values[0] >> 5) & 0xff) as u8;
    bytes[3] = ((((values[0]) & 0x1f) << 3) | ((values[1] >> 26) & 0x7)) as u8;
    bytes[4] = ((values[1] >> 18) & 0xff) as u8;
    bytes[5] = ((values[1] >> 10) & 0xff) as u8;
    bytes[6] = ((values[1] >> 2) & 0xff) as u8;
    bytes[7] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 23) & 0x3f)) as u8;
    bytes[8] = ((values[2] >> 15) & 0xff) as u8;
    bytes[9] = ((values[2] >> 7) & 0xff) as u8;
    bytes[10] = ((((values[2]) & 0x7f) << 1) | ((values[3] >> 28) & 0x1)) as u8;
    bytes[11] = ((values[3] >> 20) & 0xff) as u8;
    bytes[12] = ((values[3] >> 12) & 0xff) as u8;
    bytes[13] = ((values[3] >> 4) & 0xff) as u8;
    bytes[14] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 25) & 0xf)) as u8;
    bytes[15] = ((values[4] >> 17) & 0xff) as u8;
    bytes[16] = ((values[4] >> 9) & 0xff) as u8;
    bytes[17] = ((values[4] >> 1) & 0xff) as u8;
    bytes[18] = ((((values[4]) & 0x1) << 7) | ((values[5] >> 22) & 0x7f)) as u8;
    bytes[19] = ((values[5] >> 14) & 0xff) as u8;
    bytes[20] = ((values[5] >> 6) & 0xff) as u8;
    bytes[21] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 27) & 0x3)) as u8;
    bytes[22] = ((values[6] >> 19) & 0xff) as u8;
    bytes[23] = ((values[6] >> 11) & 0xff) as u8;
    bytes[24] = ((values[6] >> 3) & 0xff) as u8;
    bytes[25] = ((((values[6]) & 0x7) << 5) | ((values[7] >> 24) & 0x1f)) as u8;
    bytes[26] = ((values[7] >> 16) & 0xff) as u8;
    bytes[27] = ((values[7] >> 8) & 0xff) as u8;
    bytes[28] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_30(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 30);
    bytes[0] = ((values[0] >> 22) & 0xff) as u8;
    bytes[1] = ((values[0] >> 14) & 0xff) as u8;
    bytes[2] = ((values[0] >> 6) & 0xff) as u8;
    bytes[3] = ((((values[0]) & 0x3f) << 2) | ((values[1] >> 28) & 0x3)) as u8;
    bytes[4] = ((values[1] >> 20) & 0xff) as u8;
    bytes[5] = ((values[1] >> 12) & 0xff) as u8;
    bytes[6] = ((values[1] >> 4) & 0xff) as u8;
    bytes[7] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 26) & 0xf)) as u8;
    bytes[8] = ((values[2] >> 18) & 0xff) as u8;
    bytes[9] = ((values[2] >> 10) & 0xff) as u8;
    bytes[10] = ((values[2] >> 2) & 0xff) as u8;
    bytes[11] = ((((values[2]) & 0x3) << 6) | ((values[3] >> 24) & 0x3f)) as u8;
    bytes[12] = ((values[3] >> 16) & 0xff) as u8;
    bytes[13] = ((values[3] >> 8) & 0xff) as u8;
    bytes[14] = ((values[3]) & 0xff) as u8;
    bytes[15] = ((values[4] >> 22) & 0xff) as u8;
    bytes[16] = ((values[4] >> 14) & 0xff) as u8;
    bytes[17] = ((values[4] >> 6) & 0xff) as u8;
    bytes[18] = ((((values[4]) & 0x3f) << 2) | ((values[5] >> 28) & 0x3)) as u8;
    bytes[19] = ((values[5] >> 20) & 0xff) as u8;
    bytes[20] = ((values[5] >> 12) & 0xff) as u8;
    bytes[21] = ((values[5] >> 4) & 0xff) as u8;
    bytes[22] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 26) & 0xf)) as u8;
    bytes[23] = ((values[6] >> 18) & 0xff) as u8;
    bytes[24] = ((values[6] >> 10) & 0xff) as u8;
    bytes[25] = ((values[6] >> 2) & 0xff) as u8;
    bytes[26] = ((((values[6]) & 0x3) << 6) | ((values[7] >> 24) & 0x3f)) as u8;
    bytes[27] = ((values[7] >> 16) & 0xff) as u8;
    bytes[28] = ((values[7] >> 8) & 0xff) as u8;
    bytes[29] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_31(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 31);
    bytes[0] = ((values[0] >> 23) & 0xff) as u8;
    bytes[1] = ((values[0] >> 15) & 0xff) as u8;
    bytes[2] = ((values[0] >> 7) & 0xff) as u8;
    bytes[3] = ((((values[0]) & 0x7f) << 1) | ((values[1] >> 30) & 0x1)) as u8;
    bytes[4] = ((values[1] >> 22) & 0xff) as u8;
    bytes[5] = ((values[1] >> 14) & 0xff) as u8;
    bytes[6] = ((values[1] >> 6) & 0xff) as u8;
    bytes[7] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 29) & 0x3)) as u8;
    bytes[8] = ((values[2] >> 21) & 0xff) as u8;
    bytes[9] = ((values[2] >> 13) & 0xff) as u8;
    bytes[10] = ((values[2] >> 5) & 0xff) as u8;
    bytes[11] = ((((values[2]) & 0x1f) << 3) | ((values[3] >> 28) & 0x7)) as u8;
    bytes[12] = ((values[3] >> 20) & 0xff) as u8;
    bytes[13] = ((values[3] >> 12) & 0xff) as u8;
    bytes[14] = ((values[3] >> 4) & 0xff) as u8;
    bytes[15] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 27) & 0xf)) as u8;
    bytes[16] = ((values[4] >> 19) & 0xff) as u8;
    bytes[17] = ((values[4] >> 11) & 0xff) as u8;
    bytes[18] = ((values[4] >> 3) & 0xff) as u8;
    bytes[19] = ((((values[4]) & 0x7) << 5) | ((values[5] >> 26) & 0x1f)) as u8;
    bytes[20] = ((values[5] >> 18) & 0xff) as u8;
    bytes[21] = ((values[5] >> 10) & 0xff) as u8;
    bytes[22] = ((values[5] >> 2) & 0xff) as u8;
    bytes[23] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 25) & 0x3f)) as u8;
    bytes[24] = ((values[6] >> 17) & 0xff) as u8;
    bytes[25] = ((values[6] >> 9) & 0xff) as u8;
    bytes[26] = ((values[6] >> 1) & 0xff) as u8;
    bytes[27] = ((((values[6]) & 0x1) << 7) | ((values[7] >> 24) & 0x7f)) as u8;
    bytes[28] = ((values[7] >> 16) & 0xff) as u8;
    bytes[29] = ((values[7] >> 8) & 0xff) as u8;
    bytes[30] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_32(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 32);
    bytes[0] = ((values[0] >> 24) & 0xff) as u8;
    bytes[1] = ((values[0] >> 16) & 0xff) as u8;
    bytes[2] = ((values[0] >> 8) & 0xff) as u8;
    bytes[3] = ((values[0]) & 0xff) as u8;
    bytes[4] = ((values[1] >> 24) & 0xff) as u8;
    bytes[5] = ((values[1] >> 16) & 0xff) as u8;
    bytes[6] = ((values[1] >> 8) & 0xff) as u8;
    bytes[7] = ((values[1]) & 0xff) as u8;
    bytes[8] = ((values[2] >> 24) & 0xff) as u8;
    bytes[9] = ((values[2] >> 16) & 0xff) as u8;
    bytes[10] = ((values[2] >> 8) & 0xff) as u8;
    bytes[11] = ((values[2]) & 0xff) as u8;
    bytes[12] = ((values[3] >> 24) & 0xff) as u8;
    bytes[13] = ((values[3] >> 16) & 0xff) as u8;
    bytes[14] = ((values[3] >> 8) & 0xff) as u8;
    bytes[15] = ((values[3]) & 0xff) as u8;
    bytes[16] = ((values[4] >> 24) & 0xff) as u8;
    bytes[17] = ((values[4] >> 16) & 0xff) as u8;
    bytes[18] = ((values[4] >> 8) & 0xff) as u8;
    bytes[19] = ((values[4]) & 0xff) as u8;
    bytes[20] = ((values[5] >> 24) & 0xff) as u8;
    bytes[21] = ((values[5] >> 16) & 0xff) as u8;
    bytes[22] = ((values[5] >> 8) & 0xff) as u8;
    bytes[23] = ((values[5]) & 0xff) as u8;
    bytes[24] = ((values[6] >> 24) & 0xff) as u8;
    bytes[25] = ((values[6] >> 16) & 0xff) as u8;
    bytes[26] = ((values[6] >> 8) & 0xff) as u8;
    bytes[27] = ((values[6]) & 0xff) as u8;
    bytes[28] = ((values[7] >> 24) & 0xff) as u8;
    bytes[29] = ((values[7] >> 16) & 0xff) as u8;
    bytes[30] = ((values[7] >> 8) & 0xff) as u8;
    bytes[31] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_33(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 33);
    bytes[0] = ((values[0] >> 25) & 0xff) as u8;
    bytes[1] = ((values[0] >> 17) & 0xff) as u8;
    bytes[2] = ((values[0] >> 9) & 0xff) as u8;
    bytes[3] = ((values[0] >> 1) & 0xff) as u8;
    bytes[4] = ((((values[0]) & 0x1) << 7) | ((values[1] >> 26) & 0x7f)) as u8;
    bytes[5] = ((values[1] >> 18) & 0xff) as u8;
    bytes[6] = ((values[1] >> 10) & 0xff) as u8;
    bytes[7] = ((values[1] >> 2) & 0xff) as u8;
    bytes[8] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 27) & 0x3f)) as u8;
    bytes[9] = ((values[2] >> 19) & 0xff) as u8;
    bytes[10] = ((values[2] >> 11) & 0xff) as u8;
    bytes[11] = ((values[2] >> 3) & 0xff) as u8;
    bytes[12] = ((((values[2]) & 0x7) << 5) | ((values[3] >> 28) & 0x1f)) as u8;
    bytes[13] = ((values[3] >> 20) & 0xff) as u8;
    bytes[14] = ((values[3] >> 12) & 0xff) as u8;
    bytes[15] = ((values[3] >> 4) & 0xff) as u8;
    bytes[16] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 29) & 0xf)) as u8;
    bytes[17] = ((values[4] >> 21) & 0xff) as u8;
    bytes[18] = ((values[4] >> 13) & 0xff) as u8;
    bytes[19] = ((values[4] >> 5) & 0xff) as u8;
    bytes[20] = ((((values[4]) & 0x1f) << 3) | ((values[5] >> 30) & 0x7)) as u8;
    bytes[21] = ((values[5] >> 22) & 0xff) as u8;
    bytes[22] = ((values[5] >> 14) & 0xff) as u8;
    bytes[23] = ((values[5] >> 6) & 0xff) as u8;
    bytes[24] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 31) & 0x3)) as u8;
    bytes[25] = ((values[6] >> 23) & 0xff) as u8;
    bytes[26] = ((values[6] >> 15) & 0xff) as u8;
    bytes[27] = ((values[6] >> 7) & 0xff) as u8;
    bytes[28] = ((((values[6]) & 0x7f) << 1) | ((values[7] >> 32) & 0x1)) as u8;
    bytes[29] = ((values[7] >> 24) & 0xff) as u8;
    bytes[30] = ((values[7] >> 16) & 0xff) as u8;
    bytes[31] = ((values[7] >> 8) & 0xff) as u8;
    bytes[32] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_34(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 34);
    bytes[0] = ((values[0] >> 26) & 0xff) as u8;
    bytes[1] = ((values[0] >> 18) & 0xff) as u8;
    bytes[2] = ((values[0] >> 10) & 0xff) as u8;
    bytes[3] = ((values[0] >> 2) & 0xff) as u8;
    bytes[4] = ((((values[0]) & 0x3) << 6) | ((values[1] >> 28) & 0x3f)) as u8;
    bytes[5] = ((values[1] >> 20) & 0xff) as u8;
    bytes[6] = ((values[1] >> 12) & 0xff) as u8;
    bytes[7] = ((values[1] >> 4) & 0xff) as u8;
    bytes[8] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 30) & 0xf)) as u8;
    bytes[9] = ((values[2] >> 22) & 0xff) as u8;
    bytes[10] = ((values[2] >> 14) & 0xff) as u8;
    bytes[11] = ((values[2] >> 6) & 0xff) as u8;
    bytes[12] = ((((values[2]) & 0x3f) << 2) | ((values[3] >> 32) & 0x3)) as u8;
    bytes[13] = ((values[3] >> 24) & 0xff) as u8;
    bytes[14] = ((values[3] >> 16) & 0xff) as u8;
    bytes[15] = ((values[3] >> 8) & 0xff) as u8;
    bytes[16] = ((values[3]) & 0xff) as u8;
    bytes[17] = ((values[4] >> 26) & 0xff) as u8;
    bytes[18] = ((values[4] >> 18) & 0xff) as u8;
    bytes[19] = ((values[4] >> 10) & 0xff) as u8;
    bytes[20] = ((values[4] >> 2) & 0xff) as u8;
    bytes[21] = ((((values[4]) & 0x3) << 6) | ((values[5] >> 28) & 0x3f)) as u8;
    bytes[22] = ((values[5] >> 20) & 0xff) as u8;
    bytes[23] = ((values[5] >> 12) & 0xff) as u8;
    bytes[24] = ((values[5] >> 4) & 0xff) as u8;
    bytes[25] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 30) & 0xf)) as u8;
    bytes[26] = ((values[6] >> 22) & 0xff) as u8;
    bytes[27] = ((values[6] >> 14) & 0xff) as u8;
    bytes[28] = ((values[6] >> 6) & 0xff) as u8;
    bytes[29] = ((((values[6]) & 0x3f) << 2) | ((values[7] >> 32) & 0x3)) as u8;
    bytes[30] = ((values[7] >> 24) & 0xff) as u8;
    bytes[31] = ((values[7] >> 16) & 0xff) as u8;
    bytes[32] = ((values[7] >> 8) & 0xff) as u8;
    bytes[33] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_35(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 35);
    bytes[0] = ((values[0] >> 27) & 0xff) as u8;
    bytes[1] = ((values[0] >> 19) & 0xff) as u8;
    bytes[2] = ((values[0] >> 11) & 0xff) as u8;
    bytes[3] = ((values[0] >> 3) & 0xff) as u8;
    bytes[4] = ((((values[0]) & 0x7) << 5) | ((values[1] >> 30) & 0x1f)) as u8;
    bytes[5] = ((values[1] >> 22) & 0xff) as u8;
    bytes[6] = ((values[1] >> 14) & 0xff) as u8;
    bytes[7] = ((values[1] >> 6) & 0xff) as u8;
    bytes[8] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 33) & 0x3)) as u8;
    bytes[9] = ((values[2] >> 25) & 0xff) as u8;
    bytes[10] = ((values[2] >> 17) & 0xff) as u8;
    bytes[11] = ((values[2] >> 9) & 0xff) as u8;
    bytes[12] = ((values[2] >> 1) & 0xff) as u8;
    bytes[13] = ((((values[2]) & 0x1) << 7) | ((values[3] >> 28) & 0x7f)) as u8;
    bytes[14] = ((values[3] >> 20) & 0xff) as u8;
    bytes[15] = ((values[3] >> 12) & 0xff) as u8;
    bytes[16] = ((values[3] >> 4) & 0xff) as u8;
    bytes[17] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 31) & 0xf)) as u8;
    bytes[18] = ((values[4] >> 23) & 0xff) as u8;
    bytes[19] = ((values[4] >> 15) & 0xff) as u8;
    bytes[20] = ((values[4] >> 7) & 0xff) as u8;
    bytes[21] = ((((values[4]) & 0x7f) << 1) | ((values[5] >> 34) & 0x1)) as u8;
    bytes[22] = ((values[5] >> 26) & 0xff) as u8;
    bytes[23] = ((values[5] >> 18) & 0xff) as u8;
    bytes[24] = ((values[5] >> 10) & 0xff) as u8;
    bytes[25] = ((values[5] >> 2) & 0xff) as u8;
    bytes[26] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 29) & 0x3f)) as u8;
    bytes[27] = ((values[6] >> 21) & 0xff) as u8;
    bytes[28] = ((values[6] >> 13) & 0xff) as u8;
    bytes[29] = ((values[6] >> 5) & 0xff) as u8;
    bytes[30] = ((((values[6]) & 0x1f) << 3) | ((values[7] >> 32) & 0x7)) as u8;
    bytes[31] = ((values[7] >> 24) & 0xff) as u8;
    bytes[32] = ((values[7] >> 16) & 0xff) as u8;
    bytes[33] = ((values[7] >> 8) & 0xff) as u8;
    bytes[34] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_36(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 36);
    bytes[0] = ((values[0] >> 28) & 0xff) as u8;
    bytes[1] = ((values[0] >> 20) & 0xff) as u8;
    bytes[2] = ((values[0] >> 12) & 0xff) as u8;
    bytes[3] = ((values[0] >> 4) & 0xff) as u8;
    bytes[4] = ((((values[0]) & 0xf) << 4) | ((values[1] >> 32) & 0xf)) as u8;
    bytes[5] = ((values[1] >> 24) & 0xff) as u8;
    bytes[6] = ((values[1] >> 16) & 0xff) as u8;
    bytes[7] = ((values[1] >> 8) & 0xff) as u8;
    bytes[8] = ((values[1]) & 0xff) as u8;
    bytes[9] = ((values[2] >> 28) & 0xff) as u8;
    bytes[10] = ((values[2] >> 20) & 0xff) as u8;
    bytes[11] = ((values[2] >> 12) & 0xff) as u8;
    bytes[12] = ((values[2] >> 4) & 0xff) as u8;
    bytes[13] = ((((values[2]) & 0xf) << 4) | ((values[3] >> 32) & 0xf)) as u8;
    bytes[14] = ((values[3] >> 24) & 0xff) as u8;
    bytes[15] = ((values[3] >> 16) & 0xff) as u8;
    bytes[16] = ((values[3] >> 8) & 0xff) as u8;
    bytes[17] = ((values[3]) & 0xff) as u8;
    bytes[18] = ((values[4] >> 28) & 0xff) as u8;
    bytes[19] = ((values[4] >> 20) & 0xff) as u8;
    bytes[20] = ((values[4] >> 12) & 0xff) as u8;
    bytes[21] = ((values[4] >> 4) & 0xff) as u8;
    bytes[22] = ((((values[4]) & 0xf) << 4) | ((values[5] >> 32) & 0xf)) as u8;
    bytes[23] = ((values[5] >> 24) & 0xff) as u8;
    bytes[24] = ((values[5] >> 16) & 0xff) as u8;
    bytes[25] = ((values[5] >> 8) & 0xff) as u8;
    bytes[26] = ((values[5]) & 0xff) as u8;
    bytes[27] = ((values[6] >> 28) & 0xff) as u8;
    bytes[28] = ((values[6] >> 20) & 0xff) as u8;
    bytes[29] = ((values[6] >> 12) & 0xff) as u8;
    bytes[30] = ((values[6] >> 4) & 0xff) as u8;
    bytes[31] = ((((values[6]) & 0xf) << 4) | ((values[7] >> 32) & 0xf)) as u8;
    bytes[32] = ((values[7] >> 24) & 0xff) as u8;
    bytes[33] = ((values[7] >> 16) & 0xff) as u8;
    bytes[34] = ((values[7] >> 8) & 0xff) as u8;
    bytes[35] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_37(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 37);
    bytes[0] = ((values[0] >> 29) & 0xff) as u8;
    bytes[1] = ((values[0] >> 21) & 0xff) as u8;
    bytes[2] = ((values[0] >> 13) & 0xff) as u8;
    bytes[3] = ((values[0] >> 5) & 0xff) as u8;
    bytes[4] = ((((values[0]) & 0x1f) << 3) | ((values[1] >> 34) & 0x7)) as u8;
    bytes[5] = ((values[1] >> 26) & 0xff) as u8;
    bytes[6] = ((values[1] >> 18) & 0xff) as u8;
    bytes[7] = ((values[1] >> 10) & 0xff) as u8;
    bytes[8] = ((values[1] >> 2) & 0xff) as u8;
    bytes[9] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 31) & 0x3f)) as u8;
    bytes[10] = ((values[2] >> 23) & 0xff) as u8;
    bytes[11] = ((values[2] >> 15) & 0xff) as u8;
    bytes[12] = ((values[2] >> 7) & 0xff) as u8;
    bytes[13] = ((((values[2]) & 0x7f) << 1) | ((values[3] >> 36) & 0x1)) as u8;
    bytes[14] = ((values[3] >> 28) & 0xff) as u8;
    bytes[15] = ((values[3] >> 20) & 0xff) as u8;
    bytes[16] = ((values[3] >> 12) & 0xff) as u8;
    bytes[17] = ((values[3] >> 4) & 0xff) as u8;
    bytes[18] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 33) & 0xf)) as u8;
    bytes[19] = ((values[4] >> 25) & 0xff) as u8;
    bytes[20] = ((values[4] >> 17) & 0xff) as u8;
    bytes[21] = ((values[4] >> 9) & 0xff) as u8;
    bytes[22] = ((values[4] >> 1) & 0xff) as u8;
    bytes[23] = ((((values[4]) & 0x1) << 7) | ((values[5] >> 30) & 0x7f)) as u8;
    bytes[24] = ((values[5] >> 22) & 0xff) as u8;
    bytes[25] = ((values[5] >> 14) & 0xff) as u8;
    bytes[26] = ((values[5] >> 6) & 0xff) as u8;
    bytes[27] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 35) & 0x3)) as u8;
    bytes[28] = ((values[6] >> 27) & 0xff) as u8;
    bytes[29] = ((values[6] >> 19) & 0xff) as u8;
    bytes[30] = ((values[6] >> 11) & 0xff) as u8;
    bytes[31] = ((values[6] >> 3) & 0xff) as u8;
    bytes[32] = ((((values[6]) & 0x7) << 5) | ((values[7] >> 32) & 0x1f)) as u8;
    bytes[33] = ((values[7] >> 24) & 0xff) as u8;
    bytes[34] = ((values[7] >> 16) & 0xff) as u8;
    bytes[35] = ((values[7] >> 8) & 0xff) as u8;
    bytes[36] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_38(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 38);
    bytes[0] = ((values[0] >> 30) & 0xff) as u8;
    bytes[1] = ((values[0] >> 22) & 0xff) as u8;
    bytes[2] = ((values[0] >> 14) & 0xff) as u8;
    bytes[3] = ((values[0] >> 6) & 0xff) as u8;
    bytes[4] = ((((values[0]) & 0x3f) << 2) | ((values[1] >> 36) & 0x3)) as u8;
    bytes[5] = ((values[1] >> 28) & 0xff) as u8;
    bytes[6] = ((values[1] >> 20) & 0xff) as u8;
    bytes[7] = ((values[1] >> 12) & 0xff) as u8;
    bytes[8] = ((values[1] >> 4) & 0xff) as u8;
    bytes[9] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 34) & 0xf)) as u8;
    bytes[10] = ((values[2] >> 26) & 0xff) as u8;
    bytes[11] = ((values[2] >> 18) & 0xff) as u8;
    bytes[12] = ((values[2] >> 10) & 0xff) as u8;
    bytes[13] = ((values[2] >> 2) & 0xff) as u8;
    bytes[14] = ((((values[2]) & 0x3) << 6) | ((values[3] >> 32) & 0x3f)) as u8;
    bytes[15] = ((values[3] >> 24) & 0xff) as u8;
    bytes[16] = ((values[3] >> 16) & 0xff) as u8;
    bytes[17] = ((values[3] >> 8) & 0xff) as u8;
    bytes[18] = ((values[3]) & 0xff) as u8;
    bytes[19] = ((values[4] >> 30) & 0xff) as u8;
    bytes[20] = ((values[4] >> 22) & 0xff) as u8;
    bytes[21] = ((values[4] >> 14) & 0xff) as u8;
    bytes[22] = ((values[4] >> 6) & 0xff) as u8;
    bytes[23] = ((((values[4]) & 0x3f) << 2) | ((values[5] >> 36) & 0x3)) as u8;
    bytes[24] = ((values[5] >> 28) & 0xff) as u8;
    bytes[25] = ((values[5] >> 20) & 0xff) as u8;
    bytes[26] = ((values[5] >> 12) & 0xff) as u8;
    bytes[27] = ((values[5] >> 4) & 0xff) as u8;
    bytes[28] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 34) & 0xf)) as u8;
    bytes[29] = ((values[6] >> 26) & 0xff) as u8;
    bytes[30] = ((values[6] >> 18) & 0xff) as u8;
    bytes[31] = ((values[6] >> 10) & 0xff) as u8;
    bytes[32] = ((values[6] >> 2) & 0xff) as u8;
    bytes[33] = ((((values[6]) & 0x3) << 6) | ((values[7] >> 32) & 0x3f)) as u8;
    bytes[34] = ((values[7] >> 24) & 0xff) as u8;
    bytes[35] = ((values[7] >> 16) & 0xff) as u8;
    bytes[36] = ((values[7] >> 8) & 0xff) as u8;
    bytes[37] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_39(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 39);
    bytes[0] = ((values[0] >> 31) & 0xff) as u8;
    bytes[1] = ((values[0] >> 23) & 0xff) as u8;
    bytes[2] = ((values[0] >> 15) & 0xff) as u8;
    bytes[3] = ((values[0] >> 7) & 0xff) as u8;
    bytes[4] = ((((values[0]) & 0x7f) << 1) | ((values[1] >> 38) & 0x1)) as u8;
    bytes[5] = ((values[1] >> 30) & 0xff) as u8;
    bytes[6] = ((values[1] >> 22) & 0xff) as u8;
    bytes[7] = ((values[1] >> 14) & 0xff) as u8;
    bytes[8] = ((values[1] >> 6) & 0xff) as u8;
    bytes[9] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 37) & 0x3)) as u8;
    bytes[10] = ((values[2] >> 29) & 0xff) as u8;
    bytes[11] = ((values[2] >> 21) & 0xff) as u8;
    bytes[12] = ((values[2] >> 13) & 0xff) as u8;
    bytes[13] = ((values[2] >> 5) & 0xff) as u8;
    bytes[14] = ((((values[2]) & 0x1f) << 3) | ((values[3] >> 36) & 0x7)) as u8;
    bytes[15] = ((values[3] >> 28) & 0xff) as u8;
    bytes[16] = ((values[3] >> 20) & 0xff) as u8;
    bytes[17] = ((values[3] >> 12) & 0xff) as u8;
    bytes[18] = ((values[3] >> 4) & 0xff) as u8;
    bytes[19] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 35) & 0xf)) as u8;
    bytes[20] = ((values[4] >> 27) & 0xff) as u8;
    bytes[21] = ((values[4] >> 19) & 0xff) as u8;
    bytes[22] = ((values[4] >> 11) & 0xff) as u8;
    bytes[23] = ((values[4] >> 3) & 0xff) as u8;
    bytes[24] = ((((values[4]) & 0x7) << 5) | ((values[5] >> 34) & 0x1f)) as u8;
    bytes[25] = ((values[5] >> 26) & 0xff) as u8;
    bytes[26] = ((values[5] >> 18) & 0xff) as u8;
    bytes[27] = ((values[5] >> 10) & 0xff) as u8;
    bytes[28] = ((values[5] >> 2) & 0xff) as u8;
    bytes[29] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 33) & 0x3f)) as u8;
    bytes[30] = ((values[6] >> 25) & 0xff) as u8;
    bytes[31] = ((values[6] >> 17) & 0xff) as u8;
    bytes[32] = ((values[6] >> 9) & 0xff) as u8;
    bytes[33] = ((values[6] >> 1) & 0xff) as u8;
    bytes[34] = ((((values[6]) & 0x1) << 7) | ((values[7] >> 32) & 0x7f)) as u8;
    bytes[35] = ((values[7] >> 24) & 0xff) as u8;
    bytes[36] = ((values[7] >> 16) & 0xff) as u8;
    bytes[37] = ((values[7] >> 8) & 0xff) as u8;
    bytes[38] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_40(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 40);
    bytes[0] = ((values[0] >> 32) & 0xff) as u8;
    bytes[1] = ((values[0] >> 24) & 0xff) as u8;
    bytes[2] = ((values[0] >> 16) & 0xff) as u8;
    bytes[3] = ((values[0] >> 8) & 0xff) as u8;
    bytes[4] = ((values[0]) & 0xff) as u8;
    bytes[5] = ((values[1] >> 32) & 0xff) as u8;
    bytes[6] = ((values[1] >> 24) & 0xff) as u8;
    bytes[7] = ((values[1] >> 16) & 0xff) as u8;
    bytes[8] = ((values[1] >> 8) & 0xff) as u8;
    bytes[9] = ((values[1]) & 0xff) as u8;
    bytes[10] = ((values[2] >> 32) & 0xff) as u8;
    bytes[11] = ((values[2] >> 24) & 0xff) as u8;
    bytes[12] = ((values[2] >> 16) & 0xff) as u8;
    bytes[13] = ((values[2] >> 8) & 0xff) as u8;
    bytes[14] = ((values[2]) & 0xff) as u8;
    bytes[15] = ((values[3] >> 32) & 0xff) as u8;
    bytes[16] = ((values[3] >> 24) & 0xff) as u8;
    bytes[17] = ((values[3] >> 16) & 0xff) as u8;
    bytes[18] = ((values[3] >> 8) & 0xff) as u8;
    bytes[19] = ((values[3]) & 0xff) as u8;
    bytes[20] = ((values[4] >> 32) & 0xff) as u8;
    bytes[21] = ((values[4] >> 24) & 0xff) as u8;
    bytes[22] = ((values[4] >> 16) & 0xff) as u8;
    bytes[23] = ((values[4] >> 8) & 0xff) as u8;
    bytes[24] = ((values[4]) & 0xff) as u8;
    bytes[25] = ((values[5] >> 32) & 0xff) as u8;
    bytes[26] = ((values[5] >> 24) & 0xff) as u8;
    bytes[27] = ((values[5] >> 16) & 0xff) as u8;
    bytes[28] = ((values[5] >> 8) & 0xff) as u8;
    bytes[29] = ((values[5]) & 0xff) as u8;
    bytes[30] = ((values[6] >> 32) & 0xff) as u8;
    bytes[31] = ((values[6] >> 24) & 0xff) as u8;
    bytes[32] = ((values[6] >> 16) & 0xff) as u8;
    bytes[33] = ((values[6] >> 8) & 0xff) as u8;
    bytes[34] = ((values[6]) & 0xff) as u8;
    bytes[35] = ((values[7] >> 32) & 0xff) as u8;
    bytes[36] = ((values[7] >> 24) & 0xff) as u8;
    bytes[37] = ((values[7] >> 16) & 0xff) as u8;
    bytes[38] = ((values[7] >> 8) & 0xff) as u8;
    bytes[39] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_41(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 41);
    bytes[0] = ((values[0] >> 33) & 0xff) as u8;
    bytes[1] = ((values[0] >> 25) & 0xff) as u8;
    bytes[2] = ((values[0] >> 17) & 0xff) as u8;
    bytes[3] = ((values[0] >> 9) & 0xff) as u8;
    bytes[4] = ((values[0] >> 1) & 0xff) as u8;
    bytes[5] = ((((values[0]) & 0x1) << 7) | ((values[1] >> 34) & 0x7f)) as u8;
    bytes[6] = ((values[1] >> 26) & 0xff) as u8;
    bytes[7] = ((values[1] >> 18) & 0xff) as u8;
    bytes[8] = ((values[1] >> 10) & 0xff) as u8;
    bytes[9] = ((values[1] >> 2) & 0xff) as u8;
    bytes[10] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 35) & 0x3f)) as u8;
    bytes[11] = ((values[2] >> 27) & 0xff) as u8;
    bytes[12] = ((values[2] >> 19) & 0xff) as u8;
    bytes[13] = ((values[2] >> 11) & 0xff) as u8;
    bytes[14] = ((values[2] >> 3) & 0xff) as u8;
    bytes[15] = ((((values[2]) & 0x7) << 5) | ((values[3] >> 36) & 0x1f)) as u8;
    bytes[16] = ((values[3] >> 28) & 0xff) as u8;
    bytes[17] = ((values[3] >> 20) & 0xff) as u8;
    bytes[18] = ((values[3] >> 12) & 0xff) as u8;
    bytes[19] = ((values[3] >> 4) & 0xff) as u8;
    bytes[20] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 37) & 0xf)) as u8;
    bytes[21] = ((values[4] >> 29) & 0xff) as u8;
    bytes[22] = ((values[4] >> 21) & 0xff) as u8;
    bytes[23] = ((values[4] >> 13) & 0xff) as u8;
    bytes[24] = ((values[4] >> 5) & 0xff) as u8;
    bytes[25] = ((((values[4]) & 0x1f) << 3) | ((values[5] >> 38) & 0x7)) as u8;
    bytes[26] = ((values[5] >> 30) & 0xff) as u8;
    bytes[27] = ((values[5] >> 22) & 0xff) as u8;
    bytes[28] = ((values[5] >> 14) & 0xff) as u8;
    bytes[29] = ((values[5] >> 6) & 0xff) as u8;
    bytes[30] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 39) & 0x3)) as u8;
    bytes[31] = ((values[6] >> 31) & 0xff) as u8;
    bytes[32] = ((values[6] >> 23) & 0xff) as u8;
    bytes[33] = ((values[6] >> 15) & 0xff) as u8;
    bytes[34] = ((values[6] >> 7) & 0xff) as u8;
    bytes[35] = ((((values[6]) & 0x7f) << 1) | ((values[7] >> 40) & 0x1)) as u8;
    bytes[36] = ((values[7] >> 32) & 0xff) as u8;
    bytes[37] = ((values[7] >> 24) & 0xff) as u8;
    bytes[38] = ((values[7] >> 16) & 0xff) as u8;
    bytes[39] = ((values[7] >> 8) & 0xff) as u8;
    bytes[40] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_42(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 42);
    bytes[0] = ((values[0] >> 34) & 0xff) as u8;
    bytes[1] = ((values[0] >> 26) & 0xff) as u8;
    bytes[2] = ((values[0] >> 18) & 0xff) as u8;
    bytes[3] = ((values[0] >> 10) & 0xff) as u8;
    bytes[4] = ((values[0] >> 2) & 0xff) as u8;
    bytes[5] = ((((values[0]) & 0x3) << 6) | ((values[1] >> 36) & 0x3f)) as u8;
    bytes[6] = ((values[1] >> 28) & 0xff) as u8;
    bytes[7] = ((values[1] >> 20) & 0xff) as u8;
    bytes[8] = ((values[1] >> 12) & 0xff) as u8;
    bytes[9] = ((values[1] >> 4) & 0xff) as u8;
    bytes[10] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 38) & 0xf)) as u8;
    bytes[11] = ((values[2] >> 30) & 0xff) as u8;
    bytes[12] = ((values[2] >> 22) & 0xff) as u8;
    bytes[13] = ((values[2] >> 14) & 0xff) as u8;
    bytes[14] = ((values[2] >> 6) & 0xff) as u8;
    bytes[15] = ((((values[2]) & 0x3f) << 2) | ((values[3] >> 40) & 0x3)) as u8;
    bytes[16] = ((values[3] >> 32) & 0xff) as u8;
    bytes[17] = ((values[3] >> 24) & 0xff) as u8;
    bytes[18] = ((values[3] >> 16) & 0xff) as u8;
    bytes[19] = ((values[3] >> 8) & 0xff) as u8;
    bytes[20] = ((values[3]) & 0xff) as u8;
    bytes[21] = ((values[4] >> 34) & 0xff) as u8;
    bytes[22] = ((values[4] >> 26) & 0xff) as u8;
    bytes[23] = ((values[4] >> 18) & 0xff) as u8;
    bytes[24] = ((values[4] >> 10) & 0xff) as u8;
    bytes[25] = ((values[4] >> 2) & 0xff) as u8;
    bytes[26] = ((((values[4]) & 0x3) << 6) | ((values[5] >> 36) & 0x3f)) as u8;
    bytes[27] = ((values[5] >> 28) & 0xff) as u8;
    bytes[28] = ((values[5] >> 20) & 0xff) as u8;
    bytes[29] = ((values[5] >> 12) & 0xff) as u8;
    bytes[30] = ((values[5] >> 4) & 0xff) as u8;
    bytes[31] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 38) & 0xf)) as u8;
    bytes[32] = ((values[6] >> 30) & 0xff) as u8;
    bytes[33] = ((values[6] >> 22) & 0xff) as u8;
    bytes[34] = ((values[6] >> 14) & 0xff) as u8;
    bytes[35] = ((values[6] >> 6) & 0xff) as u8;
    bytes[36] = ((((values[6]) & 0x3f) << 2) | ((values[7] >> 40) & 0x3)) as u8;
    bytes[37] = ((values[7] >> 32) & 0xff) as u8;
    bytes[38] = ((values[7] >> 24) & 0xff) as u8;
    bytes[39] = ((values[7] >> 16) & 0xff) as u8;
    bytes[40] = ((values[7] >> 8) & 0xff) as u8;
    bytes[41] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_43(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 43);
    bytes[0] = ((values[0] >> 35) & 0xff) as u8;
    bytes[1] = ((values[0] >> 27) & 0xff) as u8;
    bytes[2] = ((values[0] >> 19) & 0xff) as u8;
    bytes[3] = ((values[0] >> 11) & 0xff) as u8;
    bytes[4] = ((values[0] >> 3) & 0xff) as u8;
    bytes[5] = ((((values[0]) & 0x7) << 5) | ((values[1] >> 38) & 0x1f)) as u8;
    bytes[6] = ((values[1] >> 30) & 0xff) as u8;
    bytes[7] = ((values[1] >> 22) & 0xff) as u8;
    bytes[8] = ((values[1] >> 14) & 0xff) as u8;
    bytes[9] = ((values[1] >> 6) & 0xff) as u8;
    bytes[10] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 41) & 0x3)) as u8;
    bytes[11] = ((values[2] >> 33) & 0xff) as u8;
    bytes[12] = ((values[2] >> 25) & 0xff) as u8;
    bytes[13] = ((values[2] >> 17) & 0xff) as u8;
    bytes[14] = ((values[2] >> 9) & 0xff) as u8;
    bytes[15] = ((values[2] >> 1) & 0xff) as u8;
    bytes[16] = ((((values[2]) & 0x1) << 7) | ((values[3] >> 36) & 0x7f)) as u8;
    bytes[17] = ((values[3] >> 28) & 0xff) as u8;
    bytes[18] = ((values[3] >> 20) & 0xff) as u8;
    bytes[19] = ((values[3] >> 12) & 0xff) as u8;
    bytes[20] = ((values[3] >> 4) & 0xff) as u8;
    bytes[21] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 39) & 0xf)) as u8;
    bytes[22] = ((values[4] >> 31) & 0xff) as u8;
    bytes[23] = ((values[4] >> 23) & 0xff) as u8;
    bytes[24] = ((values[4] >> 15) & 0xff) as u8;
    bytes[25] = ((values[4] >> 7) & 0xff) as u8;
    bytes[26] = ((((values[4]) & 0x7f) << 1) | ((values[5] >> 42) & 0x1)) as u8;
    bytes[27] = ((values[5] >> 34) & 0xff) as u8;
    bytes[28] = ((values[5] >> 26) & 0xff) as u8;
    bytes[29] = ((values[5] >> 18) & 0xff) as u8;
    bytes[30] = ((values[5] >> 10) & 0xff) as u8;
    bytes[31] = ((values[5] >> 2) & 0xff) as u8;
    bytes[32] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 37) & 0x3f)) as u8;
    bytes[33] = ((values[6] >> 29) & 0xff) as u8;
    bytes[34] = ((values[6] >> 21) & 0xff) as u8;
    bytes[35] = ((values[6] >> 13) & 0xff) as u8;
    bytes[36] = ((values[6] >> 5) & 0xff) as u8;
    bytes[37] = ((((values[6]) & 0x1f) << 3) | ((values[7] >> 40) & 0x7)) as u8;
    bytes[38] = ((values[7] >> 32) & 0xff) as u8;
    bytes[39] = ((values[7] >> 24) & 0xff) as u8;
    bytes[40] = ((values[7] >> 16) & 0xff) as u8;
    bytes[41] = ((values[7] >> 8) & 0xff) as u8;
    bytes[42] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_44(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 44);
    bytes[0] = ((values[0] >> 36) & 0xff) as u8;
    bytes[1] = ((values[0] >> 28) & 0xff) as u8;
    bytes[2] = ((values[0] >> 20) & 0xff) as u8;
    bytes[3] = ((values[0] >> 12) & 0xff) as u8;
    bytes[4] = ((values[0] >> 4) & 0xff) as u8;
    bytes[5] = ((((values[0]) & 0xf) << 4) | ((values[1] >> 40) & 0xf)) as u8;
    bytes[6] = ((values[1] >> 32) & 0xff) as u8;
    bytes[7] = ((values[1] >> 24) & 0xff) as u8;
    bytes[8] = ((values[1] >> 16) & 0xff) as u8;
    bytes[9] = ((values[1] >> 8) & 0xff) as u8;
    bytes[10] = ((values[1]) & 0xff) as u8;
    bytes[11] = ((values[2] >> 36) & 0xff) as u8;
    bytes[12] = ((values[2] >> 28) & 0xff) as u8;
    bytes[13] = ((values[2] >> 20) & 0xff) as u8;
    bytes[14] = ((values[2] >> 12) & 0xff) as u8;
    bytes[15] = ((values[2] >> 4) & 0xff) as u8;
    bytes[16] = ((((values[2]) & 0xf) << 4) | ((values[3] >> 40) & 0xf)) as u8;
    bytes[17] = ((values[3] >> 32) & 0xff) as u8;
    bytes[18] = ((values[3] >> 24) & 0xff) as u8;
    bytes[19] = ((values[3] >> 16) & 0xff) as u8;
    bytes[20] = ((values[3] >> 8) & 0xff) as u8;
    bytes[21] = ((values[3]) & 0xff) as u8;
    bytes[22] = ((values[4] >> 36) & 0xff) as u8;
    bytes[23] = ((values[4] >> 28) & 0xff) as u8;
    bytes[24] = ((values[4] >> 20) & 0xff) as u8;
    bytes[25] = ((values[4] >> 12) & 0xff) as u8;
    bytes[26] = ((values[4] >> 4) & 0xff) as u8;
    bytes[27] = ((((values[4]) & 0xf) << 4) | ((values[5] >> 40) & 0xf)) as u8;
    bytes[28] = ((values[5] >> 32) & 0xff) as u8;
    bytes[29] = ((values[5] >> 24) & 0xff) as u8;
    bytes[30] = ((values[5] >> 16) & 0xff) as u8;
    bytes[31] = ((values[5] >> 8) & 0xff) as u8;
    bytes[32] = ((values[5]) & 0xff) as u8;
    bytes[33] = ((values[6] >> 36) & 0xff) as u8;
    bytes[34] = ((values[6] >> 28) & 0xff) as u8;
    bytes[35] = ((values[6] >> 20) & 0xff) as u8;
    bytes[36] = ((values[6] >> 12) & 0xff) as u8;
    bytes[37] = ((values[6] >> 4) & 0xff) as u8;
    bytes[38] = ((((values[6]) & 0xf) << 4) | ((values[7] >> 40) & 0xf)) as u8;
    bytes[39] = ((values[7] >> 32) & 0xff) as u8;
    bytes[40] = ((values[7] >> 24) & 0xff) as u8;
    bytes[41] = ((values[7] >> 16) & 0xff) as u8;
    bytes[42] = ((values[7] >> 8) & 0xff) as u8;
    bytes[43] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_45(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 45);
    bytes[0] = ((values[0] >> 37) & 0xff) as u8;
    bytes[1] = ((values[0] >> 29) & 0xff) as u8;
    bytes[2] = ((values[0] >> 21) & 0xff) as u8;
    bytes[3] = ((values[0] >> 13) & 0xff) as u8;
    bytes[4] = ((values[0] >> 5) & 0xff) as u8;
    bytes[5] = ((((values[0]) & 0x1f) << 3) | ((values[1] >> 42) & 0x7)) as u8;
    bytes[6] = ((values[1] >> 34) & 0xff) as u8;
    bytes[7] = ((values[1] >> 26) & 0xff) as u8;
    bytes[8] = ((values[1] >> 18) & 0xff) as u8;
    bytes[9] = ((values[1] >> 10) & 0xff) as u8;
    bytes[10] = ((values[1] >> 2) & 0xff) as u8;
    bytes[11] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 39) & 0x3f)) as u8;
    bytes[12] = ((values[2] >> 31) & 0xff) as u8;
    bytes[13] = ((values[2] >> 23) & 0xff) as u8;
    bytes[14] = ((values[2] >> 15) & 0xff) as u8;
    bytes[15] = ((values[2] >> 7) & 0xff) as u8;
    bytes[16] = ((((values[2]) & 0x7f) << 1) | ((values[3] >> 44) & 0x1)) as u8;
    bytes[17] = ((values[3] >> 36) & 0xff) as u8;
    bytes[18] = ((values[3] >> 28) & 0xff) as u8;
    bytes[19] = ((values[3] >> 20) & 0xff) as u8;
    bytes[20] = ((values[3] >> 12) & 0xff) as u8;
    bytes[21] = ((values[3] >> 4) & 0xff) as u8;
    bytes[22] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 41) & 0xf)) as u8;
    bytes[23] = ((values[4] >> 33) & 0xff) as u8;
    bytes[24] = ((values[4] >> 25) & 0xff) as u8;
    bytes[25] = ((values[4] >> 17) & 0xff) as u8;
    bytes[26] = ((values[4] >> 9) & 0xff) as u8;
    bytes[27] = ((values[4] >> 1) & 0xff) as u8;
    bytes[28] = ((((values[4]) & 0x1) << 7) | ((values[5] >> 38) & 0x7f)) as u8;
    bytes[29] = ((values[5] >> 30) & 0xff) as u8;
    bytes[30] = ((values[5] >> 22) & 0xff) as u8;
    bytes[31] = ((values[5] >> 14) & 0xff) as u8;
    bytes[32] = ((values[5] >> 6) & 0xff) as u8;
    bytes[33] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 43) & 0x3)) as u8;
    bytes[34] = ((values[6] >> 35) & 0xff) as u8;
    bytes[35] = ((values[6] >> 27) & 0xff) as u8;
    bytes[36] = ((values[6] >> 19) & 0xff) as u8;
    bytes[37] = ((values[6] >> 11) & 0xff) as u8;
    bytes[38] = ((values[6] >> 3) & 0xff) as u8;
    bytes[39] = ((((values[6]) & 0x7) << 5) | ((values[7] >> 40) & 0x1f)) as u8;
    bytes[40] = ((values[7] >> 32) & 0xff) as u8;
    bytes[41] = ((values[7] >> 24) & 0xff) as u8;
    bytes[42] = ((values[7] >> 16) & 0xff) as u8;
    bytes[43] = ((values[7] >> 8) & 0xff) as u8;
    bytes[44] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_46(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 46);
    bytes[0] = ((values[0] >> 38) & 0xff) as u8;
    bytes[1] = ((values[0] >> 30) & 0xff) as u8;
    bytes[2] = ((values[0] >> 22) & 0xff) as u8;
    bytes[3] = ((values[0] >> 14) & 0xff) as u8;
    bytes[4] = ((values[0] >> 6) & 0xff) as u8;
    bytes[5] = ((((values[0]) & 0x3f) << 2) | ((values[1] >> 44) & 0x3)) as u8;
    bytes[6] = ((values[1] >> 36) & 0xff) as u8;
    bytes[7] = ((values[1] >> 28) & 0xff) as u8;
    bytes[8] = ((values[1] >> 20) & 0xff) as u8;
    bytes[9] = ((values[1] >> 12) & 0xff) as u8;
    bytes[10] = ((values[1] >> 4) & 0xff) as u8;
    bytes[11] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 42) & 0xf)) as u8;
    bytes[12] = ((values[2] >> 34) & 0xff) as u8;
    bytes[13] = ((values[2] >> 26) & 0xff) as u8;
    bytes[14] = ((values[2] >> 18) & 0xff) as u8;
    bytes[15] = ((values[2] >> 10) & 0xff) as u8;
    bytes[16] = ((values[2] >> 2) & 0xff) as u8;
    bytes[17] = ((((values[2]) & 0x3) << 6) | ((values[3] >> 40) & 0x3f)) as u8;
    bytes[18] = ((values[3] >> 32) & 0xff) as u8;
    bytes[19] = ((values[3] >> 24) & 0xff) as u8;
    bytes[20] = ((values[3] >> 16) & 0xff) as u8;
    bytes[21] = ((values[3] >> 8) & 0xff) as u8;
    bytes[22] = ((values[3]) & 0xff) as u8;
    bytes[23] = ((values[4] >> 38) & 0xff) as u8;
    bytes[24] = ((values[4] >> 30) & 0xff) as u8;
    bytes[25] = ((values[4] >> 22) & 0xff) as u8;
    bytes[26] = ((values[4] >> 14) & 0xff) as u8;
    bytes[27] = ((values[4] >> 6) & 0xff) as u8;
    bytes[28] = ((((values[4]) & 0x3f) << 2) | ((values[5] >> 44) & 0x3)) as u8;
    bytes[29] = ((values[5] >> 36) & 0xff) as u8;
    bytes[30] = ((values[5] >> 28) & 0xff) as u8;
    bytes[31] = ((values[5] >> 20) & 0xff) as u8;
    bytes[32] = ((values[5] >> 12) & 0xff) as u8;
    bytes[33] = ((values[5] >> 4) & 0xff) as u8;
    bytes[34] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 42) & 0xf)) as u8;
    bytes[35] = ((values[6] >> 34) & 0xff) as u8;
    bytes[36] = ((values[6] >> 26) & 0xff) as u8;
    bytes[37] = ((values[6] >> 18) & 0xff) as u8;
    bytes[38] = ((values[6] >> 10) & 0xff) as u8;
    bytes[39] = ((values[6] >> 2) & 0xff) as u8;
    bytes[40] = ((((values[6]) & 0x3) << 6) | ((values[7] >> 40) & 0x3f)) as u8;
    bytes[41] = ((values[7] >> 32) & 0xff) as u8;
    bytes[42] = ((values[7] >> 24) & 0xff) as u8;
    bytes[43] = ((values[7] >> 16) & 0xff) as u8;
    bytes[44] = ((values[7] >> 8) & 0xff) as u8;
    bytes[45] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_47(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 47);
    bytes[0] = ((values[0] >> 39) & 0xff) as u8;
    bytes[1] = ((values[0] >> 31) & 0xff) as u8;
    bytes[2] = ((values[0] >> 23) & 0xff) as u8;
    bytes[3] = ((values[0] >> 15) & 0xff) as u8;
    bytes[4] = ((values[0] >> 7) & 0xff) as u8;
    bytes[5] = ((((values[0]) & 0x7f) << 1) | ((values[1] >> 46) & 0x1)) as u8;
    bytes[6] = ((values[1] >> 38) & 0xff) as u8;
    bytes[7] = ((values[1] >> 30) & 0xff) as u8;
    bytes[8] = ((values[1] >> 22) & 0xff) as u8;
    bytes[9] = ((values[1] >> 14) & 0xff) as u8;
    bytes[10] = ((values[1] >> 6) & 0xff) as u8;
    bytes[11] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 45) & 0x3)) as u8;
    bytes[12] = ((values[2] >> 37) & 0xff) as u8;
    bytes[13] = ((values[2] >> 29) & 0xff) as u8;
    bytes[14] = ((values[2] >> 21) & 0xff) as u8;
    bytes[15] = ((values[2] >> 13) & 0xff) as u8;
    bytes[16] = ((values[2] >> 5) & 0xff) as u8;
    bytes[17] = ((((values[2]) & 0x1f) << 3) | ((values[3] >> 44) & 0x7)) as u8;
    bytes[18] = ((values[3] >> 36) & 0xff) as u8;
    bytes[19] = ((values[3] >> 28) & 0xff) as u8;
    bytes[20] = ((values[3] >> 20) & 0xff) as u8;
    bytes[21] = ((values[3] >> 12) & 0xff) as u8;
    bytes[22] = ((values[3] >> 4) & 0xff) as u8;
    bytes[23] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 43) & 0xf)) as u8;
    bytes[24] = ((values[4] >> 35) & 0xff) as u8;
    bytes[25] = ((values[4] >> 27) & 0xff) as u8;
    bytes[26] = ((values[4] >> 19) & 0xff) as u8;
    bytes[27] = ((values[4] >> 11) & 0xff) as u8;
    bytes[28] = ((values[4] >> 3) & 0xff) as u8;
    bytes[29] = ((((values[4]) & 0x7) << 5) | ((values[5] >> 42) & 0x1f)) as u8;
    bytes[30] = ((values[5] >> 34) & 0xff) as u8;
    bytes[31] = ((values[5] >> 26) & 0xff) as u8;
    bytes[32] = ((values[5] >> 18) & 0xff) as u8;
    bytes[33] = ((values[5] >> 10) & 0xff) as u8;
    bytes[34] = ((values[5] >> 2) & 0xff) as u8;
    bytes[35] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 41) & 0x3f)) as u8;
    bytes[36] = ((values[6] >> 33) & 0xff) as u8;
    bytes[37] = ((values[6] >> 25) & 0xff) as u8;
    bytes[38] = ((values[6] >> 17) & 0xff) as u8;
    bytes[39] = ((values[6] >> 9) & 0xff) as u8;
    bytes[40] = ((values[6] >> 1) & 0xff) as u8;
    bytes[41] = ((((values[6]) & 0x1) << 7) | ((values[7] >> 40) & 0x7f)) as u8;
    bytes[42] = ((values[7] >> 32) & 0xff) as u8;
    bytes[43] = ((values[7] >> 24) & 0xff) as u8;
    bytes[44] = ((values[7] >> 16) & 0xff) as u8;
    bytes[45] = ((values[7] >> 8) & 0xff) as u8;
    bytes[46] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_48(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 48);
    bytes[0] = ((values[0] >> 40) & 0xff) as u8;
    bytes[1] = ((values[0] >> 32) & 0xff) as u8;
    bytes[2] = ((values[0] >> 24) & 0xff) as u8;
    bytes[3] = ((values[0] >> 16) & 0xff) as u8;
    bytes[4] = ((values[0] >> 8) & 0xff) as u8;
    bytes[5] = ((values[0]) & 0xff) as u8;
    bytes[6] = ((values[1] >> 40) & 0xff) as u8;
    bytes[7] = ((values[1] >> 32) & 0xff) as u8;
    bytes[8] = ((values[1] >> 24) & 0xff) as u8;
    bytes[9] = ((values[1] >> 16) & 0xff) as u8;
    bytes[10] = ((values[1] >> 8) & 0xff) as u8;
    bytes[11] = ((values[1]) & 0xff) as u8;
    bytes[12] = ((values[2] >> 40) & 0xff) as u8;
    bytes[13] = ((values[2] >> 32) & 0xff) as u8;
    bytes[14] = ((values[2] >> 24) & 0xff) as u8;
    bytes[15] = ((values[2] >> 16) & 0xff) as u8;
    bytes[16] = ((values[2] >> 8) & 0xff) as u8;
    bytes[17] = ((values[2]) & 0xff) as u8;
    bytes[18] = ((values[3] >> 40) & 0xff) as u8;
    bytes[19] = ((values[3] >> 32) & 0xff) as u8;
    bytes[20] = ((values[3] >> 24) & 0xff) as u8;
    bytes[21] = ((values[3] >> 16) & 0xff) as u8;
    bytes[22] = ((values[3] >> 8) & 0xff) as u8;
    bytes[23] = ((values[3]) & 0xff) as u8;
    bytes[24] = ((values[4] >> 40) & 0xff) as u8;
    bytes[25] = ((values[4] >> 32) & 0xff) as u8;
    bytes[26] = ((values[4] >> 24) & 0xff) as u8;
    bytes[27] = ((values[4] >> 16) & 0xff) as u8;
    bytes[28] = ((values[4] >> 8) & 0xff) as u8;
    bytes[29] = ((values[4]) & 0xff) as u8;
    bytes[30] = ((values[5] >> 40) & 0xff) as u8;
    bytes[31] = ((values[5] >> 32) & 0xff) as u8;
    bytes[32] = ((values[5] >> 24) & 0xff) as u8;
    bytes[33] = ((values[5] >> 16) & 0xff) as u8;
    bytes[34] = ((values[5] >> 8) & 0xff) as u8;
    bytes[35] = ((values[5]) & 0xff) as u8;
    bytes[36] = ((values[6] >> 40) & 0xff) as u8;
    bytes[37] = ((values[6] >> 32) & 0xff) as u8;
    bytes[38] = ((values[6] >> 24) & 0xff) as u8;
    bytes[39] = ((values[6] >> 16) & 0xff) as u8;
    bytes[40] = ((values[6] >> 8) & 0xff) as u8;
    bytes[41] = ((values[6]) & 0xff) as u8;
    bytes[42] = ((values[7] >> 40) & 0xff) as u8;
    bytes[43] = ((values[7] >> 32) & 0xff) as u8;
    bytes[44] = ((values[7] >> 24) & 0xff) as u8;
    bytes[45] = ((values[7] >> 16) & 0xff) as u8;
    bytes[46] = ((values[7] >> 8) & 0xff) as u8;
    bytes[47] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_49(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 49);
    bytes[0] = ((values[0] >> 41) & 0xff) as u8;
    bytes[1] = ((values[0] >> 33) & 0xff) as u8;
    bytes[2] = ((values[0] >> 25) & 0xff) as u8;
    bytes[3] = ((values[0] >> 17) & 0xff) as u8;
    bytes[4] = ((values[0] >> 9) & 0xff) as u8;
    bytes[5] = ((values[0] >> 1) & 0xff) as u8;
    bytes[6] = ((((values[0]) & 0x1) << 7) | ((values[1] >> 42) & 0x7f)) as u8;
    bytes[7] = ((values[1] >> 34) & 0xff) as u8;
    bytes[8] = ((values[1] >> 26) & 0xff) as u8;
    bytes[9] = ((values[1] >> 18) & 0xff) as u8;
    bytes[10] = ((values[1] >> 10) & 0xff) as u8;
    bytes[11] = ((values[1] >> 2) & 0xff) as u8;
    bytes[12] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 43) & 0x3f)) as u8;
    bytes[13] = ((values[2] >> 35) & 0xff) as u8;
    bytes[14] = ((values[2] >> 27) & 0xff) as u8;
    bytes[15] = ((values[2] >> 19) & 0xff) as u8;
    bytes[16] = ((values[2] >> 11) & 0xff) as u8;
    bytes[17] = ((values[2] >> 3) & 0xff) as u8;
    bytes[18] = ((((values[2]) & 0x7) << 5) | ((values[3] >> 44) & 0x1f)) as u8;
    bytes[19] = ((values[3] >> 36) & 0xff) as u8;
    bytes[20] = ((values[3] >> 28) & 0xff) as u8;
    bytes[21] = ((values[3] >> 20) & 0xff) as u8;
    bytes[22] = ((values[3] >> 12) & 0xff) as u8;
    bytes[23] = ((values[3] >> 4) & 0xff) as u8;
    bytes[24] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 45) & 0xf)) as u8;
    bytes[25] = ((values[4] >> 37) & 0xff) as u8;
    bytes[26] = ((values[4] >> 29) & 0xff) as u8;
    bytes[27] = ((values[4] >> 21) & 0xff) as u8;
    bytes[28] = ((values[4] >> 13) & 0xff) as u8;
    bytes[29] = ((values[4] >> 5) & 0xff) as u8;
    bytes[30] = ((((values[4]) & 0x1f) << 3) | ((values[5] >> 46) & 0x7)) as u8;
    bytes[31] = ((values[5] >> 38) & 0xff) as u8;
    bytes[32] = ((values[5] >> 30) & 0xff) as u8;
    bytes[33] = ((values[5] >> 22) & 0xff) as u8;
    bytes[34] = ((values[5] >> 14) & 0xff) as u8;
    bytes[35] = ((values[5] >> 6) & 0xff) as u8;
    bytes[36] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 47) & 0x3)) as u8;
    bytes[37] = ((values[6] >> 39) & 0xff) as u8;
    bytes[38] = ((values[6] >> 31) & 0xff) as u8;
    bytes[39] = ((values[6] >> 23) & 0xff) as u8;
    bytes[40] = ((values[6] >> 15) & 0xff) as u8;
    bytes[41] = ((values[6] >> 7) & 0xff) as u8;
    bytes[42] = ((((values[6]) & 0x7f) << 1) | ((values[7] >> 48) & 0x1)) as u8;
    bytes[43] = ((values[7] >> 40) & 0xff) as u8;
    bytes[44] = ((values[7] >> 32) & 0xff) as u8;
    bytes[45] = ((values[7] >> 24) & 0xff) as u8;
    bytes[46] = ((values[7] >> 16) & 0xff) as u8;
    bytes[47] = ((values[7] >> 8) & 0xff) as u8;
    bytes[48] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_50(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 50);
    bytes[0] = ((values[0] >> 42) & 0xff) as u8;
    bytes[1] = ((values[0] >> 34) & 0xff) as u8;
    bytes[2] = ((values[0] >> 26) & 0xff) as u8;
    bytes[3] = ((values[0] >> 18) & 0xff) as u8;
    bytes[4] = ((values[0] >> 10) & 0xff) as u8;
    bytes[5] = ((values[0] >> 2) & 0xff) as u8;
    bytes[6] = ((((values[0]) & 0x3) << 6) | ((values[1] >> 44) & 0x3f)) as u8;
    bytes[7] = ((values[1] >> 36) & 0xff) as u8;
    bytes[8] = ((values[1] >> 28) & 0xff) as u8;
    bytes[9] = ((values[1] >> 20) & 0xff) as u8;
    bytes[10] = ((values[1] >> 12) & 0xff) as u8;
    bytes[11] = ((values[1] >> 4) & 0xff) as u8;
    bytes[12] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 46) & 0xf)) as u8;
    bytes[13] = ((values[2] >> 38) & 0xff) as u8;
    bytes[14] = ((values[2] >> 30) & 0xff) as u8;
    bytes[15] = ((values[2] >> 22) & 0xff) as u8;
    bytes[16] = ((values[2] >> 14) & 0xff) as u8;
    bytes[17] = ((values[2] >> 6) & 0xff) as u8;
    bytes[18] = ((((values[2]) & 0x3f) << 2) | ((values[3] >> 48) & 0x3)) as u8;
    bytes[19] = ((values[3] >> 40) & 0xff) as u8;
    bytes[20] = ((values[3] >> 32) & 0xff) as u8;
    bytes[21] = ((values[3] >> 24) & 0xff) as u8;
    bytes[22] = ((values[3] >> 16) & 0xff) as u8;
    bytes[23] = ((values[3] >> 8) & 0xff) as u8;
    bytes[24] = ((values[3]) & 0xff) as u8;
    bytes[25] = ((values[4] >> 42) & 0xff) as u8;
    bytes[26] = ((values[4] >> 34) & 0xff) as u8;
    bytes[27] = ((values[4] >> 26) & 0xff) as u8;
    bytes[28] = ((values[4] >> 18) & 0xff) as u8;
    bytes[29] = ((values[4] >> 10) & 0xff) as u8;
    bytes[30] = ((values[4] >> 2) & 0xff) as u8;
    bytes[31] = ((((values[4]) & 0x3) << 6) | ((values[5] >> 44) & 0x3f)) as u8;
    bytes[32] = ((values[5] >> 36) & 0xff) as u8;
    bytes[33] = ((values[5] >> 28) & 0xff) as u8;
    bytes[34] = ((values[5] >> 20) & 0xff) as u8;
    bytes[35] = ((values[5] >> 12) & 0xff) as u8;
    bytes[36] = ((values[5] >> 4) & 0xff) as u8;
    bytes[37] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 46) & 0xf)) as u8;
    bytes[38] = ((values[6] >> 38) & 0xff) as u8;
    bytes[39] = ((values[6] >> 30) & 0xff) as u8;
    bytes[40] = ((values[6] >> 22) & 0xff) as u8;
    bytes[41] = ((values[6] >> 14) & 0xff) as u8;
    bytes[42] = ((values[6] >> 6) & 0xff) as u8;
    bytes[43] = ((((values[6]) & 0x3f) << 2) | ((values[7] >> 48) & 0x3)) as u8;
    bytes[44] = ((values[7] >> 40) & 0xff) as u8;
    bytes[45] = ((values[7] >> 32) & 0xff) as u8;
    bytes[46] = ((values[7] >> 24) & 0xff) as u8;
    bytes[47] = ((values[7] >> 16) & 0xff) as u8;
    bytes[48] = ((values[7] >> 8) & 0xff) as u8;
    bytes[49] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_51(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 51);
    bytes[0] = ((values[0] >> 43) & 0xff) as u8;
    bytes[1] = ((values[0] >> 35) & 0xff) as u8;
    bytes[2] = ((values[0] >> 27) & 0xff) as u8;
    bytes[3] = ((values[0] >> 19) & 0xff) as u8;
    bytes[4] = ((values[0] >> 11) & 0xff) as u8;
    bytes[5] = ((values[0] >> 3) & 0xff) as u8;
    bytes[6] = ((((values[0]) & 0x7) << 5) | ((values[1] >> 46) & 0x1f)) as u8;
    bytes[7] = ((values[1] >> 38) & 0xff) as u8;
    bytes[8] = ((values[1] >> 30) & 0xff) as u8;
    bytes[9] = ((values[1] >> 22) & 0xff) as u8;
    bytes[10] = ((values[1] >> 14) & 0xff) as u8;
    bytes[11] = ((values[1] >> 6) & 0xff) as u8;
    bytes[12] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 49) & 0x3)) as u8;
    bytes[13] = ((values[2] >> 41) & 0xff) as u8;
    bytes[14] = ((values[2] >> 33) & 0xff) as u8;
    bytes[15] = ((values[2] >> 25) & 0xff) as u8;
    bytes[16] = ((values[2] >> 17) & 0xff) as u8;
    bytes[17] = ((values[2] >> 9) & 0xff) as u8;
    bytes[18] = ((values[2] >> 1) & 0xff) as u8;
    bytes[19] = ((((values[2]) & 0x1) << 7) | ((values[3] >> 44) & 0x7f)) as u8;
    bytes[20] = ((values[3] >> 36) & 0xff) as u8;
    bytes[21] = ((values[3] >> 28) & 0xff) as u8;
    bytes[22] = ((values[3] >> 20) & 0xff) as u8;
    bytes[23] = ((values[3] >> 12) & 0xff) as u8;
    bytes[24] = ((values[3] >> 4) & 0xff) as u8;
    bytes[25] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 47) & 0xf)) as u8;
    bytes[26] = ((values[4] >> 39) & 0xff) as u8;
    bytes[27] = ((values[4] >> 31) & 0xff) as u8;
    bytes[28] = ((values[4] >> 23) & 0xff) as u8;
    bytes[29] = ((values[4] >> 15) & 0xff) as u8;
    bytes[30] = ((values[4] >> 7) & 0xff) as u8;
    bytes[31] = ((((values[4]) & 0x7f) << 1) | ((values[5] >> 50) & 0x1)) as u8;
    bytes[32] = ((values[5] >> 42) & 0xff) as u8;
    bytes[33] = ((values[5] >> 34) & 0xff) as u8;
    bytes[34] = ((values[5] >> 26) & 0xff) as u8;
    bytes[35] = ((values[5] >> 18) & 0xff) as u8;
    bytes[36] = ((values[5] >> 10) & 0xff) as u8;
    bytes[37] = ((values[5] >> 2) & 0xff) as u8;
    bytes[38] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 45) & 0x3f)) as u8;
    bytes[39] = ((values[6] >> 37) & 0xff) as u8;
    bytes[40] = ((values[6] >> 29) & 0xff) as u8;
    bytes[41] = ((values[6] >> 21) & 0xff) as u8;
    bytes[42] = ((values[6] >> 13) & 0xff) as u8;
    bytes[43] = ((values[6] >> 5) & 0xff) as u8;
    bytes[44] = ((((values[6]) & 0x1f) << 3) | ((values[7] >> 48) & 0x7)) as u8;
    bytes[45] = ((values[7] >> 40) & 0xff) as u8;
    bytes[46] = ((values[7] >> 32) & 0xff) as u8;
    bytes[47] = ((values[7] >> 24) & 0xff) as u8;
    bytes[48] = ((values[7] >> 16) & 0xff) as u8;
    bytes[49] = ((values[7] >> 8) & 0xff) as u8;
    bytes[50] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_52(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 52);
    bytes[0] = ((values[0] >> 44) & 0xff) as u8;
    bytes[1] = ((values[0] >> 36) & 0xff) as u8;
    bytes[2] = ((values[0] >> 28) & 0xff) as u8;
    bytes[3] = ((values[0] >> 20) & 0xff) as u8;
    bytes[4] = ((values[0] >> 12) & 0xff) as u8;
    bytes[5] = ((values[0] >> 4) & 0xff) as u8;
    bytes[6] = ((((values[0]) & 0xf) << 4) | ((values[1] >> 48) & 0xf)) as u8;
    bytes[7] = ((values[1] >> 40) & 0xff) as u8;
    bytes[8] = ((values[1] >> 32) & 0xff) as u8;
    bytes[9] = ((values[1] >> 24) & 0xff) as u8;
    bytes[10] = ((values[1] >> 16) & 0xff) as u8;
    bytes[11] = ((values[1] >> 8) & 0xff) as u8;
    bytes[12] = ((values[1]) & 0xff) as u8;
    bytes[13] = ((values[2] >> 44) & 0xff) as u8;
    bytes[14] = ((values[2] >> 36) & 0xff) as u8;
    bytes[15] = ((values[2] >> 28) & 0xff) as u8;
    bytes[16] = ((values[2] >> 20) & 0xff) as u8;
    bytes[17] = ((values[2] >> 12) & 0xff) as u8;
    bytes[18] = ((values[2] >> 4) & 0xff) as u8;
    bytes[19] = ((((values[2]) & 0xf) << 4) | ((values[3] >> 48) & 0xf)) as u8;
    bytes[20] = ((values[3] >> 40) & 0xff) as u8;
    bytes[21] = ((values[3] >> 32) & 0xff) as u8;
    bytes[22] = ((values[3] >> 24) & 0xff) as u8;
    bytes[23] = ((values[3] >> 16) & 0xff) as u8;
    bytes[24] = ((values[3] >> 8) & 0xff) as u8;
    bytes[25] = ((values[3]) & 0xff) as u8;
    bytes[26] = ((values[4] >> 44) & 0xff) as u8;
    bytes[27] = ((values[4] >> 36) & 0xff) as u8;
    bytes[28] = ((values[4] >> 28) & 0xff) as u8;
    bytes[29] = ((values[4] >> 20) & 0xff) as u8;
    bytes[30] = ((values[4] >> 12) & 0xff) as u8;
    bytes[31] = ((values[4] >> 4) & 0xff) as u8;
    bytes[32] = ((((values[4]) & 0xf) << 4) | ((values[5] >> 48) & 0xf)) as u8;
    bytes[33] = ((values[5] >> 40) & 0xff) as u8;
    bytes[34] = ((values[5] >> 32) & 0xff) as u8;
    bytes[35] = ((values[5] >> 24) & 0xff) as u8;
    bytes[36] = ((values[5] >> 16) & 0xff) as u8;
    bytes[37] = ((values[5] >> 8) & 0xff) as u8;
    bytes[38] = ((values[5]) & 0xff) as u8;
    bytes[39] = ((values[6] >> 44) & 0xff) as u8;
    bytes[40] = ((values[6] >> 36) & 0xff) as u8;
    bytes[41] = ((values[6] >> 28) & 0xff) as u8;
    bytes[42] = ((values[6] >> 20) & 0xff) as u8;
    bytes[43] = ((values[6] >> 12) & 0xff) as u8;
    bytes[44] = ((values[6] >> 4) & 0xff) as u8;
    bytes[45] = ((((values[6]) & 0xf) << 4) | ((values[7] >> 48) & 0xf)) as u8;
    bytes[46] = ((values[7] >> 40) & 0xff) as u8;
    bytes[47] = ((values[7] >> 32) & 0xff) as u8;
    bytes[48] = ((values[7] >> 24) & 0xff) as u8;
    bytes[49] = ((values[7] >> 16) & 0xff) as u8;
    bytes[50] = ((values[7] >> 8) & 0xff) as u8;
    bytes[51] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_53(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 53);
    bytes[0] = ((values[0] >> 45) & 0xff) as u8;
    bytes[1] = ((values[0] >> 37) & 0xff) as u8;
    bytes[2] = ((values[0] >> 29) & 0xff) as u8;
    bytes[3] = ((values[0] >> 21) & 0xff) as u8;
    bytes[4] = ((values[0] >> 13) & 0xff) as u8;
    bytes[5] = ((values[0] >> 5) & 0xff) as u8;
    bytes[6] = ((((values[0]) & 0x1f) << 3) | ((values[1] >> 50) & 0x7)) as u8;
    bytes[7] = ((values[1] >> 42) & 0xff) as u8;
    bytes[8] = ((values[1] >> 34) & 0xff) as u8;
    bytes[9] = ((values[1] >> 26) & 0xff) as u8;
    bytes[10] = ((values[1] >> 18) & 0xff) as u8;
    bytes[11] = ((values[1] >> 10) & 0xff) as u8;
    bytes[12] = ((values[1] >> 2) & 0xff) as u8;
    bytes[13] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 47) & 0x3f)) as u8;
    bytes[14] = ((values[2] >> 39) & 0xff) as u8;
    bytes[15] = ((values[2] >> 31) & 0xff) as u8;
    bytes[16] = ((values[2] >> 23) & 0xff) as u8;
    bytes[17] = ((values[2] >> 15) & 0xff) as u8;
    bytes[18] = ((values[2] >> 7) & 0xff) as u8;
    bytes[19] = ((((values[2]) & 0x7f) << 1) | ((values[3] >> 52) & 0x1)) as u8;
    bytes[20] = ((values[3] >> 44) & 0xff) as u8;
    bytes[21] = ((values[3] >> 36) & 0xff) as u8;
    bytes[22] = ((values[3] >> 28) & 0xff) as u8;
    bytes[23] = ((values[3] >> 20) & 0xff) as u8;
    bytes[24] = ((values[3] >> 12) & 0xff) as u8;
    bytes[25] = ((values[3] >> 4) & 0xff) as u8;
    bytes[26] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 49) & 0xf)) as u8;
    bytes[27] = ((values[4] >> 41) & 0xff) as u8;
    bytes[28] = ((values[4] >> 33) & 0xff) as u8;
    bytes[29] = ((values[4] >> 25) & 0xff) as u8;
    bytes[30] = ((values[4] >> 17) & 0xff) as u8;
    bytes[31] = ((values[4] >> 9) & 0xff) as u8;
    bytes[32] = ((values[4] >> 1) & 0xff) as u8;
    bytes[33] = ((((values[4]) & 0x1) << 7) | ((values[5] >> 46) & 0x7f)) as u8;
    bytes[34] = ((values[5] >> 38) & 0xff) as u8;
    bytes[35] = ((values[5] >> 30) & 0xff) as u8;
    bytes[36] = ((values[5] >> 22) & 0xff) as u8;
    bytes[37] = ((values[5] >> 14) & 0xff) as u8;
    bytes[38] = ((values[5] >> 6) & 0xff) as u8;
    bytes[39] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 51) & 0x3)) as u8;
    bytes[40] = ((values[6] >> 43) & 0xff) as u8;
    bytes[41] = ((values[6] >> 35) & 0xff) as u8;
    bytes[42] = ((values[6] >> 27) & 0xff) as u8;
    bytes[43] = ((values[6] >> 19) & 0xff) as u8;
    bytes[44] = ((values[6] >> 11) & 0xff) as u8;
    bytes[45] = ((values[6] >> 3) & 0xff) as u8;
    bytes[46] = ((((values[6]) & 0x7) << 5) | ((values[7] >> 48) & 0x1f)) as u8;
    bytes[47] = ((values[7] >> 40) & 0xff) as u8;
    bytes[48] = ((values[7] >> 32) & 0xff) as u8;
    bytes[49] = ((values[7] >> 24) & 0xff) as u8;
    bytes[50] = ((values[7] >> 16) & 0xff) as u8;
    bytes[51] = ((values[7] >> 8) & 0xff) as u8;
    bytes[52] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_54(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 54);
    bytes[0] = ((values[0] >> 46) & 0xff) as u8;
    bytes[1] = ((values[0] >> 38) & 0xff) as u8;
    bytes[2] = ((values[0] >> 30) & 0xff) as u8;
    bytes[3] = ((values[0] >> 22) & 0xff) as u8;
    bytes[4] = ((values[0] >> 14) & 0xff) as u8;
    bytes[5] = ((values[0] >> 6) & 0xff) as u8;
    bytes[6] = ((((values[0]) & 0x3f) << 2) | ((values[1] >> 52) & 0x3)) as u8;
    bytes[7] = ((values[1] >> 44) & 0xff) as u8;
    bytes[8] = ((values[1] >> 36) & 0xff) as u8;
    bytes[9] = ((values[1] >> 28) & 0xff) as u8;
    bytes[10] = ((values[1] >> 20) & 0xff) as u8;
    bytes[11] = ((values[1] >> 12) & 0xff) as u8;
    bytes[12] = ((values[1] >> 4) & 0xff) as u8;
    bytes[13] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 50) & 0xf)) as u8;
    bytes[14] = ((values[2] >> 42) & 0xff) as u8;
    bytes[15] = ((values[2] >> 34) & 0xff) as u8;
    bytes[16] = ((values[2] >> 26) & 0xff) as u8;
    bytes[17] = ((values[2] >> 18) & 0xff) as u8;
    bytes[18] = ((values[2] >> 10) & 0xff) as u8;
    bytes[19] = ((values[2] >> 2) & 0xff) as u8;
    bytes[20] = ((((values[2]) & 0x3) << 6) | ((values[3] >> 48) & 0x3f)) as u8;
    bytes[21] = ((values[3] >> 40) & 0xff) as u8;
    bytes[22] = ((values[3] >> 32) & 0xff) as u8;
    bytes[23] = ((values[3] >> 24) & 0xff) as u8;
    bytes[24] = ((values[3] >> 16) & 0xff) as u8;
    bytes[25] = ((values[3] >> 8) & 0xff) as u8;
    bytes[26] = ((values[3]) & 0xff) as u8;
    bytes[27] = ((values[4] >> 46) & 0xff) as u8;
    bytes[28] = ((values[4] >> 38) & 0xff) as u8;
    bytes[29] = ((values[4] >> 30) & 0xff) as u8;
    bytes[30] = ((values[4] >> 22) & 0xff) as u8;
    bytes[31] = ((values[4] >> 14) & 0xff) as u8;
    bytes[32] = ((values[4] >> 6) & 0xff) as u8;
    bytes[33] = ((((values[4]) & 0x3f) << 2) | ((values[5] >> 52) & 0x3)) as u8;
    bytes[34] = ((values[5] >> 44) & 0xff) as u8;
    bytes[35] = ((values[5] >> 36) & 0xff) as u8;
    bytes[36] = ((values[5] >> 28) & 0xff) as u8;
    bytes[37] = ((values[5] >> 20) & 0xff) as u8;
    bytes[38] = ((values[5] >> 12) & 0xff) as u8;
    bytes[39] = ((values[5] >> 4) & 0xff) as u8;
    bytes[40] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 50) & 0xf)) as u8;
    bytes[41] = ((values[6] >> 42) & 0xff) as u8;
    bytes[42] = ((values[6] >> 34) & 0xff) as u8;
    bytes[43] = ((values[6] >> 26) & 0xff) as u8;
    bytes[44] = ((values[6] >> 18) & 0xff) as u8;
    bytes[45] = ((values[6] >> 10) & 0xff) as u8;
    bytes[46] = ((values[6] >> 2) & 0xff) as u8;
    bytes[47] = ((((values[6]) & 0x3) << 6) | ((values[7] >> 48) & 0x3f)) as u8;
    bytes[48] = ((values[7] >> 40) & 0xff) as u8;
    bytes[49] = ((values[7] >> 32) & 0xff) as u8;
    bytes[50] = ((values[7] >> 24) & 0xff) as u8;
    bytes[51] = ((values[7] >> 16) & 0xff) as u8;
    bytes[52] = ((values[7] >> 8) & 0xff) as u8;
    bytes[53] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_55(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 55);
    bytes[0] = ((values[0] >> 47) & 0xff) as u8;
    bytes[1] = ((values[0] >> 39) & 0xff) as u8;
    bytes[2] = ((values[0] >> 31) & 0xff) as u8;
    bytes[3] = ((values[0] >> 23) & 0xff) as u8;
    bytes[4] = ((values[0] >> 15) & 0xff) as u8;
    bytes[5] = ((values[0] >> 7) & 0xff) as u8;
    bytes[6] = ((((values[0]) & 0x7f) << 1) | ((values[1] >> 54) & 0x1)) as u8;
    bytes[7] = ((values[1] >> 46) & 0xff) as u8;
    bytes[8] = ((values[1] >> 38) & 0xff) as u8;
    bytes[9] = ((values[1] >> 30) & 0xff) as u8;
    bytes[10] = ((values[1] >> 22) & 0xff) as u8;
    bytes[11] = ((values[1] >> 14) & 0xff) as u8;
    bytes[12] = ((values[1] >> 6) & 0xff) as u8;
    bytes[13] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 53) & 0x3)) as u8;
    bytes[14] = ((values[2] >> 45) & 0xff) as u8;
    bytes[15] = ((values[2] >> 37) & 0xff) as u8;
    bytes[16] = ((values[2] >> 29) & 0xff) as u8;
    bytes[17] = ((values[2] >> 21) & 0xff) as u8;
    bytes[18] = ((values[2] >> 13) & 0xff) as u8;
    bytes[19] = ((values[2] >> 5) & 0xff) as u8;
    bytes[20] = ((((values[2]) & 0x1f) << 3) | ((values[3] >> 52) & 0x7)) as u8;
    bytes[21] = ((values[3] >> 44) & 0xff) as u8;
    bytes[22] = ((values[3] >> 36) & 0xff) as u8;
    bytes[23] = ((values[3] >> 28) & 0xff) as u8;
    bytes[24] = ((values[3] >> 20) & 0xff) as u8;
    bytes[25] = ((values[3] >> 12) & 0xff) as u8;
    bytes[26] = ((values[3] >> 4) & 0xff) as u8;
    bytes[27] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 51) & 0xf)) as u8;
    bytes[28] = ((values[4] >> 43) & 0xff) as u8;
    bytes[29] = ((values[4] >> 35) & 0xff) as u8;
    bytes[30] = ((values[4] >> 27) & 0xff) as u8;
    bytes[31] = ((values[4] >> 19) & 0xff) as u8;
    bytes[32] = ((values[4] >> 11) & 0xff) as u8;
    bytes[33] = ((values[4] >> 3) & 0xff) as u8;
    bytes[34] = ((((values[4]) & 0x7) << 5) | ((values[5] >> 50) & 0x1f)) as u8;
    bytes[35] = ((values[5] >> 42) & 0xff) as u8;
    bytes[36] = ((values[5] >> 34) & 0xff) as u8;
    bytes[37] = ((values[5] >> 26) & 0xff) as u8;
    bytes[38] = ((values[5] >> 18) & 0xff) as u8;
    bytes[39] = ((values[5] >> 10) & 0xff) as u8;
    bytes[40] = ((values[5] >> 2) & 0xff) as u8;
    bytes[41] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 49) & 0x3f)) as u8;
    bytes[42] = ((values[6] >> 41) & 0xff) as u8;
    bytes[43] = ((values[6] >> 33) & 0xff) as u8;
    bytes[44] = ((values[6] >> 25) & 0xff) as u8;
    bytes[45] = ((values[6] >> 17) & 0xff) as u8;
    bytes[46] = ((values[6] >> 9) & 0xff) as u8;
    bytes[47] = ((values[6] >> 1) & 0xff) as u8;
    bytes[48] = ((((values[6]) & 0x1) << 7) | ((values[7] >> 48) & 0x7f)) as u8;
    bytes[49] = ((values[7] >> 40) & 0xff) as u8;
    bytes[50] = ((values[7] >> 32) & 0xff) as u8;
    bytes[51] = ((values[7] >> 24) & 0xff) as u8;
    bytes[52] = ((values[7] >> 16) & 0xff) as u8;
    bytes[53] = ((values[7] >> 8) & 0xff) as u8;
    bytes[54] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_56(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 56);
    bytes[0] = ((values[0] >> 48) & 0xff) as u8;
    bytes[1] = ((values[0] >> 40) & 0xff) as u8;
    bytes[2] = ((values[0] >> 32) & 0xff) as u8;
    bytes[3] = ((values[0] >> 24) & 0xff) as u8;
    bytes[4] = ((values[0] >> 16) & 0xff) as u8;
    bytes[5] = ((values[0] >> 8) & 0xff) as u8;
    bytes[6] = ((values[0]) & 0xff) as u8;
    bytes[7] = ((values[1] >> 48) & 0xff) as u8;
    bytes[8] = ((values[1] >> 40) & 0xff) as u8;
    bytes[9] = ((values[1] >> 32) & 0xff) as u8;
    bytes[10] = ((values[1] >> 24) & 0xff) as u8;
    bytes[11] = ((values[1] >> 16) & 0xff) as u8;
    bytes[12] = ((values[1] >> 8) & 0xff) as u8;
    bytes[13] = ((values[1]) & 0xff) as u8;
    bytes[14] = ((values[2] >> 48) & 0xff) as u8;
    bytes[15] = ((values[2] >> 40) & 0xff) as u8;
    bytes[16] = ((values[2] >> 32) & 0xff) as u8;
    bytes[17] = ((values[2] >> 24) & 0xff) as u8;
    bytes[18] = ((values[2] >> 16) & 0xff) as u8;
    bytes[19] = ((values[2] >> 8) & 0xff) as u8;
    bytes[20] = ((values[2]) & 0xff) as u8;
    bytes[21] = ((values[3] >> 48) & 0xff) as u8;
    bytes[22] = ((values[3] >> 40) & 0xff) as u8;
    bytes[23] = ((values[3] >> 32) & 0xff) as u8;
    bytes[24] = ((values[3] >> 24) & 0xff) as u8;
    bytes[25] = ((values[3] >> 16) & 0xff) as u8;
    bytes[26] = ((values[3] >> 8) & 0xff) as u8;
    bytes[27] = ((values[3]) & 0xff) as u8;
    bytes[28] = ((values[4] >> 48) & 0xff) as u8;
    bytes[29] = ((values[4] >> 40) & 0xff) as u8;
    bytes[30] = ((values[4] >> 32) & 0xff) as u8;
    bytes[31] = ((values[4] >> 24) & 0xff) as u8;
    bytes[32] = ((values[4] >> 16) & 0xff) as u8;
    bytes[33] = ((values[4] >> 8) & 0xff) as u8;
    bytes[34] = ((values[4]) & 0xff) as u8;
    bytes[35] = ((values[5] >> 48) & 0xff) as u8;
    bytes[36] = ((values[5] >> 40) & 0xff) as u8;
    bytes[37] = ((values[5] >> 32) & 0xff) as u8;
    bytes[38] = ((values[5] >> 24) & 0xff) as u8;
    bytes[39] = ((values[5] >> 16) & 0xff) as u8;
    bytes[40] = ((values[5] >> 8) & 0xff) as u8;
    bytes[41] = ((values[5]) & 0xff) as u8;
    bytes[42] = ((values[6] >> 48) & 0xff) as u8;
    bytes[43] = ((values[6] >> 40) & 0xff) as u8;
    bytes[44] = ((values[6] >> 32) & 0xff) as u8;
    bytes[45] = ((values[6] >> 24) & 0xff) as u8;
    bytes[46] = ((values[6] >> 16) & 0xff) as u8;
    bytes[47] = ((values[6] >> 8) & 0xff) as u8;
    bytes[48] = ((values[6]) & 0xff) as u8;
    bytes[49] = ((values[7] >> 48) & 0xff) as u8;
    bytes[50] = ((values[7] >> 40) & 0xff) as u8;
    bytes[51] = ((values[7] >> 32) & 0xff) as u8;
    bytes[52] = ((values[7] >> 24) & 0xff) as u8;
    bytes[53] = ((values[7] >> 16) & 0xff) as u8;
    bytes[54] = ((values[7] >> 8) & 0xff) as u8;
    bytes[55] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_57(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 57);
    bytes[0] = ((values[0] >> 49) & 0xff) as u8;
    bytes[1] = ((values[0] >> 41) & 0xff) as u8;
    bytes[2] = ((values[0] >> 33) & 0xff) as u8;
    bytes[3] = ((values[0] >> 25) & 0xff) as u8;
    bytes[4] = ((values[0] >> 17) & 0xff) as u8;
    bytes[5] = ((values[0] >> 9) & 0xff) as u8;
    bytes[6] = ((values[0] >> 1) & 0xff) as u8;
    bytes[7] = ((((values[0]) & 0x1) << 7) | ((values[1] >> 50) & 0x7f)) as u8;
    bytes[8] = ((values[1] >> 42) & 0xff) as u8;
    bytes[9] = ((values[1] >> 34) & 0xff) as u8;
    bytes[10] = ((values[1] >> 26) & 0xff) as u8;
    bytes[11] = ((values[1] >> 18) & 0xff) as u8;
    bytes[12] = ((values[1] >> 10) & 0xff) as u8;
    bytes[13] = ((values[1] >> 2) & 0xff) as u8;
    bytes[14] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 51) & 0x3f)) as u8;
    bytes[15] = ((values[2] >> 43) & 0xff) as u8;
    bytes[16] = ((values[2] >> 35) & 0xff) as u8;
    bytes[17] = ((values[2] >> 27) & 0xff) as u8;
    bytes[18] = ((values[2] >> 19) & 0xff) as u8;
    bytes[19] = ((values[2] >> 11) & 0xff) as u8;
    bytes[20] = ((values[2] >> 3) & 0xff) as u8;
    bytes[21] = ((((values[2]) & 0x7) << 5) | ((values[3] >> 52) & 0x1f)) as u8;
    bytes[22] = ((values[3] >> 44) & 0xff) as u8;
    bytes[23] = ((values[3] >> 36) & 0xff) as u8;
    bytes[24] = ((values[3] >> 28) & 0xff) as u8;
    bytes[25] = ((values[3] >> 20) & 0xff) as u8;
    bytes[26] = ((values[3] >> 12) & 0xff) as u8;
    bytes[27] = ((values[3] >> 4) & 0xff) as u8;
    bytes[28] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 53) & 0xf)) as u8;
    bytes[29] = ((values[4] >> 45) & 0xff) as u8;
    bytes[30] = ((values[4] >> 37) & 0xff) as u8;
    bytes[31] = ((values[4] >> 29) & 0xff) as u8;
    bytes[32] = ((values[4] >> 21) & 0xff) as u8;
    bytes[33] = ((values[4] >> 13) & 0xff) as u8;
    bytes[34] = ((values[4] >> 5) & 0xff) as u8;
    bytes[35] = ((((values[4]) & 0x1f) << 3) | ((values[5] >> 54) & 0x7)) as u8;
    bytes[36] = ((values[5] >> 46) & 0xff) as u8;
    bytes[37] = ((values[5] >> 38) & 0xff) as u8;
    bytes[38] = ((values[5] >> 30) & 0xff) as u8;
    bytes[39] = ((values[5] >> 22) & 0xff) as u8;
    bytes[40] = ((values[5] >> 14) & 0xff) as u8;
    bytes[41] = ((values[5] >> 6) & 0xff) as u8;
    bytes[42] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 55) & 0x3)) as u8;
    bytes[43] = ((values[6] >> 47) & 0xff) as u8;
    bytes[44] = ((values[6] >> 39) & 0xff) as u8;
    bytes[45] = ((values[6] >> 31) & 0xff) as u8;
    bytes[46] = ((values[6] >> 23) & 0xff) as u8;
    bytes[47] = ((values[6] >> 15) & 0xff) as u8;
    bytes[48] = ((values[6] >> 7) & 0xff) as u8;
    bytes[49] = ((((values[6]) & 0x7f) << 1) | ((values[7] >> 56) & 0x1)) as u8;
    bytes[50] = ((values[7] >> 48) & 0xff) as u8;
    bytes[51] = ((values[7] >> 40) & 0xff) as u8;
    bytes[52] = ((values[7] >> 32) & 0xff) as u8;
    bytes[53] = ((values[7] >> 24) & 0xff) as u8;
    bytes[54] = ((values[7] >> 16) & 0xff) as u8;
    bytes[55] = ((values[7] >> 8) & 0xff) as u8;
    bytes[56] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_58(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 58);
    bytes[0] = ((values[0] >> 50) & 0xff) as u8;
    bytes[1] = ((values[0] >> 42) & 0xff) as u8;
    bytes[2] = ((values[0] >> 34) & 0xff) as u8;
    bytes[3] = ((values[0] >> 26) & 0xff) as u8;
    bytes[4] = ((values[0] >> 18) & 0xff) as u8;
    bytes[5] = ((values[0] >> 10) & 0xff) as u8;
    bytes[6] = ((values[0] >> 2) & 0xff) as u8;
    bytes[7] = ((((values[0]) & 0x3) << 6) | ((values[1] >> 52) & 0x3f)) as u8;
    bytes[8] = ((values[1] >> 44) & 0xff) as u8;
    bytes[9] = ((values[1] >> 36) & 0xff) as u8;
    bytes[10] = ((values[1] >> 28) & 0xff) as u8;
    bytes[11] = ((values[1] >> 20) & 0xff) as u8;
    bytes[12] = ((values[1] >> 12) & 0xff) as u8;
    bytes[13] = ((values[1] >> 4) & 0xff) as u8;
    bytes[14] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 54) & 0xf)) as u8;
    bytes[15] = ((values[2] >> 46) & 0xff) as u8;
    bytes[16] = ((values[2] >> 38) & 0xff) as u8;
    bytes[17] = ((values[2] >> 30) & 0xff) as u8;
    bytes[18] = ((values[2] >> 22) & 0xff) as u8;
    bytes[19] = ((values[2] >> 14) & 0xff) as u8;
    bytes[20] = ((values[2] >> 6) & 0xff) as u8;
    bytes[21] = ((((values[2]) & 0x3f) << 2) | ((values[3] >> 56) & 0x3)) as u8;
    bytes[22] = ((values[3] >> 48) & 0xff) as u8;
    bytes[23] = ((values[3] >> 40) & 0xff) as u8;
    bytes[24] = ((values[3] >> 32) & 0xff) as u8;
    bytes[25] = ((values[3] >> 24) & 0xff) as u8;
    bytes[26] = ((values[3] >> 16) & 0xff) as u8;
    bytes[27] = ((values[3] >> 8) & 0xff) as u8;
    bytes[28] = ((values[3]) & 0xff) as u8;
    bytes[29] = ((values[4] >> 50) & 0xff) as u8;
    bytes[30] = ((values[4] >> 42) & 0xff) as u8;
    bytes[31] = ((values[4] >> 34) & 0xff) as u8;
    bytes[32] = ((values[4] >> 26) & 0xff) as u8;
    bytes[33] = ((values[4] >> 18) & 0xff) as u8;
    bytes[34] = ((values[4] >> 10) & 0xff) as u8;
    bytes[35] = ((values[4] >> 2) & 0xff) as u8;
    bytes[36] = ((((values[4]) & 0x3) << 6) | ((values[5] >> 52) & 0x3f)) as u8;
    bytes[37] = ((values[5] >> 44) & 0xff) as u8;
    bytes[38] = ((values[5] >> 36) & 0xff) as u8;
    bytes[39] = ((values[5] >> 28) & 0xff) as u8;
    bytes[40] = ((values[5] >> 20) & 0xff) as u8;
    bytes[41] = ((values[5] >> 12) & 0xff) as u8;
    bytes[42] = ((values[5] >> 4) & 0xff) as u8;
    bytes[43] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 54) & 0xf)) as u8;
    bytes[44] = ((values[6] >> 46) & 0xff) as u8;
    bytes[45] = ((values[6] >> 38) & 0xff) as u8;
    bytes[46] = ((values[6] >> 30) & 0xff) as u8;
    bytes[47] = ((values[6] >> 22) & 0xff) as u8;
    bytes[48] = ((values[6] >> 14) & 0xff) as u8;
    bytes[49] = ((values[6] >> 6) & 0xff) as u8;
    bytes[50] = ((((values[6]) & 0x3f) << 2) | ((values[7] >> 56) & 0x3)) as u8;
    bytes[51] = ((values[7] >> 48) & 0xff) as u8;
    bytes[52] = ((values[7] >> 40) & 0xff) as u8;
    bytes[53] = ((values[7] >> 32) & 0xff) as u8;
    bytes[54] = ((values[7] >> 24) & 0xff) as u8;
    bytes[55] = ((values[7] >> 16) & 0xff) as u8;
    bytes[56] = ((values[7] >> 8) & 0xff) as u8;
    bytes[57] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_59(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 59);
    bytes[0] = ((values[0] >> 51) & 0xff) as u8;
    bytes[1] = ((values[0] >> 43) & 0xff) as u8;
    bytes[2] = ((values[0] >> 35) & 0xff) as u8;
    bytes[3] = ((values[0] >> 27) & 0xff) as u8;
    bytes[4] = ((values[0] >> 19) & 0xff) as u8;
    bytes[5] = ((values[0] >> 11) & 0xff) as u8;
    bytes[6] = ((values[0] >> 3) & 0xff) as u8;
    bytes[7] = ((((values[0]) & 0x7) << 5) | ((values[1] >> 54) & 0x1f)) as u8;
    bytes[8] = ((values[1] >> 46) & 0xff) as u8;
    bytes[9] = ((values[1] >> 38) & 0xff) as u8;
    bytes[10] = ((values[1] >> 30) & 0xff) as u8;
    bytes[11] = ((values[1] >> 22) & 0xff) as u8;
    bytes[12] = ((values[1] >> 14) & 0xff) as u8;
    bytes[13] = ((values[1] >> 6) & 0xff) as u8;
    bytes[14] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 57) & 0x3)) as u8;
    bytes[15] = ((values[2] >> 49) & 0xff) as u8;
    bytes[16] = ((values[2] >> 41) & 0xff) as u8;
    bytes[17] = ((values[2] >> 33) & 0xff) as u8;
    bytes[18] = ((values[2] >> 25) & 0xff) as u8;
    bytes[19] = ((values[2] >> 17) & 0xff) as u8;
    bytes[20] = ((values[2] >> 9) & 0xff) as u8;
    bytes[21] = ((values[2] >> 1) & 0xff) as u8;
    bytes[22] = ((((values[2]) & 0x1) << 7) | ((values[3] >> 52) & 0x7f)) as u8;
    bytes[23] = ((values[3] >> 44) & 0xff) as u8;
    bytes[24] = ((values[3] >> 36) & 0xff) as u8;
    bytes[25] = ((values[3] >> 28) & 0xff) as u8;
    bytes[26] = ((values[3] >> 20) & 0xff) as u8;
    bytes[27] = ((values[3] >> 12) & 0xff) as u8;
    bytes[28] = ((values[3] >> 4) & 0xff) as u8;
    bytes[29] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 55) & 0xf)) as u8;
    bytes[30] = ((values[4] >> 47) & 0xff) as u8;
    bytes[31] = ((values[4] >> 39) & 0xff) as u8;
    bytes[32] = ((values[4] >> 31) & 0xff) as u8;
    bytes[33] = ((values[4] >> 23) & 0xff) as u8;
    bytes[34] = ((values[4] >> 15) & 0xff) as u8;
    bytes[35] = ((values[4] >> 7) & 0xff) as u8;
    bytes[36] = ((((values[4]) & 0x7f) << 1) | ((values[5] >> 58) & 0x1)) as u8;
    bytes[37] = ((values[5] >> 50) & 0xff) as u8;
    bytes[38] = ((values[5] >> 42) & 0xff) as u8;
    bytes[39] = ((values[5] >> 34) & 0xff) as u8;
    bytes[40] = ((values[5] >> 26) & 0xff) as u8;
    bytes[41] = ((values[5] >> 18) & 0xff) as u8;
    bytes[42] = ((values[5] >> 10) & 0xff) as u8;
    bytes[43] = ((values[5] >> 2) & 0xff) as u8;
    bytes[44] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 53) & 0x3f)) as u8;
    bytes[45] = ((values[6] >> 45) & 0xff) as u8;
    bytes[46] = ((values[6] >> 37) & 0xff) as u8;
    bytes[47] = ((values[6] >> 29) & 0xff) as u8;
    bytes[48] = ((values[6] >> 21) & 0xff) as u8;
    bytes[49] = ((values[6] >> 13) & 0xff) as u8;
    bytes[50] = ((values[6] >> 5) & 0xff) as u8;
    bytes[51] = ((((values[6]) & 0x1f) << 3) | ((values[7] >> 56) & 0x7)) as u8;
    bytes[52] = ((values[7] >> 48) & 0xff) as u8;
    bytes[53] = ((values[7] >> 40) & 0xff) as u8;
    bytes[54] = ((values[7] >> 32) & 0xff) as u8;
    bytes[55] = ((values[7] >> 24) & 0xff) as u8;
    bytes[56] = ((values[7] >> 16) & 0xff) as u8;
    bytes[57] = ((values[7] >> 8) & 0xff) as u8;
    bytes[58] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_60(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 60);
    bytes[0] = ((values[0] >> 52) & 0xff) as u8;
    bytes[1] = ((values[0] >> 44) & 0xff) as u8;
    bytes[2] = ((values[0] >> 36) & 0xff) as u8;
    bytes[3] = ((values[0] >> 28) & 0xff) as u8;
    bytes[4] = ((values[0] >> 20) & 0xff) as u8;
    bytes[5] = ((values[0] >> 12) & 0xff) as u8;
    bytes[6] = ((values[0] >> 4) & 0xff) as u8;
    bytes[7] = ((((values[0]) & 0xf) << 4) | ((values[1] >> 56) & 0xf)) as u8;
    bytes[8] = ((values[1] >> 48) & 0xff) as u8;
    bytes[9] = ((values[1] >> 40) & 0xff) as u8;
    bytes[10] = ((values[1] >> 32) & 0xff) as u8;
    bytes[11] = ((values[1] >> 24) & 0xff) as u8;
    bytes[12] = ((values[1] >> 16) & 0xff) as u8;
    bytes[13] = ((values[1] >> 8) & 0xff) as u8;
    bytes[14] = ((values[1]) & 0xff) as u8;
    bytes[15] = ((values[2] >> 52) & 0xff) as u8;
    bytes[16] = ((values[2] >> 44) & 0xff) as u8;
    bytes[17] = ((values[2] >> 36) & 0xff) as u8;
    bytes[18] = ((values[2] >> 28) & 0xff) as u8;
    bytes[19] = ((values[2] >> 20) & 0xff) as u8;
    bytes[20] = ((values[2] >> 12) & 0xff) as u8;
    bytes[21] = ((values[2] >> 4) & 0xff) as u8;
    bytes[22] = ((((values[2]) & 0xf) << 4) | ((values[3] >> 56) & 0xf)) as u8;
    bytes[23] = ((values[3] >> 48) & 0xff) as u8;
    bytes[24] = ((values[3] >> 40) & 0xff) as u8;
    bytes[25] = ((values[3] >> 32) & 0xff) as u8;
    bytes[26] = ((values[3] >> 24) & 0xff) as u8;
    bytes[27] = ((values[3] >> 16) & 0xff) as u8;
    bytes[28] = ((values[3] >> 8) & 0xff) as u8;
    bytes[29] = ((values[3]) & 0xff) as u8;
    bytes[30] = ((values[4] >> 52) & 0xff) as u8;
    bytes[31] = ((values[4] >> 44) & 0xff) as u8;
    bytes[32] = ((values[4] >> 36) & 0xff) as u8;
    bytes[33] = ((values[4] >> 28) & 0xff) as u8;
    bytes[34] = ((values[4] >> 20) & 0xff) as u8;
    bytes[35] = ((values[4] >> 12) & 0xff) as u8;
    bytes[36] = ((values[4] >> 4) & 0xff) as u8;
    bytes[37] = ((((values[4]) & 0xf) << 4) | ((values[5] >> 56) & 0xf)) as u8;
    bytes[38] = ((values[5] >> 48) & 0xff) as u8;
    bytes[39] = ((values[5] >> 40) & 0xff) as u8;
    bytes[40] = ((values[5] >> 32) & 0xff) as u8;
    bytes[41] = ((values[5] >> 24) & 0xff) as u8;
    bytes[42] = ((values[5] >> 16) & 0xff) as u8;
    bytes[43] = ((values[5] >> 8) & 0xff) as u8;
    bytes[44] = ((values[5]) & 0xff) as u8;
    bytes[45] = ((values[6] >> 52) & 0xff) as u8;
    bytes[46] = ((values[6] >> 44) & 0xff) as u8;
    bytes[47] = ((values[6] >> 36) & 0xff) as u8;
    bytes[48] = ((values[6] >> 28) & 0xff) as u8;
    bytes[49] = ((values[6] >> 20) & 0xff) as u8;
    bytes[50] = ((values[6] >> 12) & 0xff) as u8;
    bytes[51] = ((values[6] >> 4) & 0xff) as u8;
    bytes[52] = ((((values[6]) & 0xf) << 4) | ((values[7] >> 56) & 0xf)) as u8;
    bytes[53] = ((values[7] >> 48) & 0xff) as u8;
    bytes[54] = ((values[7] >> 40) & 0xff) as u8;
    bytes[55] = ((values[7] >> 32) & 0xff) as u8;
    bytes[56] = ((values[7] >> 24) & 0xff) as u8;
    bytes[57] = ((values[7] >> 16) & 0xff) as u8;
    bytes[58] = ((values[7] >> 8) & 0xff) as u8;
    bytes[59] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_61(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 61);
    bytes[0] = ((values[0] >> 53) & 0xff) as u8;
    bytes[1] = ((values[0] >> 45) & 0xff) as u8;
    bytes[2] = ((values[0] >> 37) & 0xff) as u8;
    bytes[3] = ((values[0] >> 29) & 0xff) as u8;
    bytes[4] = ((values[0] >> 21) & 0xff) as u8;
    bytes[5] = ((values[0] >> 13) & 0xff) as u8;
    bytes[6] = ((values[0] >> 5) & 0xff) as u8;
    bytes[7] = ((((values[0]) & 0x1f) << 3) | ((values[1] >> 58) & 0x7)) as u8;
    bytes[8] = ((values[1] >> 50) & 0xff) as u8;
    bytes[9] = ((values[1] >> 42) & 0xff) as u8;
    bytes[10] = ((values[1] >> 34) & 0xff) as u8;
    bytes[11] = ((values[1] >> 26) & 0xff) as u8;
    bytes[12] = ((values[1] >> 18) & 0xff) as u8;
    bytes[13] = ((values[1] >> 10) & 0xff) as u8;
    bytes[14] = ((values[1] >> 2) & 0xff) as u8;
    bytes[15] = ((((values[1]) & 0x3) << 6) | ((values[2] >> 55) & 0x3f)) as u8;
    bytes[16] = ((values[2] >> 47) & 0xff) as u8;
    bytes[17] = ((values[2] >> 39) & 0xff) as u8;
    bytes[18] = ((values[2] >> 31) & 0xff) as u8;
    bytes[19] = ((values[2] >> 23) & 0xff) as u8;
    bytes[20] = ((values[2] >> 15) & 0xff) as u8;
    bytes[21] = ((values[2] >> 7) & 0xff) as u8;
    bytes[22] = ((((values[2]) & 0x7f) << 1) | ((values[3] >> 60) & 0x1)) as u8;
    bytes[23] = ((values[3] >> 52) & 0xff) as u8;
    bytes[24] = ((values[3] >> 44) & 0xff) as u8;
    bytes[25] = ((values[3] >> 36) & 0xff) as u8;
    bytes[26] = ((values[3] >> 28) & 0xff) as u8;
    bytes[27] = ((values[3] >> 20) & 0xff) as u8;
    bytes[28] = ((values[3] >> 12) & 0xff) as u8;
    bytes[29] = ((values[3] >> 4) & 0xff) as u8;
    bytes[30] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 57) & 0xf)) as u8;
    bytes[31] = ((values[4] >> 49) & 0xff) as u8;
    bytes[32] = ((values[4] >> 41) & 0xff) as u8;
    bytes[33] = ((values[4] >> 33) & 0xff) as u8;
    bytes[34] = ((values[4] >> 25) & 0xff) as u8;
    bytes[35] = ((values[4] >> 17) & 0xff) as u8;
    bytes[36] = ((values[4] >> 9) & 0xff) as u8;
    bytes[37] = ((values[4] >> 1) & 0xff) as u8;
    bytes[38] = ((((values[4]) & 0x1) << 7) | ((values[5] >> 54) & 0x7f)) as u8;
    bytes[39] = ((values[5] >> 46) & 0xff) as u8;
    bytes[40] = ((values[5] >> 38) & 0xff) as u8;
    bytes[41] = ((values[5] >> 30) & 0xff) as u8;
    bytes[42] = ((values[5] >> 22) & 0xff) as u8;
    bytes[43] = ((values[5] >> 14) & 0xff) as u8;
    bytes[44] = ((values[5] >> 6) & 0xff) as u8;
    bytes[45] = ((((values[5]) & 0x3f) << 2) | ((values[6] >> 59) & 0x3)) as u8;
    bytes[46] = ((values[6] >> 51) & 0xff) as u8;
    bytes[47] = ((values[6] >> 43) & 0xff) as u8;
    bytes[48] = ((values[6] >> 35) & 0xff) as u8;
    bytes[49] = ((values[6] >> 27) & 0xff) as u8;
    bytes[50] = ((values[6] >> 19) & 0xff) as u8;
    bytes[51] = ((values[6] >> 11) & 0xff) as u8;
    bytes[52] = ((values[6] >> 3) & 0xff) as u8;
    bytes[53] = ((((values[6]) & 0x7) << 5) | ((values[7] >> 56) & 0x1f)) as u8;
    bytes[54] = ((values[7] >> 48) & 0xff) as u8;
    bytes[55] = ((values[7] >> 40) & 0xff) as u8;
    bytes[56] = ((values[7] >> 32) & 0xff) as u8;
    bytes[57] = ((values[7] >> 24) & 0xff) as u8;
    bytes[58] = ((values[7] >> 16) & 0xff) as u8;
    bytes[59] = ((values[7] >> 8) & 0xff) as u8;
    bytes[60] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_62(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 62);
    bytes[0] = ((values[0] >> 54) & 0xff) as u8;
    bytes[1] = ((values[0] >> 46) & 0xff) as u8;
    bytes[2] = ((values[0] >> 38) & 0xff) as u8;
    bytes[3] = ((values[0] >> 30) & 0xff) as u8;
    bytes[4] = ((values[0] >> 22) & 0xff) as u8;
    bytes[5] = ((values[0] >> 14) & 0xff) as u8;
    bytes[6] = ((values[0] >> 6) & 0xff) as u8;
    bytes[7] = ((((values[0]) & 0x3f) << 2) | ((values[1] >> 60) & 0x3)) as u8;
    bytes[8] = ((values[1] >> 52) & 0xff) as u8;
    bytes[9] = ((values[1] >> 44) & 0xff) as u8;
    bytes[10] = ((values[1] >> 36) & 0xff) as u8;
    bytes[11] = ((values[1] >> 28) & 0xff) as u8;
    bytes[12] = ((values[1] >> 20) & 0xff) as u8;
    bytes[13] = ((values[1] >> 12) & 0xff) as u8;
    bytes[14] = ((values[1] >> 4) & 0xff) as u8;
    bytes[15] = ((((values[1]) & 0xf) << 4) | ((values[2] >> 58) & 0xf)) as u8;
    bytes[16] = ((values[2] >> 50) & 0xff) as u8;
    bytes[17] = ((values[2] >> 42) & 0xff) as u8;
    bytes[18] = ((values[2] >> 34) & 0xff) as u8;
    bytes[19] = ((values[2] >> 26) & 0xff) as u8;
    bytes[20] = ((values[2] >> 18) & 0xff) as u8;
    bytes[21] = ((values[2] >> 10) & 0xff) as u8;
    bytes[22] = ((values[2] >> 2) & 0xff) as u8;
    bytes[23] = ((((values[2]) & 0x3) << 6) | ((values[3] >> 56) & 0x3f)) as u8;
    bytes[24] = ((values[3] >> 48) & 0xff) as u8;
    bytes[25] = ((values[3] >> 40) & 0xff) as u8;
    bytes[26] = ((values[3] >> 32) & 0xff) as u8;
    bytes[27] = ((values[3] >> 24) & 0xff) as u8;
    bytes[28] = ((values[3] >> 16) & 0xff) as u8;
    bytes[29] = ((values[3] >> 8) & 0xff) as u8;
    bytes[30] = ((values[3]) & 0xff) as u8;
    bytes[31] = ((values[4] >> 54) & 0xff) as u8;
    bytes[32] = ((values[4] >> 46) & 0xff) as u8;
    bytes[33] = ((values[4] >> 38) & 0xff) as u8;
    bytes[34] = ((values[4] >> 30) & 0xff) as u8;
    bytes[35] = ((values[4] >> 22) & 0xff) as u8;
    bytes[36] = ((values[4] >> 14) & 0xff) as u8;
    bytes[37] = ((values[4] >> 6) & 0xff) as u8;
    bytes[38] = ((((values[4]) & 0x3f) << 2) | ((values[5] >> 60) & 0x3)) as u8;
    bytes[39] = ((values[5] >> 52) & 0xff) as u8;
    bytes[40] = ((values[5] >> 44) & 0xff) as u8;
    bytes[41] = ((values[5] >> 36) & 0xff) as u8;
    bytes[42] = ((values[5] >> 28) & 0xff) as u8;
    bytes[43] = ((values[5] >> 20) & 0xff) as u8;
    bytes[44] = ((values[5] >> 12) & 0xff) as u8;
    bytes[45] = ((values[5] >> 4) & 0xff) as u8;
    bytes[46] = ((((values[5]) & 0xf) << 4) | ((values[6] >> 58) & 0xf)) as u8;
    bytes[47] = ((values[6] >> 50) & 0xff) as u8;
    bytes[48] = ((values[6] >> 42) & 0xff) as u8;
    bytes[49] = ((values[6] >> 34) & 0xff) as u8;
    bytes[50] = ((values[6] >> 26) & 0xff) as u8;
    bytes[51] = ((values[6] >> 18) & 0xff) as u8;
    bytes[52] = ((values[6] >> 10) & 0xff) as u8;
    bytes[53] = ((values[6] >> 2) & 0xff) as u8;
    bytes[54] = ((((values[6]) & 0x3) << 6) | ((values[7] >> 56) & 0x3f)) as u8;
    bytes[55] = ((values[7] >> 48) & 0xff) as u8;
    bytes[56] = ((values[7] >> 40) & 0xff) as u8;
    bytes[57] = ((values[7] >> 32) & 0xff) as u8;
    bytes[58] = ((values[7] >> 24) & 0xff) as u8;
    bytes[59] = ((values[7] >> 16) & 0xff) as u8;
    bytes[60] = ((values[7] >> 8) & 0xff) as u8;
    bytes[61] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn pack_bits_63(values: &[u64], bytes: &mut [u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 63);
    bytes[0] = ((values[0] >> 55) & 0xff) as u8;
    bytes[1] = ((values[0] >> 47) & 0xff) as u8;
    bytes[2] = ((values[0] >> 39) & 0xff) as u8;
    bytes[3] = ((values[0] >> 31) & 0xff) as u8;
    bytes[4] = ((values[0] >> 23) & 0xff) as u8;
    bytes[5] = ((values[0] >> 15) & 0xff) as u8;
    bytes[6] = ((values[0] >> 7) & 0xff) as u8;
    bytes[7] = ((((values[0]) & 0x7f) << 1) | ((values[1] >> 62) & 0x1)) as u8;
    bytes[8] = ((values[1] >> 54) & 0xff) as u8;
    bytes[9] = ((values[1] >> 46) & 0xff) as u8;
    bytes[10] = ((values[1] >> 38) & 0xff) as u8;
    bytes[11] = ((values[1] >> 30) & 0xff) as u8;
    bytes[12] = ((values[1] >> 22) & 0xff) as u8;
    bytes[13] = ((values[1] >> 14) & 0xff) as u8;
    bytes[14] = ((values[1] >> 6) & 0xff) as u8;
    bytes[15] = ((((values[1]) & 0x3f) << 2) | ((values[2] >> 61) & 0x3)) as u8;
    bytes[16] = ((values[2] >> 53) & 0xff) as u8;
    bytes[17] = ((values[2] >> 45) & 0xff) as u8;
    bytes[18] = ((values[2] >> 37) & 0xff) as u8;
    bytes[19] = ((values[2] >> 29) & 0xff) as u8;
    bytes[20] = ((values[2] >> 21) & 0xff) as u8;
    bytes[21] = ((values[2] >> 13) & 0xff) as u8;
    bytes[22] = ((values[2] >> 5) & 0xff) as u8;
    bytes[23] = ((((values[2]) & 0x1f) << 3) | ((values[3] >> 60) & 0x7)) as u8;
    bytes[24] = ((values[3] >> 52) & 0xff) as u8;
    bytes[25] = ((values[3] >> 44) & 0xff) as u8;
    bytes[26] = ((values[3] >> 36) & 0xff) as u8;
    bytes[27] = ((values[3] >> 28) & 0xff) as u8;
    bytes[28] = ((values[3] >> 20) & 0xff) as u8;
    bytes[29] = ((values[3] >> 12) & 0xff) as u8;
    bytes[30] = ((values[3] >> 4) & 0xff) as u8;
    bytes[31] = ((((values[3]) & 0xf) << 4) | ((values[4] >> 59) & 0xf)) as u8;
    bytes[32] = ((values[4] >> 51) & 0xff) as u8;
    bytes[33] = ((values[4] >> 43) & 0xff) as u8;
    bytes[34] = ((values[4] >> 35) & 0xff) as u8;
    bytes[35] = ((values[4] >> 27) & 0xff) as u8;
    bytes[36] = ((values[4] >> 19) & 0xff) as u8;
    bytes[37] = ((values[4] >> 11) & 0xff) as u8;
    bytes[38] = ((values[4] >> 3) & 0xff) as u8;
    bytes[39] = ((((values[4]) & 0x7) << 5) | ((values[5] >> 58) & 0x1f)) as u8;
    bytes[40] = ((values[5] >> 50) & 0xff) as u8;
    bytes[41] = ((values[5] >> 42) & 0xff) as u8;
    bytes[42] = ((values[5] >> 34) & 0xff) as u8;
    bytes[43] = ((values[5] >> 26) & 0xff) as u8;
    bytes[44] = ((values[5] >> 18) & 0xff) as u8;
    bytes[45] = ((values[5] >> 10) & 0xff) as u8;
    bytes[46] = ((values[5] >> 2) & 0xff) as u8;
    bytes[47] = ((((values[5]) & 0x3) << 6) | ((values[6] >> 57) & 0x3f)) as u8;
    bytes[48] = ((values[6] >> 49) & 0xff) as u8;
    bytes[49] = ((values[6] >> 41) & 0xff) as u8;
    bytes[50] = ((values[6] >> 33) & 0xff) as u8;
    bytes[51] = ((values[6] >> 25) & 0xff) as u8;
    bytes[52] = ((values[6] >> 17) & 0xff) as u8;
    bytes[53] = ((values[6] >> 9) & 0xff) as u8;
    bytes[54] = ((values[6] >> 1) & 0xff) as u8;
    bytes[55] = ((((values[6]) & 0x1) << 7) | ((values[7] >> 56) & 0x7f)) as u8;
    bytes[56] = ((values[7] >> 48) & 0xff) as u8;
    bytes[57] = ((values[7] >> 40) & 0xff) as u8;
    bytes[58] = ((values[7] >> 32) & 0xff) as u8;
    bytes[59] = ((values[7] >> 24) & 0xff) as u8;
    bytes[60] = ((values[7] >> 16) & 0xff) as u8;
    bytes[61] = ((values[7] >> 8) & 0xff) as u8;
    bytes[62] = ((values[7]) & 0xff) as u8;
}

#[inline]
fn unpack_bits_1(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(!bytes.is_empty());
    values[0] = ((bytes[0] >> 7) & 0x1) as u64;
    values[1] = ((bytes[0] >> 6) & 0x1) as u64;
    values[2] = ((bytes[0] >> 5) & 0x1) as u64;
    values[3] = ((bytes[0] >> 4) & 0x1) as u64;
    values[4] = ((bytes[0] >> 3) & 0x1) as u64;
    values[5] = ((bytes[0] >> 2) & 0x1) as u64;
    values[6] = ((bytes[0] >> 1) & 0x1) as u64;
    values[7] = ((bytes[0]) & 0x1) as u64;
}

#[inline]
fn unpack_bits_2(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 2);
    values[0] = ((bytes[0] >> 6) & 0x3) as u64;
    values[1] = ((bytes[0] >> 4) & 0x3) as u64;
    values[2] = ((bytes[0] >> 2) & 0x3) as u64;
    values[3] = ((bytes[0]) & 0x3) as u64;
    values[4] = ((bytes[1] >> 6) & 0x3) as u64;
    values[5] = ((bytes[1] >> 4) & 0x3) as u64;
    values[6] = ((bytes[1] >> 2) & 0x3) as u64;
    values[7] = ((bytes[1]) & 0x3) as u64;
}

#[inline]
fn unpack_bits_3(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 3);
    values[0] = ((bytes[0] >> 5) & 0x7) as u64;
    values[1] = ((bytes[0] >> 2) & 0x7) as u64;
    values[2] = ((((bytes[0]) & 0x3) as u64) << 1) | (((bytes[1] >> 7) & 0x1) as u64);
    values[3] = ((bytes[1] >> 4) & 0x7) as u64;
    values[4] = ((bytes[1] >> 1) & 0x7) as u64;
    values[5] = ((((bytes[1]) & 0x1) as u64) << 2) | (((bytes[2] >> 6) & 0x3) as u64);
    values[6] = ((bytes[2] >> 3) & 0x7) as u64;
    values[7] = ((bytes[2]) & 0x7) as u64;
}

#[inline]
fn unpack_bits_4(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 4);
    values[0] = ((bytes[0] >> 4) & 0xf) as u64;
    values[1] = ((bytes[0]) & 0xf) as u64;
    values[2] = ((bytes[1] >> 4) & 0xf) as u64;
    values[3] = ((bytes[1]) & 0xf) as u64;
    values[4] = ((bytes[2] >> 4) & 0xf) as u64;
    values[5] = ((bytes[2]) & 0xf) as u64;
    values[6] = ((bytes[3] >> 4) & 0xf) as u64;
    values[7] = ((bytes[3]) & 0xf) as u64;
}

#[inline]
fn unpack_bits_5(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 5);
    values[0] = ((bytes[0] >> 3) & 0x1f) as u64;
    values[1] = ((((bytes[0]) & 0x7) as u64) << 2) | (((bytes[1] >> 6) & 0x3) as u64);
    values[2] = ((bytes[1] >> 1) & 0x1f) as u64;
    values[3] = ((((bytes[1]) & 0x1) as u64) << 4) | (((bytes[2] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[2]) & 0xf) as u64) << 1) | (((bytes[3] >> 7) & 0x1) as u64);
    values[5] = ((bytes[3] >> 2) & 0x1f) as u64;
    values[6] = ((((bytes[3]) & 0x3) as u64) << 3) | (((bytes[4] >> 5) & 0x7) as u64);
    values[7] = ((bytes[4]) & 0x1f) as u64;
}

#[inline]
fn unpack_bits_6(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 6);
    values[0] = ((bytes[0] >> 2) & 0x3f) as u64;
    values[1] = ((((bytes[0]) & 0x3) as u64) << 4) | (((bytes[1] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[1]) & 0xf) as u64) << 2) | (((bytes[2] >> 6) & 0x3) as u64);
    values[3] = ((bytes[2]) & 0x3f) as u64;
    values[4] = ((bytes[3] >> 2) & 0x3f) as u64;
    values[5] = ((((bytes[3]) & 0x3) as u64) << 4) | (((bytes[4] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[4]) & 0xf) as u64) << 2) | (((bytes[5] >> 6) & 0x3) as u64);
    values[7] = ((bytes[5]) & 0x3f) as u64;
}

#[inline]
fn unpack_bits_7(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 7);
    values[0] = ((bytes[0] >> 1) & 0x7f) as u64;
    values[1] = ((((bytes[0]) & 0x1) as u64) << 6) | (((bytes[1] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[1]) & 0x3) as u64) << 5) | (((bytes[2] >> 3) & 0x1f) as u64);
    values[3] = ((((bytes[2]) & 0x7) as u64) << 4) | (((bytes[3] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[3]) & 0xf) as u64) << 3) | (((bytes[4] >> 5) & 0x7) as u64);
    values[5] = ((((bytes[4]) & 0x1f) as u64) << 2) | (((bytes[5] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[5]) & 0x3f) as u64) << 1) | (((bytes[6] >> 7) & 0x1) as u64);
    values[7] = ((bytes[6]) & 0x7f) as u64;
}

#[inline]
fn unpack_bits_8(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 8);
    values[0] = (bytes[0]) as u64;
    values[1] = (bytes[1]) as u64;
    values[2] = (bytes[2]) as u64;
    values[3] = (bytes[3]) as u64;
    values[4] = (bytes[4]) as u64;
    values[5] = (bytes[5]) as u64;
    values[6] = (bytes[6]) as u64;
    values[7] = (bytes[7]) as u64;
}

#[inline]
fn unpack_bits_9(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 9);
    values[0] = (((bytes[0]) as u64) << 1) | (((bytes[1] >> 7) & 0x1) as u64);
    values[1] = ((((bytes[1]) & 0x7f) as u64) << 2) | (((bytes[2] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[2]) & 0x3f) as u64) << 3) | (((bytes[3] >> 5) & 0x7) as u64);
    values[3] = ((((bytes[3]) & 0x1f) as u64) << 4) | (((bytes[4] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[4]) & 0xf) as u64) << 5) | (((bytes[5] >> 3) & 0x1f) as u64);
    values[5] = ((((bytes[5]) & 0x7) as u64) << 6) | (((bytes[6] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[6]) & 0x3) as u64) << 7) | (((bytes[7] >> 1) & 0x7f) as u64);
    values[7] = ((((bytes[7]) & 0x1) as u64) << 8) | ((bytes[8]) as u64);
}

#[inline]
fn unpack_bits_10(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 10);
    values[0] = (((bytes[0]) as u64) << 2) | (((bytes[1] >> 6) & 0x3) as u64);
    values[1] = ((((bytes[1]) & 0x3f) as u64) << 4) | (((bytes[2] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[2]) & 0xf) as u64) << 6) | (((bytes[3] >> 2) & 0x3f) as u64);
    values[3] = ((((bytes[3]) & 0x3) as u64) << 8) | ((bytes[4]) as u64);
    values[4] = (((bytes[5]) as u64) << 2) | (((bytes[6] >> 6) & 0x3) as u64);
    values[5] = ((((bytes[6]) & 0x3f) as u64) << 4) | (((bytes[7] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[7]) & 0xf) as u64) << 6) | (((bytes[8] >> 2) & 0x3f) as u64);
    values[7] = ((((bytes[8]) & 0x3) as u64) << 8) | ((bytes[9]) as u64);
}

#[inline]
fn unpack_bits_11(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 11);
    values[0] = (((bytes[0]) as u64) << 3) | (((bytes[1] >> 5) & 0x7) as u64);
    values[1] = ((((bytes[1]) & 0x1f) as u64) << 6) | (((bytes[2] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[2]) & 0x3) as u64) << 9)
        | (((bytes[3]) as u64) << 1)
        | (((bytes[4] >> 7) & 0x1) as u64);
    values[3] = ((((bytes[4]) & 0x7f) as u64) << 4) | (((bytes[5] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[5]) & 0xf) as u64) << 7) | (((bytes[6] >> 1) & 0x7f) as u64);
    values[5] = ((((bytes[6]) & 0x1) as u64) << 10)
        | (((bytes[7]) as u64) << 2)
        | (((bytes[8] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[8]) & 0x3f) as u64) << 5) | (((bytes[9] >> 3) & 0x1f) as u64);
    values[7] = ((((bytes[9]) & 0x7) as u64) << 8) | ((bytes[10]) as u64);
}

#[inline]
fn unpack_bits_12(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 12);
    values[0] = (((bytes[0]) as u64) << 4) | (((bytes[1] >> 4) & 0xf) as u64);
    values[1] = ((((bytes[1]) & 0xf) as u64) << 8) | ((bytes[2]) as u64);
    values[2] = (((bytes[3]) as u64) << 4) | (((bytes[4] >> 4) & 0xf) as u64);
    values[3] = ((((bytes[4]) & 0xf) as u64) << 8) | ((bytes[5]) as u64);
    values[4] = (((bytes[6]) as u64) << 4) | (((bytes[7] >> 4) & 0xf) as u64);
    values[5] = ((((bytes[7]) & 0xf) as u64) << 8) | ((bytes[8]) as u64);
    values[6] = (((bytes[9]) as u64) << 4) | (((bytes[10] >> 4) & 0xf) as u64);
    values[7] = ((((bytes[10]) & 0xf) as u64) << 8) | ((bytes[11]) as u64);
}

#[inline]
fn unpack_bits_13(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 13);
    values[0] = (((bytes[0]) as u64) << 5) | (((bytes[1] >> 3) & 0x1f) as u64);
    values[1] = ((((bytes[1]) & 0x7) as u64) << 10)
        | (((bytes[2]) as u64) << 2)
        | (((bytes[3] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[3]) & 0x3f) as u64) << 7) | (((bytes[4] >> 1) & 0x7f) as u64);
    values[3] = ((((bytes[4]) & 0x1) as u64) << 12)
        | (((bytes[5]) as u64) << 4)
        | (((bytes[6] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[6]) & 0xf) as u64) << 9)
        | (((bytes[7]) as u64) << 1)
        | (((bytes[8] >> 7) & 0x1) as u64);
    values[5] = ((((bytes[8]) & 0x7f) as u64) << 6) | (((bytes[9] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[9]) & 0x3) as u64) << 11)
        | (((bytes[10]) as u64) << 3)
        | (((bytes[11] >> 5) & 0x7) as u64);
    values[7] = ((((bytes[11]) & 0x1f) as u64) << 8) | ((bytes[12]) as u64);
}

#[inline]
fn unpack_bits_14(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 14);
    values[0] = (((bytes[0]) as u64) << 6) | (((bytes[1] >> 2) & 0x3f) as u64);
    values[1] = ((((bytes[1]) & 0x3) as u64) << 12)
        | (((bytes[2]) as u64) << 4)
        | (((bytes[3] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[3]) & 0xf) as u64) << 10)
        | (((bytes[4]) as u64) << 2)
        | (((bytes[5] >> 6) & 0x3) as u64);
    values[3] = ((((bytes[5]) & 0x3f) as u64) << 8) | ((bytes[6]) as u64);
    values[4] = (((bytes[7]) as u64) << 6) | (((bytes[8] >> 2) & 0x3f) as u64);
    values[5] = ((((bytes[8]) & 0x3) as u64) << 12)
        | (((bytes[9]) as u64) << 4)
        | (((bytes[10] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[10]) & 0xf) as u64) << 10)
        | (((bytes[11]) as u64) << 2)
        | (((bytes[12] >> 6) & 0x3) as u64);
    values[7] = ((((bytes[12]) & 0x3f) as u64) << 8) | ((bytes[13]) as u64);
}

#[inline]
fn unpack_bits_15(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 15);
    values[0] = (((bytes[0]) as u64) << 7) | (((bytes[1] >> 1) & 0x7f) as u64);
    values[1] = ((((bytes[1]) & 0x1) as u64) << 14)
        | (((bytes[2]) as u64) << 6)
        | (((bytes[3] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[3]) & 0x3) as u64) << 13)
        | (((bytes[4]) as u64) << 5)
        | (((bytes[5] >> 3) & 0x1f) as u64);
    values[3] = ((((bytes[5]) & 0x7) as u64) << 12)
        | (((bytes[6]) as u64) << 4)
        | (((bytes[7] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[7]) & 0xf) as u64) << 11)
        | (((bytes[8]) as u64) << 3)
        | (((bytes[9] >> 5) & 0x7) as u64);
    values[5] = ((((bytes[9]) & 0x1f) as u64) << 10)
        | (((bytes[10]) as u64) << 2)
        | (((bytes[11] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[11]) & 0x3f) as u64) << 9)
        | (((bytes[12]) as u64) << 1)
        | (((bytes[13] >> 7) & 0x1) as u64);
    values[7] = ((((bytes[13]) & 0x7f) as u64) << 8) | ((bytes[14]) as u64);
}

#[inline]
fn unpack_bits_16(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 16);
    values[0] = (((bytes[0]) as u64) << 8) | ((bytes[1]) as u64);
    values[1] = (((bytes[2]) as u64) << 8) | ((bytes[3]) as u64);
    values[2] = (((bytes[4]) as u64) << 8) | ((bytes[5]) as u64);
    values[3] = (((bytes[6]) as u64) << 8) | ((bytes[7]) as u64);
    values[4] = (((bytes[8]) as u64) << 8) | ((bytes[9]) as u64);
    values[5] = (((bytes[10]) as u64) << 8) | ((bytes[11]) as u64);
    values[6] = (((bytes[12]) as u64) << 8) | ((bytes[13]) as u64);
    values[7] = (((bytes[14]) as u64) << 8) | ((bytes[15]) as u64);
}

#[inline]
fn unpack_bits_17(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 17);
    values[0] =
        (((bytes[0]) as u64) << 9) | (((bytes[1]) as u64) << 1) | (((bytes[2] >> 7) & 0x1) as u64);
    values[1] = ((((bytes[2]) & 0x7f) as u64) << 10)
        | (((bytes[3]) as u64) << 2)
        | (((bytes[4] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[4]) & 0x3f) as u64) << 11)
        | (((bytes[5]) as u64) << 3)
        | (((bytes[6] >> 5) & 0x7) as u64);
    values[3] = ((((bytes[6]) & 0x1f) as u64) << 12)
        | (((bytes[7]) as u64) << 4)
        | (((bytes[8] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[8]) & 0xf) as u64) << 13)
        | (((bytes[9]) as u64) << 5)
        | (((bytes[10] >> 3) & 0x1f) as u64);
    values[5] = ((((bytes[10]) & 0x7) as u64) << 14)
        | (((bytes[11]) as u64) << 6)
        | (((bytes[12] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[12]) & 0x3) as u64) << 15)
        | (((bytes[13]) as u64) << 7)
        | (((bytes[14] >> 1) & 0x7f) as u64);
    values[7] =
        ((((bytes[14]) & 0x1) as u64) << 16) | (((bytes[15]) as u64) << 8) | ((bytes[16]) as u64);
}

#[inline]
fn unpack_bits_18(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 18);
    values[0] =
        (((bytes[0]) as u64) << 10) | (((bytes[1]) as u64) << 2) | (((bytes[2] >> 6) & 0x3) as u64);
    values[1] = ((((bytes[2]) & 0x3f) as u64) << 12)
        | (((bytes[3]) as u64) << 4)
        | (((bytes[4] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[4]) & 0xf) as u64) << 14)
        | (((bytes[5]) as u64) << 6)
        | (((bytes[6] >> 2) & 0x3f) as u64);
    values[3] =
        ((((bytes[6]) & 0x3) as u64) << 16) | (((bytes[7]) as u64) << 8) | ((bytes[8]) as u64);
    values[4] = (((bytes[9]) as u64) << 10)
        | (((bytes[10]) as u64) << 2)
        | (((bytes[11] >> 6) & 0x3) as u64);
    values[5] = ((((bytes[11]) & 0x3f) as u64) << 12)
        | (((bytes[12]) as u64) << 4)
        | (((bytes[13] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[13]) & 0xf) as u64) << 14)
        | (((bytes[14]) as u64) << 6)
        | (((bytes[15] >> 2) & 0x3f) as u64);
    values[7] =
        ((((bytes[15]) & 0x3) as u64) << 16) | (((bytes[16]) as u64) << 8) | ((bytes[17]) as u64);
}

#[inline]
fn unpack_bits_19(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 19);
    values[0] =
        (((bytes[0]) as u64) << 11) | (((bytes[1]) as u64) << 3) | (((bytes[2] >> 5) & 0x7) as u64);
    values[1] = ((((bytes[2]) & 0x1f) as u64) << 14)
        | (((bytes[3]) as u64) << 6)
        | (((bytes[4] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[4]) & 0x3) as u64) << 17)
        | (((bytes[5]) as u64) << 9)
        | (((bytes[6]) as u64) << 1)
        | (((bytes[7] >> 7) & 0x1) as u64);
    values[3] = ((((bytes[7]) & 0x7f) as u64) << 12)
        | (((bytes[8]) as u64) << 4)
        | (((bytes[9] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[9]) & 0xf) as u64) << 15)
        | (((bytes[10]) as u64) << 7)
        | (((bytes[11] >> 1) & 0x7f) as u64);
    values[5] = ((((bytes[11]) & 0x1) as u64) << 18)
        | (((bytes[12]) as u64) << 10)
        | (((bytes[13]) as u64) << 2)
        | (((bytes[14] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[14]) & 0x3f) as u64) << 13)
        | (((bytes[15]) as u64) << 5)
        | (((bytes[16] >> 3) & 0x1f) as u64);
    values[7] =
        ((((bytes[16]) & 0x7) as u64) << 16) | (((bytes[17]) as u64) << 8) | ((bytes[18]) as u64);
}

#[inline]
fn unpack_bits_20(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 20);
    values[0] =
        (((bytes[0]) as u64) << 12) | (((bytes[1]) as u64) << 4) | (((bytes[2] >> 4) & 0xf) as u64);
    values[1] =
        ((((bytes[2]) & 0xf) as u64) << 16) | (((bytes[3]) as u64) << 8) | ((bytes[4]) as u64);
    values[2] =
        (((bytes[5]) as u64) << 12) | (((bytes[6]) as u64) << 4) | (((bytes[7] >> 4) & 0xf) as u64);
    values[3] =
        ((((bytes[7]) & 0xf) as u64) << 16) | (((bytes[8]) as u64) << 8) | ((bytes[9]) as u64);
    values[4] = (((bytes[10]) as u64) << 12)
        | (((bytes[11]) as u64) << 4)
        | (((bytes[12] >> 4) & 0xf) as u64);
    values[5] =
        ((((bytes[12]) & 0xf) as u64) << 16) | (((bytes[13]) as u64) << 8) | ((bytes[14]) as u64);
    values[6] = (((bytes[15]) as u64) << 12)
        | (((bytes[16]) as u64) << 4)
        | (((bytes[17] >> 4) & 0xf) as u64);
    values[7] =
        ((((bytes[17]) & 0xf) as u64) << 16) | (((bytes[18]) as u64) << 8) | ((bytes[19]) as u64);
}

#[inline]
fn unpack_bits_21(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 21);
    values[0] = (((bytes[0]) as u64) << 13)
        | (((bytes[1]) as u64) << 5)
        | (((bytes[2] >> 3) & 0x1f) as u64);
    values[1] = ((((bytes[2]) & 0x7) as u64) << 18)
        | (((bytes[3]) as u64) << 10)
        | (((bytes[4]) as u64) << 2)
        | (((bytes[5] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[5]) & 0x3f) as u64) << 15)
        | (((bytes[6]) as u64) << 7)
        | (((bytes[7] >> 1) & 0x7f) as u64);
    values[3] = ((((bytes[7]) & 0x1) as u64) << 20)
        | (((bytes[8]) as u64) << 12)
        | (((bytes[9]) as u64) << 4)
        | (((bytes[10] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[10]) & 0xf) as u64) << 17)
        | (((bytes[11]) as u64) << 9)
        | (((bytes[12]) as u64) << 1)
        | (((bytes[13] >> 7) & 0x1) as u64);
    values[5] = ((((bytes[13]) & 0x7f) as u64) << 14)
        | (((bytes[14]) as u64) << 6)
        | (((bytes[15] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[15]) & 0x3) as u64) << 19)
        | (((bytes[16]) as u64) << 11)
        | (((bytes[17]) as u64) << 3)
        | (((bytes[18] >> 5) & 0x7) as u64);
    values[7] =
        ((((bytes[18]) & 0x1f) as u64) << 16) | (((bytes[19]) as u64) << 8) | ((bytes[20]) as u64);
}

#[inline]
fn unpack_bits_22(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 22);
    values[0] = (((bytes[0]) as u64) << 14)
        | (((bytes[1]) as u64) << 6)
        | (((bytes[2] >> 2) & 0x3f) as u64);
    values[1] = ((((bytes[2]) & 0x3) as u64) << 20)
        | (((bytes[3]) as u64) << 12)
        | (((bytes[4]) as u64) << 4)
        | (((bytes[5] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[5]) & 0xf) as u64) << 18)
        | (((bytes[6]) as u64) << 10)
        | (((bytes[7]) as u64) << 2)
        | (((bytes[8] >> 6) & 0x3) as u64);
    values[3] =
        ((((bytes[8]) & 0x3f) as u64) << 16) | (((bytes[9]) as u64) << 8) | ((bytes[10]) as u64);
    values[4] = (((bytes[11]) as u64) << 14)
        | (((bytes[12]) as u64) << 6)
        | (((bytes[13] >> 2) & 0x3f) as u64);
    values[5] = ((((bytes[13]) & 0x3) as u64) << 20)
        | (((bytes[14]) as u64) << 12)
        | (((bytes[15]) as u64) << 4)
        | (((bytes[16] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[16]) & 0xf) as u64) << 18)
        | (((bytes[17]) as u64) << 10)
        | (((bytes[18]) as u64) << 2)
        | (((bytes[19] >> 6) & 0x3) as u64);
    values[7] =
        ((((bytes[19]) & 0x3f) as u64) << 16) | (((bytes[20]) as u64) << 8) | ((bytes[21]) as u64);
}

#[inline]
fn unpack_bits_23(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 23);
    values[0] = (((bytes[0]) as u64) << 15)
        | (((bytes[1]) as u64) << 7)
        | (((bytes[2] >> 1) & 0x7f) as u64);
    values[1] = ((((bytes[2]) & 0x1) as u64) << 22)
        | (((bytes[3]) as u64) << 14)
        | (((bytes[4]) as u64) << 6)
        | (((bytes[5] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[5]) & 0x3) as u64) << 21)
        | (((bytes[6]) as u64) << 13)
        | (((bytes[7]) as u64) << 5)
        | (((bytes[8] >> 3) & 0x1f) as u64);
    values[3] = ((((bytes[8]) & 0x7) as u64) << 20)
        | (((bytes[9]) as u64) << 12)
        | (((bytes[10]) as u64) << 4)
        | (((bytes[11] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[11]) & 0xf) as u64) << 19)
        | (((bytes[12]) as u64) << 11)
        | (((bytes[13]) as u64) << 3)
        | (((bytes[14] >> 5) & 0x7) as u64);
    values[5] = ((((bytes[14]) & 0x1f) as u64) << 18)
        | (((bytes[15]) as u64) << 10)
        | (((bytes[16]) as u64) << 2)
        | (((bytes[17] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[17]) & 0x3f) as u64) << 17)
        | (((bytes[18]) as u64) << 9)
        | (((bytes[19]) as u64) << 1)
        | (((bytes[20] >> 7) & 0x1) as u64);
    values[7] =
        ((((bytes[20]) & 0x7f) as u64) << 16) | (((bytes[21]) as u64) << 8) | ((bytes[22]) as u64);
}

#[inline]
fn unpack_bits_24(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 24);
    values[0] = (((bytes[0]) as u64) << 16) | (((bytes[1]) as u64) << 8) | ((bytes[2]) as u64);
    values[1] = (((bytes[3]) as u64) << 16) | (((bytes[4]) as u64) << 8) | ((bytes[5]) as u64);
    values[2] = (((bytes[6]) as u64) << 16) | (((bytes[7]) as u64) << 8) | ((bytes[8]) as u64);
    values[3] = (((bytes[9]) as u64) << 16) | (((bytes[10]) as u64) << 8) | ((bytes[11]) as u64);
    values[4] = (((bytes[12]) as u64) << 16) | (((bytes[13]) as u64) << 8) | ((bytes[14]) as u64);
    values[5] = (((bytes[15]) as u64) << 16) | (((bytes[16]) as u64) << 8) | ((bytes[17]) as u64);
    values[6] = (((bytes[18]) as u64) << 16) | (((bytes[19]) as u64) << 8) | ((bytes[20]) as u64);
    values[7] = (((bytes[21]) as u64) << 16) | (((bytes[22]) as u64) << 8) | ((bytes[23]) as u64);
}

#[inline]
fn unpack_bits_25(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 25);
    values[0] = (((bytes[0]) as u64) << 17)
        | (((bytes[1]) as u64) << 9)
        | (((bytes[2]) as u64) << 1)
        | (((bytes[3] >> 7) & 0x1) as u64);
    values[1] = ((((bytes[3]) & 0x7f) as u64) << 18)
        | (((bytes[4]) as u64) << 10)
        | (((bytes[5]) as u64) << 2)
        | (((bytes[6] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[6]) & 0x3f) as u64) << 19)
        | (((bytes[7]) as u64) << 11)
        | (((bytes[8]) as u64) << 3)
        | (((bytes[9] >> 5) & 0x7) as u64);
    values[3] = ((((bytes[9]) & 0x1f) as u64) << 20)
        | (((bytes[10]) as u64) << 12)
        | (((bytes[11]) as u64) << 4)
        | (((bytes[12] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[12]) & 0xf) as u64) << 21)
        | (((bytes[13]) as u64) << 13)
        | (((bytes[14]) as u64) << 5)
        | (((bytes[15] >> 3) & 0x1f) as u64);
    values[5] = ((((bytes[15]) & 0x7) as u64) << 22)
        | (((bytes[16]) as u64) << 14)
        | (((bytes[17]) as u64) << 6)
        | (((bytes[18] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[18]) & 0x3) as u64) << 23)
        | (((bytes[19]) as u64) << 15)
        | (((bytes[20]) as u64) << 7)
        | (((bytes[21] >> 1) & 0x7f) as u64);
    values[7] = ((((bytes[21]) & 0x1) as u64) << 24)
        | (((bytes[22]) as u64) << 16)
        | (((bytes[23]) as u64) << 8)
        | ((bytes[24]) as u64);
}

#[inline]
fn unpack_bits_26(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 26);
    values[0] = (((bytes[0]) as u64) << 18)
        | (((bytes[1]) as u64) << 10)
        | (((bytes[2]) as u64) << 2)
        | (((bytes[3] >> 6) & 0x3) as u64);
    values[1] = ((((bytes[3]) & 0x3f) as u64) << 20)
        | (((bytes[4]) as u64) << 12)
        | (((bytes[5]) as u64) << 4)
        | (((bytes[6] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[6]) & 0xf) as u64) << 22)
        | (((bytes[7]) as u64) << 14)
        | (((bytes[8]) as u64) << 6)
        | (((bytes[9] >> 2) & 0x3f) as u64);
    values[3] = ((((bytes[9]) & 0x3) as u64) << 24)
        | (((bytes[10]) as u64) << 16)
        | (((bytes[11]) as u64) << 8)
        | ((bytes[12]) as u64);
    values[4] = (((bytes[13]) as u64) << 18)
        | (((bytes[14]) as u64) << 10)
        | (((bytes[15]) as u64) << 2)
        | (((bytes[16] >> 6) & 0x3) as u64);
    values[5] = ((((bytes[16]) & 0x3f) as u64) << 20)
        | (((bytes[17]) as u64) << 12)
        | (((bytes[18]) as u64) << 4)
        | (((bytes[19] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[19]) & 0xf) as u64) << 22)
        | (((bytes[20]) as u64) << 14)
        | (((bytes[21]) as u64) << 6)
        | (((bytes[22] >> 2) & 0x3f) as u64);
    values[7] = ((((bytes[22]) & 0x3) as u64) << 24)
        | (((bytes[23]) as u64) << 16)
        | (((bytes[24]) as u64) << 8)
        | ((bytes[25]) as u64);
}

#[inline]
fn unpack_bits_27(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 27);
    values[0] = (((bytes[0]) as u64) << 19)
        | (((bytes[1]) as u64) << 11)
        | (((bytes[2]) as u64) << 3)
        | (((bytes[3] >> 5) & 0x7) as u64);
    values[1] = ((((bytes[3]) & 0x1f) as u64) << 22)
        | (((bytes[4]) as u64) << 14)
        | (((bytes[5]) as u64) << 6)
        | (((bytes[6] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[6]) & 0x3) as u64) << 25)
        | (((bytes[7]) as u64) << 17)
        | (((bytes[8]) as u64) << 9)
        | (((bytes[9]) as u64) << 1)
        | (((bytes[10] >> 7) & 0x1) as u64);
    values[3] = ((((bytes[10]) & 0x7f) as u64) << 20)
        | (((bytes[11]) as u64) << 12)
        | (((bytes[12]) as u64) << 4)
        | (((bytes[13] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[13]) & 0xf) as u64) << 23)
        | (((bytes[14]) as u64) << 15)
        | (((bytes[15]) as u64) << 7)
        | (((bytes[16] >> 1) & 0x7f) as u64);
    values[5] = ((((bytes[16]) & 0x1) as u64) << 26)
        | (((bytes[17]) as u64) << 18)
        | (((bytes[18]) as u64) << 10)
        | (((bytes[19]) as u64) << 2)
        | (((bytes[20] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[20]) & 0x3f) as u64) << 21)
        | (((bytes[21]) as u64) << 13)
        | (((bytes[22]) as u64) << 5)
        | (((bytes[23] >> 3) & 0x1f) as u64);
    values[7] = ((((bytes[23]) & 0x7) as u64) << 24)
        | (((bytes[24]) as u64) << 16)
        | (((bytes[25]) as u64) << 8)
        | ((bytes[26]) as u64);
}

#[inline]
fn unpack_bits_28(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 28);
    values[0] = (((bytes[0]) as u64) << 20)
        | (((bytes[1]) as u64) << 12)
        | (((bytes[2]) as u64) << 4)
        | (((bytes[3] >> 4) & 0xf) as u64);
    values[1] = ((((bytes[3]) & 0xf) as u64) << 24)
        | (((bytes[4]) as u64) << 16)
        | (((bytes[5]) as u64) << 8)
        | ((bytes[6]) as u64);
    values[2] = (((bytes[7]) as u64) << 20)
        | (((bytes[8]) as u64) << 12)
        | (((bytes[9]) as u64) << 4)
        | (((bytes[10] >> 4) & 0xf) as u64);
    values[3] = ((((bytes[10]) & 0xf) as u64) << 24)
        | (((bytes[11]) as u64) << 16)
        | (((bytes[12]) as u64) << 8)
        | ((bytes[13]) as u64);
    values[4] = (((bytes[14]) as u64) << 20)
        | (((bytes[15]) as u64) << 12)
        | (((bytes[16]) as u64) << 4)
        | (((bytes[17] >> 4) & 0xf) as u64);
    values[5] = ((((bytes[17]) & 0xf) as u64) << 24)
        | (((bytes[18]) as u64) << 16)
        | (((bytes[19]) as u64) << 8)
        | ((bytes[20]) as u64);
    values[6] = (((bytes[21]) as u64) << 20)
        | (((bytes[22]) as u64) << 12)
        | (((bytes[23]) as u64) << 4)
        | (((bytes[24] >> 4) & 0xf) as u64);
    values[7] = ((((bytes[24]) & 0xf) as u64) << 24)
        | (((bytes[25]) as u64) << 16)
        | (((bytes[26]) as u64) << 8)
        | ((bytes[27]) as u64);
}

#[inline]
fn unpack_bits_29(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 29);
    values[0] = (((bytes[0]) as u64) << 21)
        | (((bytes[1]) as u64) << 13)
        | (((bytes[2]) as u64) << 5)
        | (((bytes[3] >> 3) & 0x1f) as u64);
    values[1] = ((((bytes[3]) & 0x7) as u64) << 26)
        | (((bytes[4]) as u64) << 18)
        | (((bytes[5]) as u64) << 10)
        | (((bytes[6]) as u64) << 2)
        | (((bytes[7] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[7]) & 0x3f) as u64) << 23)
        | (((bytes[8]) as u64) << 15)
        | (((bytes[9]) as u64) << 7)
        | (((bytes[10] >> 1) & 0x7f) as u64);
    values[3] = ((((bytes[10]) & 0x1) as u64) << 28)
        | (((bytes[11]) as u64) << 20)
        | (((bytes[12]) as u64) << 12)
        | (((bytes[13]) as u64) << 4)
        | (((bytes[14] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[14]) & 0xf) as u64) << 25)
        | (((bytes[15]) as u64) << 17)
        | (((bytes[16]) as u64) << 9)
        | (((bytes[17]) as u64) << 1)
        | (((bytes[18] >> 7) & 0x1) as u64);
    values[5] = ((((bytes[18]) & 0x7f) as u64) << 22)
        | (((bytes[19]) as u64) << 14)
        | (((bytes[20]) as u64) << 6)
        | (((bytes[21] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[21]) & 0x3) as u64) << 27)
        | (((bytes[22]) as u64) << 19)
        | (((bytes[23]) as u64) << 11)
        | (((bytes[24]) as u64) << 3)
        | (((bytes[25] >> 5) & 0x7) as u64);
    values[7] = ((((bytes[25]) & 0x1f) as u64) << 24)
        | (((bytes[26]) as u64) << 16)
        | (((bytes[27]) as u64) << 8)
        | ((bytes[28]) as u64);
}

#[inline]
fn unpack_bits_30(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 30);
    values[0] = (((bytes[0]) as u64) << 22)
        | (((bytes[1]) as u64) << 14)
        | (((bytes[2]) as u64) << 6)
        | (((bytes[3] >> 2) & 0x3f) as u64);
    values[1] = ((((bytes[3]) & 0x3) as u64) << 28)
        | (((bytes[4]) as u64) << 20)
        | (((bytes[5]) as u64) << 12)
        | (((bytes[6]) as u64) << 4)
        | (((bytes[7] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[7]) & 0xf) as u64) << 26)
        | (((bytes[8]) as u64) << 18)
        | (((bytes[9]) as u64) << 10)
        | (((bytes[10]) as u64) << 2)
        | (((bytes[11] >> 6) & 0x3) as u64);
    values[3] = ((((bytes[11]) & 0x3f) as u64) << 24)
        | (((bytes[12]) as u64) << 16)
        | (((bytes[13]) as u64) << 8)
        | ((bytes[14]) as u64);
    values[4] = (((bytes[15]) as u64) << 22)
        | (((bytes[16]) as u64) << 14)
        | (((bytes[17]) as u64) << 6)
        | (((bytes[18] >> 2) & 0x3f) as u64);
    values[5] = ((((bytes[18]) & 0x3) as u64) << 28)
        | (((bytes[19]) as u64) << 20)
        | (((bytes[20]) as u64) << 12)
        | (((bytes[21]) as u64) << 4)
        | (((bytes[22] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[22]) & 0xf) as u64) << 26)
        | (((bytes[23]) as u64) << 18)
        | (((bytes[24]) as u64) << 10)
        | (((bytes[25]) as u64) << 2)
        | (((bytes[26] >> 6) & 0x3) as u64);
    values[7] = ((((bytes[26]) & 0x3f) as u64) << 24)
        | (((bytes[27]) as u64) << 16)
        | (((bytes[28]) as u64) << 8)
        | ((bytes[29]) as u64);
}

#[inline]
fn unpack_bits_31(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 31);
    values[0] = (((bytes[0]) as u64) << 23)
        | (((bytes[1]) as u64) << 15)
        | (((bytes[2]) as u64) << 7)
        | (((bytes[3] >> 1) & 0x7f) as u64);
    values[1] = ((((bytes[3]) & 0x1) as u64) << 30)
        | (((bytes[4]) as u64) << 22)
        | (((bytes[5]) as u64) << 14)
        | (((bytes[6]) as u64) << 6)
        | (((bytes[7] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[7]) & 0x3) as u64) << 29)
        | (((bytes[8]) as u64) << 21)
        | (((bytes[9]) as u64) << 13)
        | (((bytes[10]) as u64) << 5)
        | (((bytes[11] >> 3) & 0x1f) as u64);
    values[3] = ((((bytes[11]) & 0x7) as u64) << 28)
        | (((bytes[12]) as u64) << 20)
        | (((bytes[13]) as u64) << 12)
        | (((bytes[14]) as u64) << 4)
        | (((bytes[15] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[15]) & 0xf) as u64) << 27)
        | (((bytes[16]) as u64) << 19)
        | (((bytes[17]) as u64) << 11)
        | (((bytes[18]) as u64) << 3)
        | (((bytes[19] >> 5) & 0x7) as u64);
    values[5] = ((((bytes[19]) & 0x1f) as u64) << 26)
        | (((bytes[20]) as u64) << 18)
        | (((bytes[21]) as u64) << 10)
        | (((bytes[22]) as u64) << 2)
        | (((bytes[23] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[23]) & 0x3f) as u64) << 25)
        | (((bytes[24]) as u64) << 17)
        | (((bytes[25]) as u64) << 9)
        | (((bytes[26]) as u64) << 1)
        | (((bytes[27] >> 7) & 0x1) as u64);
    values[7] = ((((bytes[27]) & 0x7f) as u64) << 24)
        | (((bytes[28]) as u64) << 16)
        | (((bytes[29]) as u64) << 8)
        | ((bytes[30]) as u64);
}

#[inline]
fn unpack_bits_32(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 32);
    values[0] = (((bytes[0]) as u64) << 24)
        | (((bytes[1]) as u64) << 16)
        | (((bytes[2]) as u64) << 8)
        | ((bytes[3]) as u64);
    values[1] = (((bytes[4]) as u64) << 24)
        | (((bytes[5]) as u64) << 16)
        | (((bytes[6]) as u64) << 8)
        | ((bytes[7]) as u64);
    values[2] = (((bytes[8]) as u64) << 24)
        | (((bytes[9]) as u64) << 16)
        | (((bytes[10]) as u64) << 8)
        | ((bytes[11]) as u64);
    values[3] = (((bytes[12]) as u64) << 24)
        | (((bytes[13]) as u64) << 16)
        | (((bytes[14]) as u64) << 8)
        | ((bytes[15]) as u64);
    values[4] = (((bytes[16]) as u64) << 24)
        | (((bytes[17]) as u64) << 16)
        | (((bytes[18]) as u64) << 8)
        | ((bytes[19]) as u64);
    values[5] = (((bytes[20]) as u64) << 24)
        | (((bytes[21]) as u64) << 16)
        | (((bytes[22]) as u64) << 8)
        | ((bytes[23]) as u64);
    values[6] = (((bytes[24]) as u64) << 24)
        | (((bytes[25]) as u64) << 16)
        | (((bytes[26]) as u64) << 8)
        | ((bytes[27]) as u64);
    values[7] = (((bytes[28]) as u64) << 24)
        | (((bytes[29]) as u64) << 16)
        | (((bytes[30]) as u64) << 8)
        | ((bytes[31]) as u64);
}

#[inline]
fn unpack_bits_33(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 33);
    values[0] = (((bytes[0]) as u64) << 25)
        | (((bytes[1]) as u64) << 17)
        | (((bytes[2]) as u64) << 9)
        | (((bytes[3]) as u64) << 1)
        | (((bytes[4] >> 7) & 0x1) as u64);
    values[1] = ((((bytes[4]) & 0x7f) as u64) << 26)
        | (((bytes[5]) as u64) << 18)
        | (((bytes[6]) as u64) << 10)
        | (((bytes[7]) as u64) << 2)
        | (((bytes[8] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[8]) & 0x3f) as u64) << 27)
        | (((bytes[9]) as u64) << 19)
        | (((bytes[10]) as u64) << 11)
        | (((bytes[11]) as u64) << 3)
        | (((bytes[12] >> 5) & 0x7) as u64);
    values[3] = ((((bytes[12]) & 0x1f) as u64) << 28)
        | (((bytes[13]) as u64) << 20)
        | (((bytes[14]) as u64) << 12)
        | (((bytes[15]) as u64) << 4)
        | (((bytes[16] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[16]) & 0xf) as u64) << 29)
        | (((bytes[17]) as u64) << 21)
        | (((bytes[18]) as u64) << 13)
        | (((bytes[19]) as u64) << 5)
        | (((bytes[20] >> 3) & 0x1f) as u64);
    values[5] = ((((bytes[20]) & 0x7) as u64) << 30)
        | (((bytes[21]) as u64) << 22)
        | (((bytes[22]) as u64) << 14)
        | (((bytes[23]) as u64) << 6)
        | (((bytes[24] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[24]) & 0x3) as u64) << 31)
        | (((bytes[25]) as u64) << 23)
        | (((bytes[26]) as u64) << 15)
        | (((bytes[27]) as u64) << 7)
        | (((bytes[28] >> 1) & 0x7f) as u64);
    values[7] = ((((bytes[28]) & 0x1) as u64) << 32)
        | (((bytes[29]) as u64) << 24)
        | (((bytes[30]) as u64) << 16)
        | (((bytes[31]) as u64) << 8)
        | ((bytes[32]) as u64);
}

#[inline]
fn unpack_bits_34(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 34);
    values[0] = (((bytes[0]) as u64) << 26)
        | (((bytes[1]) as u64) << 18)
        | (((bytes[2]) as u64) << 10)
        | (((bytes[3]) as u64) << 2)
        | (((bytes[4] >> 6) & 0x3) as u64);
    values[1] = ((((bytes[4]) & 0x3f) as u64) << 28)
        | (((bytes[5]) as u64) << 20)
        | (((bytes[6]) as u64) << 12)
        | (((bytes[7]) as u64) << 4)
        | (((bytes[8] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[8]) & 0xf) as u64) << 30)
        | (((bytes[9]) as u64) << 22)
        | (((bytes[10]) as u64) << 14)
        | (((bytes[11]) as u64) << 6)
        | (((bytes[12] >> 2) & 0x3f) as u64);
    values[3] = ((((bytes[12]) & 0x3) as u64) << 32)
        | (((bytes[13]) as u64) << 24)
        | (((bytes[14]) as u64) << 16)
        | (((bytes[15]) as u64) << 8)
        | ((bytes[16]) as u64);
    values[4] = (((bytes[17]) as u64) << 26)
        | (((bytes[18]) as u64) << 18)
        | (((bytes[19]) as u64) << 10)
        | (((bytes[20]) as u64) << 2)
        | (((bytes[21] >> 6) & 0x3) as u64);
    values[5] = ((((bytes[21]) & 0x3f) as u64) << 28)
        | (((bytes[22]) as u64) << 20)
        | (((bytes[23]) as u64) << 12)
        | (((bytes[24]) as u64) << 4)
        | (((bytes[25] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[25]) & 0xf) as u64) << 30)
        | (((bytes[26]) as u64) << 22)
        | (((bytes[27]) as u64) << 14)
        | (((bytes[28]) as u64) << 6)
        | (((bytes[29] >> 2) & 0x3f) as u64);
    values[7] = ((((bytes[29]) & 0x3) as u64) << 32)
        | (((bytes[30]) as u64) << 24)
        | (((bytes[31]) as u64) << 16)
        | (((bytes[32]) as u64) << 8)
        | ((bytes[33]) as u64);
}

#[inline]
fn unpack_bits_35(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 35);
    values[0] = (((bytes[0]) as u64) << 27)
        | (((bytes[1]) as u64) << 19)
        | (((bytes[2]) as u64) << 11)
        | (((bytes[3]) as u64) << 3)
        | (((bytes[4] >> 5) & 0x7) as u64);
    values[1] = ((((bytes[4]) & 0x1f) as u64) << 30)
        | (((bytes[5]) as u64) << 22)
        | (((bytes[6]) as u64) << 14)
        | (((bytes[7]) as u64) << 6)
        | (((bytes[8] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[8]) & 0x3) as u64) << 33)
        | (((bytes[9]) as u64) << 25)
        | (((bytes[10]) as u64) << 17)
        | (((bytes[11]) as u64) << 9)
        | (((bytes[12]) as u64) << 1)
        | (((bytes[13] >> 7) & 0x1) as u64);
    values[3] = ((((bytes[13]) & 0x7f) as u64) << 28)
        | (((bytes[14]) as u64) << 20)
        | (((bytes[15]) as u64) << 12)
        | (((bytes[16]) as u64) << 4)
        | (((bytes[17] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[17]) & 0xf) as u64) << 31)
        | (((bytes[18]) as u64) << 23)
        | (((bytes[19]) as u64) << 15)
        | (((bytes[20]) as u64) << 7)
        | (((bytes[21] >> 1) & 0x7f) as u64);
    values[5] = ((((bytes[21]) & 0x1) as u64) << 34)
        | (((bytes[22]) as u64) << 26)
        | (((bytes[23]) as u64) << 18)
        | (((bytes[24]) as u64) << 10)
        | (((bytes[25]) as u64) << 2)
        | (((bytes[26] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[26]) & 0x3f) as u64) << 29)
        | (((bytes[27]) as u64) << 21)
        | (((bytes[28]) as u64) << 13)
        | (((bytes[29]) as u64) << 5)
        | (((bytes[30] >> 3) & 0x1f) as u64);
    values[7] = ((((bytes[30]) & 0x7) as u64) << 32)
        | (((bytes[31]) as u64) << 24)
        | (((bytes[32]) as u64) << 16)
        | (((bytes[33]) as u64) << 8)
        | ((bytes[34]) as u64);
}

#[inline]
fn unpack_bits_36(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 36);
    values[0] = (((bytes[0]) as u64) << 28)
        | (((bytes[1]) as u64) << 20)
        | (((bytes[2]) as u64) << 12)
        | (((bytes[3]) as u64) << 4)
        | (((bytes[4] >> 4) & 0xf) as u64);
    values[1] = ((((bytes[4]) & 0xf) as u64) << 32)
        | (((bytes[5]) as u64) << 24)
        | (((bytes[6]) as u64) << 16)
        | (((bytes[7]) as u64) << 8)
        | ((bytes[8]) as u64);
    values[2] = (((bytes[9]) as u64) << 28)
        | (((bytes[10]) as u64) << 20)
        | (((bytes[11]) as u64) << 12)
        | (((bytes[12]) as u64) << 4)
        | (((bytes[13] >> 4) & 0xf) as u64);
    values[3] = ((((bytes[13]) & 0xf) as u64) << 32)
        | (((bytes[14]) as u64) << 24)
        | (((bytes[15]) as u64) << 16)
        | (((bytes[16]) as u64) << 8)
        | ((bytes[17]) as u64);
    values[4] = (((bytes[18]) as u64) << 28)
        | (((bytes[19]) as u64) << 20)
        | (((bytes[20]) as u64) << 12)
        | (((bytes[21]) as u64) << 4)
        | (((bytes[22] >> 4) & 0xf) as u64);
    values[5] = ((((bytes[22]) & 0xf) as u64) << 32)
        | (((bytes[23]) as u64) << 24)
        | (((bytes[24]) as u64) << 16)
        | (((bytes[25]) as u64) << 8)
        | ((bytes[26]) as u64);
    values[6] = (((bytes[27]) as u64) << 28)
        | (((bytes[28]) as u64) << 20)
        | (((bytes[29]) as u64) << 12)
        | (((bytes[30]) as u64) << 4)
        | (((bytes[31] >> 4) & 0xf) as u64);
    values[7] = ((((bytes[31]) & 0xf) as u64) << 32)
        | (((bytes[32]) as u64) << 24)
        | (((bytes[33]) as u64) << 16)
        | (((bytes[34]) as u64) << 8)
        | ((bytes[35]) as u64);
}

#[inline]
fn unpack_bits_37(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 37);
    values[0] = (((bytes[0]) as u64) << 29)
        | (((bytes[1]) as u64) << 21)
        | (((bytes[2]) as u64) << 13)
        | (((bytes[3]) as u64) << 5)
        | (((bytes[4] >> 3) & 0x1f) as u64);
    values[1] = ((((bytes[4]) & 0x7) as u64) << 34)
        | (((bytes[5]) as u64) << 26)
        | (((bytes[6]) as u64) << 18)
        | (((bytes[7]) as u64) << 10)
        | (((bytes[8]) as u64) << 2)
        | (((bytes[9] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[9]) & 0x3f) as u64) << 31)
        | (((bytes[10]) as u64) << 23)
        | (((bytes[11]) as u64) << 15)
        | (((bytes[12]) as u64) << 7)
        | (((bytes[13] >> 1) & 0x7f) as u64);
    values[3] = ((((bytes[13]) & 0x1) as u64) << 36)
        | (((bytes[14]) as u64) << 28)
        | (((bytes[15]) as u64) << 20)
        | (((bytes[16]) as u64) << 12)
        | (((bytes[17]) as u64) << 4)
        | (((bytes[18] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[18]) & 0xf) as u64) << 33)
        | (((bytes[19]) as u64) << 25)
        | (((bytes[20]) as u64) << 17)
        | (((bytes[21]) as u64) << 9)
        | (((bytes[22]) as u64) << 1)
        | (((bytes[23] >> 7) & 0x1) as u64);
    values[5] = ((((bytes[23]) & 0x7f) as u64) << 30)
        | (((bytes[24]) as u64) << 22)
        | (((bytes[25]) as u64) << 14)
        | (((bytes[26]) as u64) << 6)
        | (((bytes[27] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[27]) & 0x3) as u64) << 35)
        | (((bytes[28]) as u64) << 27)
        | (((bytes[29]) as u64) << 19)
        | (((bytes[30]) as u64) << 11)
        | (((bytes[31]) as u64) << 3)
        | (((bytes[32] >> 5) & 0x7) as u64);
    values[7] = ((((bytes[32]) & 0x1f) as u64) << 32)
        | (((bytes[33]) as u64) << 24)
        | (((bytes[34]) as u64) << 16)
        | (((bytes[35]) as u64) << 8)
        | ((bytes[36]) as u64);
}

#[inline]
fn unpack_bits_38(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 38);
    values[0] = (((bytes[0]) as u64) << 30)
        | (((bytes[1]) as u64) << 22)
        | (((bytes[2]) as u64) << 14)
        | (((bytes[3]) as u64) << 6)
        | (((bytes[4] >> 2) & 0x3f) as u64);
    values[1] = ((((bytes[4]) & 0x3) as u64) << 36)
        | (((bytes[5]) as u64) << 28)
        | (((bytes[6]) as u64) << 20)
        | (((bytes[7]) as u64) << 12)
        | (((bytes[8]) as u64) << 4)
        | (((bytes[9] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[9]) & 0xf) as u64) << 34)
        | (((bytes[10]) as u64) << 26)
        | (((bytes[11]) as u64) << 18)
        | (((bytes[12]) as u64) << 10)
        | (((bytes[13]) as u64) << 2)
        | (((bytes[14] >> 6) & 0x3) as u64);
    values[3] = ((((bytes[14]) & 0x3f) as u64) << 32)
        | (((bytes[15]) as u64) << 24)
        | (((bytes[16]) as u64) << 16)
        | (((bytes[17]) as u64) << 8)
        | ((bytes[18]) as u64);
    values[4] = (((bytes[19]) as u64) << 30)
        | (((bytes[20]) as u64) << 22)
        | (((bytes[21]) as u64) << 14)
        | (((bytes[22]) as u64) << 6)
        | (((bytes[23] >> 2) & 0x3f) as u64);
    values[5] = ((((bytes[23]) & 0x3) as u64) << 36)
        | (((bytes[24]) as u64) << 28)
        | (((bytes[25]) as u64) << 20)
        | (((bytes[26]) as u64) << 12)
        | (((bytes[27]) as u64) << 4)
        | (((bytes[28] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[28]) & 0xf) as u64) << 34)
        | (((bytes[29]) as u64) << 26)
        | (((bytes[30]) as u64) << 18)
        | (((bytes[31]) as u64) << 10)
        | (((bytes[32]) as u64) << 2)
        | (((bytes[33] >> 6) & 0x3) as u64);
    values[7] = ((((bytes[33]) & 0x3f) as u64) << 32)
        | (((bytes[34]) as u64) << 24)
        | (((bytes[35]) as u64) << 16)
        | (((bytes[36]) as u64) << 8)
        | ((bytes[37]) as u64);
}

#[inline]
fn unpack_bits_39(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 39);
    values[0] = (((bytes[0]) as u64) << 31)
        | (((bytes[1]) as u64) << 23)
        | (((bytes[2]) as u64) << 15)
        | (((bytes[3]) as u64) << 7)
        | (((bytes[4] >> 1) & 0x7f) as u64);
    values[1] = ((((bytes[4]) & 0x1) as u64) << 38)
        | (((bytes[5]) as u64) << 30)
        | (((bytes[6]) as u64) << 22)
        | (((bytes[7]) as u64) << 14)
        | (((bytes[8]) as u64) << 6)
        | (((bytes[9] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[9]) & 0x3) as u64) << 37)
        | (((bytes[10]) as u64) << 29)
        | (((bytes[11]) as u64) << 21)
        | (((bytes[12]) as u64) << 13)
        | (((bytes[13]) as u64) << 5)
        | (((bytes[14] >> 3) & 0x1f) as u64);
    values[3] = ((((bytes[14]) & 0x7) as u64) << 36)
        | (((bytes[15]) as u64) << 28)
        | (((bytes[16]) as u64) << 20)
        | (((bytes[17]) as u64) << 12)
        | (((bytes[18]) as u64) << 4)
        | (((bytes[19] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[19]) & 0xf) as u64) << 35)
        | (((bytes[20]) as u64) << 27)
        | (((bytes[21]) as u64) << 19)
        | (((bytes[22]) as u64) << 11)
        | (((bytes[23]) as u64) << 3)
        | (((bytes[24] >> 5) & 0x7) as u64);
    values[5] = ((((bytes[24]) & 0x1f) as u64) << 34)
        | (((bytes[25]) as u64) << 26)
        | (((bytes[26]) as u64) << 18)
        | (((bytes[27]) as u64) << 10)
        | (((bytes[28]) as u64) << 2)
        | (((bytes[29] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[29]) & 0x3f) as u64) << 33)
        | (((bytes[30]) as u64) << 25)
        | (((bytes[31]) as u64) << 17)
        | (((bytes[32]) as u64) << 9)
        | (((bytes[33]) as u64) << 1)
        | (((bytes[34] >> 7) & 0x1) as u64);
    values[7] = ((((bytes[34]) & 0x7f) as u64) << 32)
        | (((bytes[35]) as u64) << 24)
        | (((bytes[36]) as u64) << 16)
        | (((bytes[37]) as u64) << 8)
        | ((bytes[38]) as u64);
}

#[inline]
fn unpack_bits_40(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 40);
    values[0] = (((bytes[0]) as u64) << 32)
        | (((bytes[1]) as u64) << 24)
        | (((bytes[2]) as u64) << 16)
        | (((bytes[3]) as u64) << 8)
        | ((bytes[4]) as u64);
    values[1] = (((bytes[5]) as u64) << 32)
        | (((bytes[6]) as u64) << 24)
        | (((bytes[7]) as u64) << 16)
        | (((bytes[8]) as u64) << 8)
        | ((bytes[9]) as u64);
    values[2] = (((bytes[10]) as u64) << 32)
        | (((bytes[11]) as u64) << 24)
        | (((bytes[12]) as u64) << 16)
        | (((bytes[13]) as u64) << 8)
        | ((bytes[14]) as u64);
    values[3] = (((bytes[15]) as u64) << 32)
        | (((bytes[16]) as u64) << 24)
        | (((bytes[17]) as u64) << 16)
        | (((bytes[18]) as u64) << 8)
        | ((bytes[19]) as u64);
    values[4] = (((bytes[20]) as u64) << 32)
        | (((bytes[21]) as u64) << 24)
        | (((bytes[22]) as u64) << 16)
        | (((bytes[23]) as u64) << 8)
        | ((bytes[24]) as u64);
    values[5] = (((bytes[25]) as u64) << 32)
        | (((bytes[26]) as u64) << 24)
        | (((bytes[27]) as u64) << 16)
        | (((bytes[28]) as u64) << 8)
        | ((bytes[29]) as u64);
    values[6] = (((bytes[30]) as u64) << 32)
        | (((bytes[31]) as u64) << 24)
        | (((bytes[32]) as u64) << 16)
        | (((bytes[33]) as u64) << 8)
        | ((bytes[34]) as u64);
    values[7] = (((bytes[35]) as u64) << 32)
        | (((bytes[36]) as u64) << 24)
        | (((bytes[37]) as u64) << 16)
        | (((bytes[38]) as u64) << 8)
        | ((bytes[39]) as u64);
}

#[inline]
fn unpack_bits_41(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 41);
    values[0] = (((bytes[0]) as u64) << 33)
        | (((bytes[1]) as u64) << 25)
        | (((bytes[2]) as u64) << 17)
        | (((bytes[3]) as u64) << 9)
        | (((bytes[4]) as u64) << 1)
        | (((bytes[5] >> 7) & 0x1) as u64);
    values[1] = ((((bytes[5]) & 0x7f) as u64) << 34)
        | (((bytes[6]) as u64) << 26)
        | (((bytes[7]) as u64) << 18)
        | (((bytes[8]) as u64) << 10)
        | (((bytes[9]) as u64) << 2)
        | (((bytes[10] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[10]) & 0x3f) as u64) << 35)
        | (((bytes[11]) as u64) << 27)
        | (((bytes[12]) as u64) << 19)
        | (((bytes[13]) as u64) << 11)
        | (((bytes[14]) as u64) << 3)
        | (((bytes[15] >> 5) & 0x7) as u64);
    values[3] = ((((bytes[15]) & 0x1f) as u64) << 36)
        | (((bytes[16]) as u64) << 28)
        | (((bytes[17]) as u64) << 20)
        | (((bytes[18]) as u64) << 12)
        | (((bytes[19]) as u64) << 4)
        | (((bytes[20] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[20]) & 0xf) as u64) << 37)
        | (((bytes[21]) as u64) << 29)
        | (((bytes[22]) as u64) << 21)
        | (((bytes[23]) as u64) << 13)
        | (((bytes[24]) as u64) << 5)
        | (((bytes[25] >> 3) & 0x1f) as u64);
    values[5] = ((((bytes[25]) & 0x7) as u64) << 38)
        | (((bytes[26]) as u64) << 30)
        | (((bytes[27]) as u64) << 22)
        | (((bytes[28]) as u64) << 14)
        | (((bytes[29]) as u64) << 6)
        | (((bytes[30] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[30]) & 0x3) as u64) << 39)
        | (((bytes[31]) as u64) << 31)
        | (((bytes[32]) as u64) << 23)
        | (((bytes[33]) as u64) << 15)
        | (((bytes[34]) as u64) << 7)
        | (((bytes[35] >> 1) & 0x7f) as u64);
    values[7] = ((((bytes[35]) & 0x1) as u64) << 40)
        | (((bytes[36]) as u64) << 32)
        | (((bytes[37]) as u64) << 24)
        | (((bytes[38]) as u64) << 16)
        | (((bytes[39]) as u64) << 8)
        | ((bytes[40]) as u64);
}

#[inline]
fn unpack_bits_42(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 42);
    values[0] = (((bytes[0]) as u64) << 34)
        | (((bytes[1]) as u64) << 26)
        | (((bytes[2]) as u64) << 18)
        | (((bytes[3]) as u64) << 10)
        | (((bytes[4]) as u64) << 2)
        | (((bytes[5] >> 6) & 0x3) as u64);
    values[1] = ((((bytes[5]) & 0x3f) as u64) << 36)
        | (((bytes[6]) as u64) << 28)
        | (((bytes[7]) as u64) << 20)
        | (((bytes[8]) as u64) << 12)
        | (((bytes[9]) as u64) << 4)
        | (((bytes[10] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[10]) & 0xf) as u64) << 38)
        | (((bytes[11]) as u64) << 30)
        | (((bytes[12]) as u64) << 22)
        | (((bytes[13]) as u64) << 14)
        | (((bytes[14]) as u64) << 6)
        | (((bytes[15] >> 2) & 0x3f) as u64);
    values[3] = ((((bytes[15]) & 0x3) as u64) << 40)
        | (((bytes[16]) as u64) << 32)
        | (((bytes[17]) as u64) << 24)
        | (((bytes[18]) as u64) << 16)
        | (((bytes[19]) as u64) << 8)
        | ((bytes[20]) as u64);
    values[4] = (((bytes[21]) as u64) << 34)
        | (((bytes[22]) as u64) << 26)
        | (((bytes[23]) as u64) << 18)
        | (((bytes[24]) as u64) << 10)
        | (((bytes[25]) as u64) << 2)
        | (((bytes[26] >> 6) & 0x3) as u64);
    values[5] = ((((bytes[26]) & 0x3f) as u64) << 36)
        | (((bytes[27]) as u64) << 28)
        | (((bytes[28]) as u64) << 20)
        | (((bytes[29]) as u64) << 12)
        | (((bytes[30]) as u64) << 4)
        | (((bytes[31] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[31]) & 0xf) as u64) << 38)
        | (((bytes[32]) as u64) << 30)
        | (((bytes[33]) as u64) << 22)
        | (((bytes[34]) as u64) << 14)
        | (((bytes[35]) as u64) << 6)
        | (((bytes[36] >> 2) & 0x3f) as u64);
    values[7] = ((((bytes[36]) & 0x3) as u64) << 40)
        | (((bytes[37]) as u64) << 32)
        | (((bytes[38]) as u64) << 24)
        | (((bytes[39]) as u64) << 16)
        | (((bytes[40]) as u64) << 8)
        | ((bytes[41]) as u64);
}

#[inline]
fn unpack_bits_43(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 43);
    values[0] = (((bytes[0]) as u64) << 35)
        | (((bytes[1]) as u64) << 27)
        | (((bytes[2]) as u64) << 19)
        | (((bytes[3]) as u64) << 11)
        | (((bytes[4]) as u64) << 3)
        | (((bytes[5] >> 5) & 0x7) as u64);
    values[1] = ((((bytes[5]) & 0x1f) as u64) << 38)
        | (((bytes[6]) as u64) << 30)
        | (((bytes[7]) as u64) << 22)
        | (((bytes[8]) as u64) << 14)
        | (((bytes[9]) as u64) << 6)
        | (((bytes[10] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[10]) & 0x3) as u64) << 41)
        | (((bytes[11]) as u64) << 33)
        | (((bytes[12]) as u64) << 25)
        | (((bytes[13]) as u64) << 17)
        | (((bytes[14]) as u64) << 9)
        | (((bytes[15]) as u64) << 1)
        | (((bytes[16] >> 7) & 0x1) as u64);
    values[3] = ((((bytes[16]) & 0x7f) as u64) << 36)
        | (((bytes[17]) as u64) << 28)
        | (((bytes[18]) as u64) << 20)
        | (((bytes[19]) as u64) << 12)
        | (((bytes[20]) as u64) << 4)
        | (((bytes[21] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[21]) & 0xf) as u64) << 39)
        | (((bytes[22]) as u64) << 31)
        | (((bytes[23]) as u64) << 23)
        | (((bytes[24]) as u64) << 15)
        | (((bytes[25]) as u64) << 7)
        | (((bytes[26] >> 1) & 0x7f) as u64);
    values[5] = ((((bytes[26]) & 0x1) as u64) << 42)
        | (((bytes[27]) as u64) << 34)
        | (((bytes[28]) as u64) << 26)
        | (((bytes[29]) as u64) << 18)
        | (((bytes[30]) as u64) << 10)
        | (((bytes[31]) as u64) << 2)
        | (((bytes[32] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[32]) & 0x3f) as u64) << 37)
        | (((bytes[33]) as u64) << 29)
        | (((bytes[34]) as u64) << 21)
        | (((bytes[35]) as u64) << 13)
        | (((bytes[36]) as u64) << 5)
        | (((bytes[37] >> 3) & 0x1f) as u64);
    values[7] = ((((bytes[37]) & 0x7) as u64) << 40)
        | (((bytes[38]) as u64) << 32)
        | (((bytes[39]) as u64) << 24)
        | (((bytes[40]) as u64) << 16)
        | (((bytes[41]) as u64) << 8)
        | ((bytes[42]) as u64);
}

#[inline]
fn unpack_bits_44(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 44);
    values[0] = (((bytes[0]) as u64) << 36)
        | (((bytes[1]) as u64) << 28)
        | (((bytes[2]) as u64) << 20)
        | (((bytes[3]) as u64) << 12)
        | (((bytes[4]) as u64) << 4)
        | (((bytes[5] >> 4) & 0xf) as u64);
    values[1] = ((((bytes[5]) & 0xf) as u64) << 40)
        | (((bytes[6]) as u64) << 32)
        | (((bytes[7]) as u64) << 24)
        | (((bytes[8]) as u64) << 16)
        | (((bytes[9]) as u64) << 8)
        | ((bytes[10]) as u64);
    values[2] = (((bytes[11]) as u64) << 36)
        | (((bytes[12]) as u64) << 28)
        | (((bytes[13]) as u64) << 20)
        | (((bytes[14]) as u64) << 12)
        | (((bytes[15]) as u64) << 4)
        | (((bytes[16] >> 4) & 0xf) as u64);
    values[3] = ((((bytes[16]) & 0xf) as u64) << 40)
        | (((bytes[17]) as u64) << 32)
        | (((bytes[18]) as u64) << 24)
        | (((bytes[19]) as u64) << 16)
        | (((bytes[20]) as u64) << 8)
        | ((bytes[21]) as u64);
    values[4] = (((bytes[22]) as u64) << 36)
        | (((bytes[23]) as u64) << 28)
        | (((bytes[24]) as u64) << 20)
        | (((bytes[25]) as u64) << 12)
        | (((bytes[26]) as u64) << 4)
        | (((bytes[27] >> 4) & 0xf) as u64);
    values[5] = ((((bytes[27]) & 0xf) as u64) << 40)
        | (((bytes[28]) as u64) << 32)
        | (((bytes[29]) as u64) << 24)
        | (((bytes[30]) as u64) << 16)
        | (((bytes[31]) as u64) << 8)
        | ((bytes[32]) as u64);
    values[6] = (((bytes[33]) as u64) << 36)
        | (((bytes[34]) as u64) << 28)
        | (((bytes[35]) as u64) << 20)
        | (((bytes[36]) as u64) << 12)
        | (((bytes[37]) as u64) << 4)
        | (((bytes[38] >> 4) & 0xf) as u64);
    values[7] = ((((bytes[38]) & 0xf) as u64) << 40)
        | (((bytes[39]) as u64) << 32)
        | (((bytes[40]) as u64) << 24)
        | (((bytes[41]) as u64) << 16)
        | (((bytes[42]) as u64) << 8)
        | ((bytes[43]) as u64);
}

#[inline]
fn unpack_bits_45(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 45);
    values[0] = (((bytes[0]) as u64) << 37)
        | (((bytes[1]) as u64) << 29)
        | (((bytes[2]) as u64) << 21)
        | (((bytes[3]) as u64) << 13)
        | (((bytes[4]) as u64) << 5)
        | (((bytes[5] >> 3) & 0x1f) as u64);
    values[1] = ((((bytes[5]) & 0x7) as u64) << 42)
        | (((bytes[6]) as u64) << 34)
        | (((bytes[7]) as u64) << 26)
        | (((bytes[8]) as u64) << 18)
        | (((bytes[9]) as u64) << 10)
        | (((bytes[10]) as u64) << 2)
        | (((bytes[11] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[11]) & 0x3f) as u64) << 39)
        | (((bytes[12]) as u64) << 31)
        | (((bytes[13]) as u64) << 23)
        | (((bytes[14]) as u64) << 15)
        | (((bytes[15]) as u64) << 7)
        | (((bytes[16] >> 1) & 0x7f) as u64);
    values[3] = ((((bytes[16]) & 0x1) as u64) << 44)
        | (((bytes[17]) as u64) << 36)
        | (((bytes[18]) as u64) << 28)
        | (((bytes[19]) as u64) << 20)
        | (((bytes[20]) as u64) << 12)
        | (((bytes[21]) as u64) << 4)
        | (((bytes[22] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[22]) & 0xf) as u64) << 41)
        | (((bytes[23]) as u64) << 33)
        | (((bytes[24]) as u64) << 25)
        | (((bytes[25]) as u64) << 17)
        | (((bytes[26]) as u64) << 9)
        | (((bytes[27]) as u64) << 1)
        | (((bytes[28] >> 7) & 0x1) as u64);
    values[5] = ((((bytes[28]) & 0x7f) as u64) << 38)
        | (((bytes[29]) as u64) << 30)
        | (((bytes[30]) as u64) << 22)
        | (((bytes[31]) as u64) << 14)
        | (((bytes[32]) as u64) << 6)
        | (((bytes[33] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[33]) & 0x3) as u64) << 43)
        | (((bytes[34]) as u64) << 35)
        | (((bytes[35]) as u64) << 27)
        | (((bytes[36]) as u64) << 19)
        | (((bytes[37]) as u64) << 11)
        | (((bytes[38]) as u64) << 3)
        | (((bytes[39] >> 5) & 0x7) as u64);
    values[7] = ((((bytes[39]) & 0x1f) as u64) << 40)
        | (((bytes[40]) as u64) << 32)
        | (((bytes[41]) as u64) << 24)
        | (((bytes[42]) as u64) << 16)
        | (((bytes[43]) as u64) << 8)
        | ((bytes[44]) as u64);
}

#[inline]
fn unpack_bits_46(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 46);
    values[0] = (((bytes[0]) as u64) << 38)
        | (((bytes[1]) as u64) << 30)
        | (((bytes[2]) as u64) << 22)
        | (((bytes[3]) as u64) << 14)
        | (((bytes[4]) as u64) << 6)
        | (((bytes[5] >> 2) & 0x3f) as u64);
    values[1] = ((((bytes[5]) & 0x3) as u64) << 44)
        | (((bytes[6]) as u64) << 36)
        | (((bytes[7]) as u64) << 28)
        | (((bytes[8]) as u64) << 20)
        | (((bytes[9]) as u64) << 12)
        | (((bytes[10]) as u64) << 4)
        | (((bytes[11] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[11]) & 0xf) as u64) << 42)
        | (((bytes[12]) as u64) << 34)
        | (((bytes[13]) as u64) << 26)
        | (((bytes[14]) as u64) << 18)
        | (((bytes[15]) as u64) << 10)
        | (((bytes[16]) as u64) << 2)
        | (((bytes[17] >> 6) & 0x3) as u64);
    values[3] = ((((bytes[17]) & 0x3f) as u64) << 40)
        | (((bytes[18]) as u64) << 32)
        | (((bytes[19]) as u64) << 24)
        | (((bytes[20]) as u64) << 16)
        | (((bytes[21]) as u64) << 8)
        | ((bytes[22]) as u64);
    values[4] = (((bytes[23]) as u64) << 38)
        | (((bytes[24]) as u64) << 30)
        | (((bytes[25]) as u64) << 22)
        | (((bytes[26]) as u64) << 14)
        | (((bytes[27]) as u64) << 6)
        | (((bytes[28] >> 2) & 0x3f) as u64);
    values[5] = ((((bytes[28]) & 0x3) as u64) << 44)
        | (((bytes[29]) as u64) << 36)
        | (((bytes[30]) as u64) << 28)
        | (((bytes[31]) as u64) << 20)
        | (((bytes[32]) as u64) << 12)
        | (((bytes[33]) as u64) << 4)
        | (((bytes[34] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[34]) & 0xf) as u64) << 42)
        | (((bytes[35]) as u64) << 34)
        | (((bytes[36]) as u64) << 26)
        | (((bytes[37]) as u64) << 18)
        | (((bytes[38]) as u64) << 10)
        | (((bytes[39]) as u64) << 2)
        | (((bytes[40] >> 6) & 0x3) as u64);
    values[7] = ((((bytes[40]) & 0x3f) as u64) << 40)
        | (((bytes[41]) as u64) << 32)
        | (((bytes[42]) as u64) << 24)
        | (((bytes[43]) as u64) << 16)
        | (((bytes[44]) as u64) << 8)
        | ((bytes[45]) as u64);
}

#[inline]
fn unpack_bits_47(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 47);
    values[0] = (((bytes[0]) as u64) << 39)
        | (((bytes[1]) as u64) << 31)
        | (((bytes[2]) as u64) << 23)
        | (((bytes[3]) as u64) << 15)
        | (((bytes[4]) as u64) << 7)
        | (((bytes[5] >> 1) & 0x7f) as u64);
    values[1] = ((((bytes[5]) & 0x1) as u64) << 46)
        | (((bytes[6]) as u64) << 38)
        | (((bytes[7]) as u64) << 30)
        | (((bytes[8]) as u64) << 22)
        | (((bytes[9]) as u64) << 14)
        | (((bytes[10]) as u64) << 6)
        | (((bytes[11] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[11]) & 0x3) as u64) << 45)
        | (((bytes[12]) as u64) << 37)
        | (((bytes[13]) as u64) << 29)
        | (((bytes[14]) as u64) << 21)
        | (((bytes[15]) as u64) << 13)
        | (((bytes[16]) as u64) << 5)
        | (((bytes[17] >> 3) & 0x1f) as u64);
    values[3] = ((((bytes[17]) & 0x7) as u64) << 44)
        | (((bytes[18]) as u64) << 36)
        | (((bytes[19]) as u64) << 28)
        | (((bytes[20]) as u64) << 20)
        | (((bytes[21]) as u64) << 12)
        | (((bytes[22]) as u64) << 4)
        | (((bytes[23] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[23]) & 0xf) as u64) << 43)
        | (((bytes[24]) as u64) << 35)
        | (((bytes[25]) as u64) << 27)
        | (((bytes[26]) as u64) << 19)
        | (((bytes[27]) as u64) << 11)
        | (((bytes[28]) as u64) << 3)
        | (((bytes[29] >> 5) & 0x7) as u64);
    values[5] = ((((bytes[29]) & 0x1f) as u64) << 42)
        | (((bytes[30]) as u64) << 34)
        | (((bytes[31]) as u64) << 26)
        | (((bytes[32]) as u64) << 18)
        | (((bytes[33]) as u64) << 10)
        | (((bytes[34]) as u64) << 2)
        | (((bytes[35] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[35]) & 0x3f) as u64) << 41)
        | (((bytes[36]) as u64) << 33)
        | (((bytes[37]) as u64) << 25)
        | (((bytes[38]) as u64) << 17)
        | (((bytes[39]) as u64) << 9)
        | (((bytes[40]) as u64) << 1)
        | (((bytes[41] >> 7) & 0x1) as u64);
    values[7] = ((((bytes[41]) & 0x7f) as u64) << 40)
        | (((bytes[42]) as u64) << 32)
        | (((bytes[43]) as u64) << 24)
        | (((bytes[44]) as u64) << 16)
        | (((bytes[45]) as u64) << 8)
        | ((bytes[46]) as u64);
}

#[inline]
fn unpack_bits_48(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 48);
    values[0] = (((bytes[0]) as u64) << 40)
        | (((bytes[1]) as u64) << 32)
        | (((bytes[2]) as u64) << 24)
        | (((bytes[3]) as u64) << 16)
        | (((bytes[4]) as u64) << 8)
        | ((bytes[5]) as u64);
    values[1] = (((bytes[6]) as u64) << 40)
        | (((bytes[7]) as u64) << 32)
        | (((bytes[8]) as u64) << 24)
        | (((bytes[9]) as u64) << 16)
        | (((bytes[10]) as u64) << 8)
        | ((bytes[11]) as u64);
    values[2] = (((bytes[12]) as u64) << 40)
        | (((bytes[13]) as u64) << 32)
        | (((bytes[14]) as u64) << 24)
        | (((bytes[15]) as u64) << 16)
        | (((bytes[16]) as u64) << 8)
        | ((bytes[17]) as u64);
    values[3] = (((bytes[18]) as u64) << 40)
        | (((bytes[19]) as u64) << 32)
        | (((bytes[20]) as u64) << 24)
        | (((bytes[21]) as u64) << 16)
        | (((bytes[22]) as u64) << 8)
        | ((bytes[23]) as u64);
    values[4] = (((bytes[24]) as u64) << 40)
        | (((bytes[25]) as u64) << 32)
        | (((bytes[26]) as u64) << 24)
        | (((bytes[27]) as u64) << 16)
        | (((bytes[28]) as u64) << 8)
        | ((bytes[29]) as u64);
    values[5] = (((bytes[30]) as u64) << 40)
        | (((bytes[31]) as u64) << 32)
        | (((bytes[32]) as u64) << 24)
        | (((bytes[33]) as u64) << 16)
        | (((bytes[34]) as u64) << 8)
        | ((bytes[35]) as u64);
    values[6] = (((bytes[36]) as u64) << 40)
        | (((bytes[37]) as u64) << 32)
        | (((bytes[38]) as u64) << 24)
        | (((bytes[39]) as u64) << 16)
        | (((bytes[40]) as u64) << 8)
        | ((bytes[41]) as u64);
    values[7] = (((bytes[42]) as u64) << 40)
        | (((bytes[43]) as u64) << 32)
        | (((bytes[44]) as u64) << 24)
        | (((bytes[45]) as u64) << 16)
        | (((bytes[46]) as u64) << 8)
        | ((bytes[47]) as u64);
}

#[inline]
fn unpack_bits_49(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 49);
    values[0] = (((bytes[0]) as u64) << 41)
        | (((bytes[1]) as u64) << 33)
        | (((bytes[2]) as u64) << 25)
        | (((bytes[3]) as u64) << 17)
        | (((bytes[4]) as u64) << 9)
        | (((bytes[5]) as u64) << 1)
        | (((bytes[6] >> 7) & 0x1) as u64);
    values[1] = ((((bytes[6]) & 0x7f) as u64) << 42)
        | (((bytes[7]) as u64) << 34)
        | (((bytes[8]) as u64) << 26)
        | (((bytes[9]) as u64) << 18)
        | (((bytes[10]) as u64) << 10)
        | (((bytes[11]) as u64) << 2)
        | (((bytes[12] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[12]) & 0x3f) as u64) << 43)
        | (((bytes[13]) as u64) << 35)
        | (((bytes[14]) as u64) << 27)
        | (((bytes[15]) as u64) << 19)
        | (((bytes[16]) as u64) << 11)
        | (((bytes[17]) as u64) << 3)
        | (((bytes[18] >> 5) & 0x7) as u64);
    values[3] = ((((bytes[18]) & 0x1f) as u64) << 44)
        | (((bytes[19]) as u64) << 36)
        | (((bytes[20]) as u64) << 28)
        | (((bytes[21]) as u64) << 20)
        | (((bytes[22]) as u64) << 12)
        | (((bytes[23]) as u64) << 4)
        | (((bytes[24] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[24]) & 0xf) as u64) << 45)
        | (((bytes[25]) as u64) << 37)
        | (((bytes[26]) as u64) << 29)
        | (((bytes[27]) as u64) << 21)
        | (((bytes[28]) as u64) << 13)
        | (((bytes[29]) as u64) << 5)
        | (((bytes[30] >> 3) & 0x1f) as u64);
    values[5] = ((((bytes[30]) & 0x7) as u64) << 46)
        | (((bytes[31]) as u64) << 38)
        | (((bytes[32]) as u64) << 30)
        | (((bytes[33]) as u64) << 22)
        | (((bytes[34]) as u64) << 14)
        | (((bytes[35]) as u64) << 6)
        | (((bytes[36] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[36]) & 0x3) as u64) << 47)
        | (((bytes[37]) as u64) << 39)
        | (((bytes[38]) as u64) << 31)
        | (((bytes[39]) as u64) << 23)
        | (((bytes[40]) as u64) << 15)
        | (((bytes[41]) as u64) << 7)
        | (((bytes[42] >> 1) & 0x7f) as u64);
    values[7] = ((((bytes[42]) & 0x1) as u64) << 48)
        | (((bytes[43]) as u64) << 40)
        | (((bytes[44]) as u64) << 32)
        | (((bytes[45]) as u64) << 24)
        | (((bytes[46]) as u64) << 16)
        | (((bytes[47]) as u64) << 8)
        | ((bytes[48]) as u64);
}

#[inline]
fn unpack_bits_50(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 50);
    values[0] = (((bytes[0]) as u64) << 42)
        | (((bytes[1]) as u64) << 34)
        | (((bytes[2]) as u64) << 26)
        | (((bytes[3]) as u64) << 18)
        | (((bytes[4]) as u64) << 10)
        | (((bytes[5]) as u64) << 2)
        | (((bytes[6] >> 6) & 0x3) as u64);
    values[1] = ((((bytes[6]) & 0x3f) as u64) << 44)
        | (((bytes[7]) as u64) << 36)
        | (((bytes[8]) as u64) << 28)
        | (((bytes[9]) as u64) << 20)
        | (((bytes[10]) as u64) << 12)
        | (((bytes[11]) as u64) << 4)
        | (((bytes[12] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[12]) & 0xf) as u64) << 46)
        | (((bytes[13]) as u64) << 38)
        | (((bytes[14]) as u64) << 30)
        | (((bytes[15]) as u64) << 22)
        | (((bytes[16]) as u64) << 14)
        | (((bytes[17]) as u64) << 6)
        | (((bytes[18] >> 2) & 0x3f) as u64);
    values[3] = ((((bytes[18]) & 0x3) as u64) << 48)
        | (((bytes[19]) as u64) << 40)
        | (((bytes[20]) as u64) << 32)
        | (((bytes[21]) as u64) << 24)
        | (((bytes[22]) as u64) << 16)
        | (((bytes[23]) as u64) << 8)
        | ((bytes[24]) as u64);
    values[4] = (((bytes[25]) as u64) << 42)
        | (((bytes[26]) as u64) << 34)
        | (((bytes[27]) as u64) << 26)
        | (((bytes[28]) as u64) << 18)
        | (((bytes[29]) as u64) << 10)
        | (((bytes[30]) as u64) << 2)
        | (((bytes[31] >> 6) & 0x3) as u64);
    values[5] = ((((bytes[31]) & 0x3f) as u64) << 44)
        | (((bytes[32]) as u64) << 36)
        | (((bytes[33]) as u64) << 28)
        | (((bytes[34]) as u64) << 20)
        | (((bytes[35]) as u64) << 12)
        | (((bytes[36]) as u64) << 4)
        | (((bytes[37] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[37]) & 0xf) as u64) << 46)
        | (((bytes[38]) as u64) << 38)
        | (((bytes[39]) as u64) << 30)
        | (((bytes[40]) as u64) << 22)
        | (((bytes[41]) as u64) << 14)
        | (((bytes[42]) as u64) << 6)
        | (((bytes[43] >> 2) & 0x3f) as u64);
    values[7] = ((((bytes[43]) & 0x3) as u64) << 48)
        | (((bytes[44]) as u64) << 40)
        | (((bytes[45]) as u64) << 32)
        | (((bytes[46]) as u64) << 24)
        | (((bytes[47]) as u64) << 16)
        | (((bytes[48]) as u64) << 8)
        | ((bytes[49]) as u64);
}

#[inline]
fn unpack_bits_51(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 51);
    values[0] = (((bytes[0]) as u64) << 43)
        | (((bytes[1]) as u64) << 35)
        | (((bytes[2]) as u64) << 27)
        | (((bytes[3]) as u64) << 19)
        | (((bytes[4]) as u64) << 11)
        | (((bytes[5]) as u64) << 3)
        | (((bytes[6] >> 5) & 0x7) as u64);
    values[1] = ((((bytes[6]) & 0x1f) as u64) << 46)
        | (((bytes[7]) as u64) << 38)
        | (((bytes[8]) as u64) << 30)
        | (((bytes[9]) as u64) << 22)
        | (((bytes[10]) as u64) << 14)
        | (((bytes[11]) as u64) << 6)
        | (((bytes[12] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[12]) & 0x3) as u64) << 49)
        | (((bytes[13]) as u64) << 41)
        | (((bytes[14]) as u64) << 33)
        | (((bytes[15]) as u64) << 25)
        | (((bytes[16]) as u64) << 17)
        | (((bytes[17]) as u64) << 9)
        | (((bytes[18]) as u64) << 1)
        | (((bytes[19] >> 7) & 0x1) as u64);
    values[3] = ((((bytes[19]) & 0x7f) as u64) << 44)
        | (((bytes[20]) as u64) << 36)
        | (((bytes[21]) as u64) << 28)
        | (((bytes[22]) as u64) << 20)
        | (((bytes[23]) as u64) << 12)
        | (((bytes[24]) as u64) << 4)
        | (((bytes[25] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[25]) & 0xf) as u64) << 47)
        | (((bytes[26]) as u64) << 39)
        | (((bytes[27]) as u64) << 31)
        | (((bytes[28]) as u64) << 23)
        | (((bytes[29]) as u64) << 15)
        | (((bytes[30]) as u64) << 7)
        | (((bytes[31] >> 1) & 0x7f) as u64);
    values[5] = ((((bytes[31]) & 0x1) as u64) << 50)
        | (((bytes[32]) as u64) << 42)
        | (((bytes[33]) as u64) << 34)
        | (((bytes[34]) as u64) << 26)
        | (((bytes[35]) as u64) << 18)
        | (((bytes[36]) as u64) << 10)
        | (((bytes[37]) as u64) << 2)
        | (((bytes[38] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[38]) & 0x3f) as u64) << 45)
        | (((bytes[39]) as u64) << 37)
        | (((bytes[40]) as u64) << 29)
        | (((bytes[41]) as u64) << 21)
        | (((bytes[42]) as u64) << 13)
        | (((bytes[43]) as u64) << 5)
        | (((bytes[44] >> 3) & 0x1f) as u64);
    values[7] = ((((bytes[44]) & 0x7) as u64) << 48)
        | (((bytes[45]) as u64) << 40)
        | (((bytes[46]) as u64) << 32)
        | (((bytes[47]) as u64) << 24)
        | (((bytes[48]) as u64) << 16)
        | (((bytes[49]) as u64) << 8)
        | ((bytes[50]) as u64);
}

#[inline]
fn unpack_bits_52(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 52);
    values[0] = (((bytes[0]) as u64) << 44)
        | (((bytes[1]) as u64) << 36)
        | (((bytes[2]) as u64) << 28)
        | (((bytes[3]) as u64) << 20)
        | (((bytes[4]) as u64) << 12)
        | (((bytes[5]) as u64) << 4)
        | (((bytes[6] >> 4) & 0xf) as u64);
    values[1] = ((((bytes[6]) & 0xf) as u64) << 48)
        | (((bytes[7]) as u64) << 40)
        | (((bytes[8]) as u64) << 32)
        | (((bytes[9]) as u64) << 24)
        | (((bytes[10]) as u64) << 16)
        | (((bytes[11]) as u64) << 8)
        | ((bytes[12]) as u64);
    values[2] = (((bytes[13]) as u64) << 44)
        | (((bytes[14]) as u64) << 36)
        | (((bytes[15]) as u64) << 28)
        | (((bytes[16]) as u64) << 20)
        | (((bytes[17]) as u64) << 12)
        | (((bytes[18]) as u64) << 4)
        | (((bytes[19] >> 4) & 0xf) as u64);
    values[3] = ((((bytes[19]) & 0xf) as u64) << 48)
        | (((bytes[20]) as u64) << 40)
        | (((bytes[21]) as u64) << 32)
        | (((bytes[22]) as u64) << 24)
        | (((bytes[23]) as u64) << 16)
        | (((bytes[24]) as u64) << 8)
        | ((bytes[25]) as u64);
    values[4] = (((bytes[26]) as u64) << 44)
        | (((bytes[27]) as u64) << 36)
        | (((bytes[28]) as u64) << 28)
        | (((bytes[29]) as u64) << 20)
        | (((bytes[30]) as u64) << 12)
        | (((bytes[31]) as u64) << 4)
        | (((bytes[32] >> 4) & 0xf) as u64);
    values[5] = ((((bytes[32]) & 0xf) as u64) << 48)
        | (((bytes[33]) as u64) << 40)
        | (((bytes[34]) as u64) << 32)
        | (((bytes[35]) as u64) << 24)
        | (((bytes[36]) as u64) << 16)
        | (((bytes[37]) as u64) << 8)
        | ((bytes[38]) as u64);
    values[6] = (((bytes[39]) as u64) << 44)
        | (((bytes[40]) as u64) << 36)
        | (((bytes[41]) as u64) << 28)
        | (((bytes[42]) as u64) << 20)
        | (((bytes[43]) as u64) << 12)
        | (((bytes[44]) as u64) << 4)
        | (((bytes[45] >> 4) & 0xf) as u64);
    values[7] = ((((bytes[45]) & 0xf) as u64) << 48)
        | (((bytes[46]) as u64) << 40)
        | (((bytes[47]) as u64) << 32)
        | (((bytes[48]) as u64) << 24)
        | (((bytes[49]) as u64) << 16)
        | (((bytes[50]) as u64) << 8)
        | ((bytes[51]) as u64);
}

#[inline]
fn unpack_bits_53(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 53);
    values[0] = (((bytes[0]) as u64) << 45)
        | (((bytes[1]) as u64) << 37)
        | (((bytes[2]) as u64) << 29)
        | (((bytes[3]) as u64) << 21)
        | (((bytes[4]) as u64) << 13)
        | (((bytes[5]) as u64) << 5)
        | (((bytes[6] >> 3) & 0x1f) as u64);
    values[1] = ((((bytes[6]) & 0x7) as u64) << 50)
        | (((bytes[7]) as u64) << 42)
        | (((bytes[8]) as u64) << 34)
        | (((bytes[9]) as u64) << 26)
        | (((bytes[10]) as u64) << 18)
        | (((bytes[11]) as u64) << 10)
        | (((bytes[12]) as u64) << 2)
        | (((bytes[13] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[13]) & 0x3f) as u64) << 47)
        | (((bytes[14]) as u64) << 39)
        | (((bytes[15]) as u64) << 31)
        | (((bytes[16]) as u64) << 23)
        | (((bytes[17]) as u64) << 15)
        | (((bytes[18]) as u64) << 7)
        | (((bytes[19] >> 1) & 0x7f) as u64);
    values[3] = ((((bytes[19]) & 0x1) as u64) << 52)
        | (((bytes[20]) as u64) << 44)
        | (((bytes[21]) as u64) << 36)
        | (((bytes[22]) as u64) << 28)
        | (((bytes[23]) as u64) << 20)
        | (((bytes[24]) as u64) << 12)
        | (((bytes[25]) as u64) << 4)
        | (((bytes[26] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[26]) & 0xf) as u64) << 49)
        | (((bytes[27]) as u64) << 41)
        | (((bytes[28]) as u64) << 33)
        | (((bytes[29]) as u64) << 25)
        | (((bytes[30]) as u64) << 17)
        | (((bytes[31]) as u64) << 9)
        | (((bytes[32]) as u64) << 1)
        | (((bytes[33] >> 7) & 0x1) as u64);
    values[5] = ((((bytes[33]) & 0x7f) as u64) << 46)
        | (((bytes[34]) as u64) << 38)
        | (((bytes[35]) as u64) << 30)
        | (((bytes[36]) as u64) << 22)
        | (((bytes[37]) as u64) << 14)
        | (((bytes[38]) as u64) << 6)
        | (((bytes[39] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[39]) & 0x3) as u64) << 51)
        | (((bytes[40]) as u64) << 43)
        | (((bytes[41]) as u64) << 35)
        | (((bytes[42]) as u64) << 27)
        | (((bytes[43]) as u64) << 19)
        | (((bytes[44]) as u64) << 11)
        | (((bytes[45]) as u64) << 3)
        | (((bytes[46] >> 5) & 0x7) as u64);
    values[7] = ((((bytes[46]) & 0x1f) as u64) << 48)
        | (((bytes[47]) as u64) << 40)
        | (((bytes[48]) as u64) << 32)
        | (((bytes[49]) as u64) << 24)
        | (((bytes[50]) as u64) << 16)
        | (((bytes[51]) as u64) << 8)
        | ((bytes[52]) as u64);
}

#[inline]
fn unpack_bits_54(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 54);
    values[0] = (((bytes[0]) as u64) << 46)
        | (((bytes[1]) as u64) << 38)
        | (((bytes[2]) as u64) << 30)
        | (((bytes[3]) as u64) << 22)
        | (((bytes[4]) as u64) << 14)
        | (((bytes[5]) as u64) << 6)
        | (((bytes[6] >> 2) & 0x3f) as u64);
    values[1] = ((((bytes[6]) & 0x3) as u64) << 52)
        | (((bytes[7]) as u64) << 44)
        | (((bytes[8]) as u64) << 36)
        | (((bytes[9]) as u64) << 28)
        | (((bytes[10]) as u64) << 20)
        | (((bytes[11]) as u64) << 12)
        | (((bytes[12]) as u64) << 4)
        | (((bytes[13] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[13]) & 0xf) as u64) << 50)
        | (((bytes[14]) as u64) << 42)
        | (((bytes[15]) as u64) << 34)
        | (((bytes[16]) as u64) << 26)
        | (((bytes[17]) as u64) << 18)
        | (((bytes[18]) as u64) << 10)
        | (((bytes[19]) as u64) << 2)
        | (((bytes[20] >> 6) & 0x3) as u64);
    values[3] = ((((bytes[20]) & 0x3f) as u64) << 48)
        | (((bytes[21]) as u64) << 40)
        | (((bytes[22]) as u64) << 32)
        | (((bytes[23]) as u64) << 24)
        | (((bytes[24]) as u64) << 16)
        | (((bytes[25]) as u64) << 8)
        | ((bytes[26]) as u64);
    values[4] = (((bytes[27]) as u64) << 46)
        | (((bytes[28]) as u64) << 38)
        | (((bytes[29]) as u64) << 30)
        | (((bytes[30]) as u64) << 22)
        | (((bytes[31]) as u64) << 14)
        | (((bytes[32]) as u64) << 6)
        | (((bytes[33] >> 2) & 0x3f) as u64);
    values[5] = ((((bytes[33]) & 0x3) as u64) << 52)
        | (((bytes[34]) as u64) << 44)
        | (((bytes[35]) as u64) << 36)
        | (((bytes[36]) as u64) << 28)
        | (((bytes[37]) as u64) << 20)
        | (((bytes[38]) as u64) << 12)
        | (((bytes[39]) as u64) << 4)
        | (((bytes[40] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[40]) & 0xf) as u64) << 50)
        | (((bytes[41]) as u64) << 42)
        | (((bytes[42]) as u64) << 34)
        | (((bytes[43]) as u64) << 26)
        | (((bytes[44]) as u64) << 18)
        | (((bytes[45]) as u64) << 10)
        | (((bytes[46]) as u64) << 2)
        | (((bytes[47] >> 6) & 0x3) as u64);
    values[7] = ((((bytes[47]) & 0x3f) as u64) << 48)
        | (((bytes[48]) as u64) << 40)
        | (((bytes[49]) as u64) << 32)
        | (((bytes[50]) as u64) << 24)
        | (((bytes[51]) as u64) << 16)
        | (((bytes[52]) as u64) << 8)
        | ((bytes[53]) as u64);
}

#[inline]
fn unpack_bits_55(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 55);
    values[0] = (((bytes[0]) as u64) << 47)
        | (((bytes[1]) as u64) << 39)
        | (((bytes[2]) as u64) << 31)
        | (((bytes[3]) as u64) << 23)
        | (((bytes[4]) as u64) << 15)
        | (((bytes[5]) as u64) << 7)
        | (((bytes[6] >> 1) & 0x7f) as u64);
    values[1] = ((((bytes[6]) & 0x1) as u64) << 54)
        | (((bytes[7]) as u64) << 46)
        | (((bytes[8]) as u64) << 38)
        | (((bytes[9]) as u64) << 30)
        | (((bytes[10]) as u64) << 22)
        | (((bytes[11]) as u64) << 14)
        | (((bytes[12]) as u64) << 6)
        | (((bytes[13] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[13]) & 0x3) as u64) << 53)
        | (((bytes[14]) as u64) << 45)
        | (((bytes[15]) as u64) << 37)
        | (((bytes[16]) as u64) << 29)
        | (((bytes[17]) as u64) << 21)
        | (((bytes[18]) as u64) << 13)
        | (((bytes[19]) as u64) << 5)
        | (((bytes[20] >> 3) & 0x1f) as u64);
    values[3] = ((((bytes[20]) & 0x7) as u64) << 52)
        | (((bytes[21]) as u64) << 44)
        | (((bytes[22]) as u64) << 36)
        | (((bytes[23]) as u64) << 28)
        | (((bytes[24]) as u64) << 20)
        | (((bytes[25]) as u64) << 12)
        | (((bytes[26]) as u64) << 4)
        | (((bytes[27] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[27]) & 0xf) as u64) << 51)
        | (((bytes[28]) as u64) << 43)
        | (((bytes[29]) as u64) << 35)
        | (((bytes[30]) as u64) << 27)
        | (((bytes[31]) as u64) << 19)
        | (((bytes[32]) as u64) << 11)
        | (((bytes[33]) as u64) << 3)
        | (((bytes[34] >> 5) & 0x7) as u64);
    values[5] = ((((bytes[34]) & 0x1f) as u64) << 50)
        | (((bytes[35]) as u64) << 42)
        | (((bytes[36]) as u64) << 34)
        | (((bytes[37]) as u64) << 26)
        | (((bytes[38]) as u64) << 18)
        | (((bytes[39]) as u64) << 10)
        | (((bytes[40]) as u64) << 2)
        | (((bytes[41] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[41]) & 0x3f) as u64) << 49)
        | (((bytes[42]) as u64) << 41)
        | (((bytes[43]) as u64) << 33)
        | (((bytes[44]) as u64) << 25)
        | (((bytes[45]) as u64) << 17)
        | (((bytes[46]) as u64) << 9)
        | (((bytes[47]) as u64) << 1)
        | (((bytes[48] >> 7) & 0x1) as u64);
    values[7] = ((((bytes[48]) & 0x7f) as u64) << 48)
        | (((bytes[49]) as u64) << 40)
        | (((bytes[50]) as u64) << 32)
        | (((bytes[51]) as u64) << 24)
        | (((bytes[52]) as u64) << 16)
        | (((bytes[53]) as u64) << 8)
        | ((bytes[54]) as u64);
}

#[inline]
fn unpack_bits_56(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 56);
    values[0] = (((bytes[0]) as u64) << 48)
        | (((bytes[1]) as u64) << 40)
        | (((bytes[2]) as u64) << 32)
        | (((bytes[3]) as u64) << 24)
        | (((bytes[4]) as u64) << 16)
        | (((bytes[5]) as u64) << 8)
        | ((bytes[6]) as u64);
    values[1] = (((bytes[7]) as u64) << 48)
        | (((bytes[8]) as u64) << 40)
        | (((bytes[9]) as u64) << 32)
        | (((bytes[10]) as u64) << 24)
        | (((bytes[11]) as u64) << 16)
        | (((bytes[12]) as u64) << 8)
        | ((bytes[13]) as u64);
    values[2] = (((bytes[14]) as u64) << 48)
        | (((bytes[15]) as u64) << 40)
        | (((bytes[16]) as u64) << 32)
        | (((bytes[17]) as u64) << 24)
        | (((bytes[18]) as u64) << 16)
        | (((bytes[19]) as u64) << 8)
        | ((bytes[20]) as u64);
    values[3] = (((bytes[21]) as u64) << 48)
        | (((bytes[22]) as u64) << 40)
        | (((bytes[23]) as u64) << 32)
        | (((bytes[24]) as u64) << 24)
        | (((bytes[25]) as u64) << 16)
        | (((bytes[26]) as u64) << 8)
        | ((bytes[27]) as u64);
    values[4] = (((bytes[28]) as u64) << 48)
        | (((bytes[29]) as u64) << 40)
        | (((bytes[30]) as u64) << 32)
        | (((bytes[31]) as u64) << 24)
        | (((bytes[32]) as u64) << 16)
        | (((bytes[33]) as u64) << 8)
        | ((bytes[34]) as u64);
    values[5] = (((bytes[35]) as u64) << 48)
        | (((bytes[36]) as u64) << 40)
        | (((bytes[37]) as u64) << 32)
        | (((bytes[38]) as u64) << 24)
        | (((bytes[39]) as u64) << 16)
        | (((bytes[40]) as u64) << 8)
        | ((bytes[41]) as u64);
    values[6] = (((bytes[42]) as u64) << 48)
        | (((bytes[43]) as u64) << 40)
        | (((bytes[44]) as u64) << 32)
        | (((bytes[45]) as u64) << 24)
        | (((bytes[46]) as u64) << 16)
        | (((bytes[47]) as u64) << 8)
        | ((bytes[48]) as u64);
    values[7] = (((bytes[49]) as u64) << 48)
        | (((bytes[50]) as u64) << 40)
        | (((bytes[51]) as u64) << 32)
        | (((bytes[52]) as u64) << 24)
        | (((bytes[53]) as u64) << 16)
        | (((bytes[54]) as u64) << 8)
        | ((bytes[55]) as u64);
}

#[inline]
fn unpack_bits_57(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 57);
    values[0] = (((bytes[0]) as u64) << 49)
        | (((bytes[1]) as u64) << 41)
        | (((bytes[2]) as u64) << 33)
        | (((bytes[3]) as u64) << 25)
        | (((bytes[4]) as u64) << 17)
        | (((bytes[5]) as u64) << 9)
        | (((bytes[6]) as u64) << 1)
        | (((bytes[7] >> 7) & 0x1) as u64);
    values[1] = ((((bytes[7]) & 0x7f) as u64) << 50)
        | (((bytes[8]) as u64) << 42)
        | (((bytes[9]) as u64) << 34)
        | (((bytes[10]) as u64) << 26)
        | (((bytes[11]) as u64) << 18)
        | (((bytes[12]) as u64) << 10)
        | (((bytes[13]) as u64) << 2)
        | (((bytes[14] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[14]) & 0x3f) as u64) << 51)
        | (((bytes[15]) as u64) << 43)
        | (((bytes[16]) as u64) << 35)
        | (((bytes[17]) as u64) << 27)
        | (((bytes[18]) as u64) << 19)
        | (((bytes[19]) as u64) << 11)
        | (((bytes[20]) as u64) << 3)
        | (((bytes[21] >> 5) & 0x7) as u64);
    values[3] = ((((bytes[21]) & 0x1f) as u64) << 52)
        | (((bytes[22]) as u64) << 44)
        | (((bytes[23]) as u64) << 36)
        | (((bytes[24]) as u64) << 28)
        | (((bytes[25]) as u64) << 20)
        | (((bytes[26]) as u64) << 12)
        | (((bytes[27]) as u64) << 4)
        | (((bytes[28] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[28]) & 0xf) as u64) << 53)
        | (((bytes[29]) as u64) << 45)
        | (((bytes[30]) as u64) << 37)
        | (((bytes[31]) as u64) << 29)
        | (((bytes[32]) as u64) << 21)
        | (((bytes[33]) as u64) << 13)
        | (((bytes[34]) as u64) << 5)
        | (((bytes[35] >> 3) & 0x1f) as u64);
    values[5] = ((((bytes[35]) & 0x7) as u64) << 54)
        | (((bytes[36]) as u64) << 46)
        | (((bytes[37]) as u64) << 38)
        | (((bytes[38]) as u64) << 30)
        | (((bytes[39]) as u64) << 22)
        | (((bytes[40]) as u64) << 14)
        | (((bytes[41]) as u64) << 6)
        | (((bytes[42] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[42]) & 0x3) as u64) << 55)
        | (((bytes[43]) as u64) << 47)
        | (((bytes[44]) as u64) << 39)
        | (((bytes[45]) as u64) << 31)
        | (((bytes[46]) as u64) << 23)
        | (((bytes[47]) as u64) << 15)
        | (((bytes[48]) as u64) << 7)
        | (((bytes[49] >> 1) & 0x7f) as u64);
    values[7] = ((((bytes[49]) & 0x1) as u64) << 56)
        | (((bytes[50]) as u64) << 48)
        | (((bytes[51]) as u64) << 40)
        | (((bytes[52]) as u64) << 32)
        | (((bytes[53]) as u64) << 24)
        | (((bytes[54]) as u64) << 16)
        | (((bytes[55]) as u64) << 8)
        | ((bytes[56]) as u64);
}

#[inline]
fn unpack_bits_58(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 58);
    values[0] = (((bytes[0]) as u64) << 50)
        | (((bytes[1]) as u64) << 42)
        | (((bytes[2]) as u64) << 34)
        | (((bytes[3]) as u64) << 26)
        | (((bytes[4]) as u64) << 18)
        | (((bytes[5]) as u64) << 10)
        | (((bytes[6]) as u64) << 2)
        | (((bytes[7] >> 6) & 0x3) as u64);
    values[1] = ((((bytes[7]) & 0x3f) as u64) << 52)
        | (((bytes[8]) as u64) << 44)
        | (((bytes[9]) as u64) << 36)
        | (((bytes[10]) as u64) << 28)
        | (((bytes[11]) as u64) << 20)
        | (((bytes[12]) as u64) << 12)
        | (((bytes[13]) as u64) << 4)
        | (((bytes[14] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[14]) & 0xf) as u64) << 54)
        | (((bytes[15]) as u64) << 46)
        | (((bytes[16]) as u64) << 38)
        | (((bytes[17]) as u64) << 30)
        | (((bytes[18]) as u64) << 22)
        | (((bytes[19]) as u64) << 14)
        | (((bytes[20]) as u64) << 6)
        | (((bytes[21] >> 2) & 0x3f) as u64);
    values[3] = ((((bytes[21]) & 0x3) as u64) << 56)
        | (((bytes[22]) as u64) << 48)
        | (((bytes[23]) as u64) << 40)
        | (((bytes[24]) as u64) << 32)
        | (((bytes[25]) as u64) << 24)
        | (((bytes[26]) as u64) << 16)
        | (((bytes[27]) as u64) << 8)
        | ((bytes[28]) as u64);
    values[4] = (((bytes[29]) as u64) << 50)
        | (((bytes[30]) as u64) << 42)
        | (((bytes[31]) as u64) << 34)
        | (((bytes[32]) as u64) << 26)
        | (((bytes[33]) as u64) << 18)
        | (((bytes[34]) as u64) << 10)
        | (((bytes[35]) as u64) << 2)
        | (((bytes[36] >> 6) & 0x3) as u64);
    values[5] = ((((bytes[36]) & 0x3f) as u64) << 52)
        | (((bytes[37]) as u64) << 44)
        | (((bytes[38]) as u64) << 36)
        | (((bytes[39]) as u64) << 28)
        | (((bytes[40]) as u64) << 20)
        | (((bytes[41]) as u64) << 12)
        | (((bytes[42]) as u64) << 4)
        | (((bytes[43] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[43]) & 0xf) as u64) << 54)
        | (((bytes[44]) as u64) << 46)
        | (((bytes[45]) as u64) << 38)
        | (((bytes[46]) as u64) << 30)
        | (((bytes[47]) as u64) << 22)
        | (((bytes[48]) as u64) << 14)
        | (((bytes[49]) as u64) << 6)
        | (((bytes[50] >> 2) & 0x3f) as u64);
    values[7] = ((((bytes[50]) & 0x3) as u64) << 56)
        | (((bytes[51]) as u64) << 48)
        | (((bytes[52]) as u64) << 40)
        | (((bytes[53]) as u64) << 32)
        | (((bytes[54]) as u64) << 24)
        | (((bytes[55]) as u64) << 16)
        | (((bytes[56]) as u64) << 8)
        | ((bytes[57]) as u64);
}

#[inline]
fn unpack_bits_59(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 59);
    values[0] = (((bytes[0]) as u64) << 51)
        | (((bytes[1]) as u64) << 43)
        | (((bytes[2]) as u64) << 35)
        | (((bytes[3]) as u64) << 27)
        | (((bytes[4]) as u64) << 19)
        | (((bytes[5]) as u64) << 11)
        | (((bytes[6]) as u64) << 3)
        | (((bytes[7] >> 5) & 0x7) as u64);
    values[1] = ((((bytes[7]) & 0x1f) as u64) << 54)
        | (((bytes[8]) as u64) << 46)
        | (((bytes[9]) as u64) << 38)
        | (((bytes[10]) as u64) << 30)
        | (((bytes[11]) as u64) << 22)
        | (((bytes[12]) as u64) << 14)
        | (((bytes[13]) as u64) << 6)
        | (((bytes[14] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[14]) & 0x3) as u64) << 57)
        | (((bytes[15]) as u64) << 49)
        | (((bytes[16]) as u64) << 41)
        | (((bytes[17]) as u64) << 33)
        | (((bytes[18]) as u64) << 25)
        | (((bytes[19]) as u64) << 17)
        | (((bytes[20]) as u64) << 9)
        | (((bytes[21]) as u64) << 1)
        | (((bytes[22] >> 7) & 0x1) as u64);
    values[3] = ((((bytes[22]) & 0x7f) as u64) << 52)
        | (((bytes[23]) as u64) << 44)
        | (((bytes[24]) as u64) << 36)
        | (((bytes[25]) as u64) << 28)
        | (((bytes[26]) as u64) << 20)
        | (((bytes[27]) as u64) << 12)
        | (((bytes[28]) as u64) << 4)
        | (((bytes[29] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[29]) & 0xf) as u64) << 55)
        | (((bytes[30]) as u64) << 47)
        | (((bytes[31]) as u64) << 39)
        | (((bytes[32]) as u64) << 31)
        | (((bytes[33]) as u64) << 23)
        | (((bytes[34]) as u64) << 15)
        | (((bytes[35]) as u64) << 7)
        | (((bytes[36] >> 1) & 0x7f) as u64);
    values[5] = ((((bytes[36]) & 0x1) as u64) << 58)
        | (((bytes[37]) as u64) << 50)
        | (((bytes[38]) as u64) << 42)
        | (((bytes[39]) as u64) << 34)
        | (((bytes[40]) as u64) << 26)
        | (((bytes[41]) as u64) << 18)
        | (((bytes[42]) as u64) << 10)
        | (((bytes[43]) as u64) << 2)
        | (((bytes[44] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[44]) & 0x3f) as u64) << 53)
        | (((bytes[45]) as u64) << 45)
        | (((bytes[46]) as u64) << 37)
        | (((bytes[47]) as u64) << 29)
        | (((bytes[48]) as u64) << 21)
        | (((bytes[49]) as u64) << 13)
        | (((bytes[50]) as u64) << 5)
        | (((bytes[51] >> 3) & 0x1f) as u64);
    values[7] = ((((bytes[51]) & 0x7) as u64) << 56)
        | (((bytes[52]) as u64) << 48)
        | (((bytes[53]) as u64) << 40)
        | (((bytes[54]) as u64) << 32)
        | (((bytes[55]) as u64) << 24)
        | (((bytes[56]) as u64) << 16)
        | (((bytes[57]) as u64) << 8)
        | ((bytes[58]) as u64);
}

#[inline]
fn unpack_bits_60(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 60);
    values[0] = (((bytes[0]) as u64) << 52)
        | (((bytes[1]) as u64) << 44)
        | (((bytes[2]) as u64) << 36)
        | (((bytes[3]) as u64) << 28)
        | (((bytes[4]) as u64) << 20)
        | (((bytes[5]) as u64) << 12)
        | (((bytes[6]) as u64) << 4)
        | (((bytes[7] >> 4) & 0xf) as u64);
    values[1] = ((((bytes[7]) & 0xf) as u64) << 56)
        | (((bytes[8]) as u64) << 48)
        | (((bytes[9]) as u64) << 40)
        | (((bytes[10]) as u64) << 32)
        | (((bytes[11]) as u64) << 24)
        | (((bytes[12]) as u64) << 16)
        | (((bytes[13]) as u64) << 8)
        | ((bytes[14]) as u64);
    values[2] = (((bytes[15]) as u64) << 52)
        | (((bytes[16]) as u64) << 44)
        | (((bytes[17]) as u64) << 36)
        | (((bytes[18]) as u64) << 28)
        | (((bytes[19]) as u64) << 20)
        | (((bytes[20]) as u64) << 12)
        | (((bytes[21]) as u64) << 4)
        | (((bytes[22] >> 4) & 0xf) as u64);
    values[3] = ((((bytes[22]) & 0xf) as u64) << 56)
        | (((bytes[23]) as u64) << 48)
        | (((bytes[24]) as u64) << 40)
        | (((bytes[25]) as u64) << 32)
        | (((bytes[26]) as u64) << 24)
        | (((bytes[27]) as u64) << 16)
        | (((bytes[28]) as u64) << 8)
        | ((bytes[29]) as u64);
    values[4] = (((bytes[30]) as u64) << 52)
        | (((bytes[31]) as u64) << 44)
        | (((bytes[32]) as u64) << 36)
        | (((bytes[33]) as u64) << 28)
        | (((bytes[34]) as u64) << 20)
        | (((bytes[35]) as u64) << 12)
        | (((bytes[36]) as u64) << 4)
        | (((bytes[37] >> 4) & 0xf) as u64);
    values[5] = ((((bytes[37]) & 0xf) as u64) << 56)
        | (((bytes[38]) as u64) << 48)
        | (((bytes[39]) as u64) << 40)
        | (((bytes[40]) as u64) << 32)
        | (((bytes[41]) as u64) << 24)
        | (((bytes[42]) as u64) << 16)
        | (((bytes[43]) as u64) << 8)
        | ((bytes[44]) as u64);
    values[6] = (((bytes[45]) as u64) << 52)
        | (((bytes[46]) as u64) << 44)
        | (((bytes[47]) as u64) << 36)
        | (((bytes[48]) as u64) << 28)
        | (((bytes[49]) as u64) << 20)
        | (((bytes[50]) as u64) << 12)
        | (((bytes[51]) as u64) << 4)
        | (((bytes[52] >> 4) & 0xf) as u64);
    values[7] = ((((bytes[52]) & 0xf) as u64) << 56)
        | (((bytes[53]) as u64) << 48)
        | (((bytes[54]) as u64) << 40)
        | (((bytes[55]) as u64) << 32)
        | (((bytes[56]) as u64) << 24)
        | (((bytes[57]) as u64) << 16)
        | (((bytes[58]) as u64) << 8)
        | ((bytes[59]) as u64);
}

#[inline]
fn unpack_bits_61(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 61);
    values[0] = (((bytes[0]) as u64) << 53)
        | (((bytes[1]) as u64) << 45)
        | (((bytes[2]) as u64) << 37)
        | (((bytes[3]) as u64) << 29)
        | (((bytes[4]) as u64) << 21)
        | (((bytes[5]) as u64) << 13)
        | (((bytes[6]) as u64) << 5)
        | (((bytes[7] >> 3) & 0x1f) as u64);
    values[1] = ((((bytes[7]) & 0x7) as u64) << 58)
        | (((bytes[8]) as u64) << 50)
        | (((bytes[9]) as u64) << 42)
        | (((bytes[10]) as u64) << 34)
        | (((bytes[11]) as u64) << 26)
        | (((bytes[12]) as u64) << 18)
        | (((bytes[13]) as u64) << 10)
        | (((bytes[14]) as u64) << 2)
        | (((bytes[15] >> 6) & 0x3) as u64);
    values[2] = ((((bytes[15]) & 0x3f) as u64) << 55)
        | (((bytes[16]) as u64) << 47)
        | (((bytes[17]) as u64) << 39)
        | (((bytes[18]) as u64) << 31)
        | (((bytes[19]) as u64) << 23)
        | (((bytes[20]) as u64) << 15)
        | (((bytes[21]) as u64) << 7)
        | (((bytes[22] >> 1) & 0x7f) as u64);
    values[3] = ((((bytes[22]) & 0x1) as u64) << 60)
        | (((bytes[23]) as u64) << 52)
        | (((bytes[24]) as u64) << 44)
        | (((bytes[25]) as u64) << 36)
        | (((bytes[26]) as u64) << 28)
        | (((bytes[27]) as u64) << 20)
        | (((bytes[28]) as u64) << 12)
        | (((bytes[29]) as u64) << 4)
        | (((bytes[30] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[30]) & 0xf) as u64) << 57)
        | (((bytes[31]) as u64) << 49)
        | (((bytes[32]) as u64) << 41)
        | (((bytes[33]) as u64) << 33)
        | (((bytes[34]) as u64) << 25)
        | (((bytes[35]) as u64) << 17)
        | (((bytes[36]) as u64) << 9)
        | (((bytes[37]) as u64) << 1)
        | (((bytes[38] >> 7) & 0x1) as u64);
    values[5] = ((((bytes[38]) & 0x7f) as u64) << 54)
        | (((bytes[39]) as u64) << 46)
        | (((bytes[40]) as u64) << 38)
        | (((bytes[41]) as u64) << 30)
        | (((bytes[42]) as u64) << 22)
        | (((bytes[43]) as u64) << 14)
        | (((bytes[44]) as u64) << 6)
        | (((bytes[45] >> 2) & 0x3f) as u64);
    values[6] = ((((bytes[45]) & 0x3) as u64) << 59)
        | (((bytes[46]) as u64) << 51)
        | (((bytes[47]) as u64) << 43)
        | (((bytes[48]) as u64) << 35)
        | (((bytes[49]) as u64) << 27)
        | (((bytes[50]) as u64) << 19)
        | (((bytes[51]) as u64) << 11)
        | (((bytes[52]) as u64) << 3)
        | (((bytes[53] >> 5) & 0x7) as u64);
    values[7] = ((((bytes[53]) & 0x1f) as u64) << 56)
        | (((bytes[54]) as u64) << 48)
        | (((bytes[55]) as u64) << 40)
        | (((bytes[56]) as u64) << 32)
        | (((bytes[57]) as u64) << 24)
        | (((bytes[58]) as u64) << 16)
        | (((bytes[59]) as u64) << 8)
        | ((bytes[60]) as u64);
}

#[inline]
fn unpack_bits_62(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 62);
    values[0] = (((bytes[0]) as u64) << 54)
        | (((bytes[1]) as u64) << 46)
        | (((bytes[2]) as u64) << 38)
        | (((bytes[3]) as u64) << 30)
        | (((bytes[4]) as u64) << 22)
        | (((bytes[5]) as u64) << 14)
        | (((bytes[6]) as u64) << 6)
        | (((bytes[7] >> 2) & 0x3f) as u64);
    values[1] = ((((bytes[7]) & 0x3) as u64) << 60)
        | (((bytes[8]) as u64) << 52)
        | (((bytes[9]) as u64) << 44)
        | (((bytes[10]) as u64) << 36)
        | (((bytes[11]) as u64) << 28)
        | (((bytes[12]) as u64) << 20)
        | (((bytes[13]) as u64) << 12)
        | (((bytes[14]) as u64) << 4)
        | (((bytes[15] >> 4) & 0xf) as u64);
    values[2] = ((((bytes[15]) & 0xf) as u64) << 58)
        | (((bytes[16]) as u64) << 50)
        | (((bytes[17]) as u64) << 42)
        | (((bytes[18]) as u64) << 34)
        | (((bytes[19]) as u64) << 26)
        | (((bytes[20]) as u64) << 18)
        | (((bytes[21]) as u64) << 10)
        | (((bytes[22]) as u64) << 2)
        | (((bytes[23] >> 6) & 0x3) as u64);
    values[3] = ((((bytes[23]) & 0x3f) as u64) << 56)
        | (((bytes[24]) as u64) << 48)
        | (((bytes[25]) as u64) << 40)
        | (((bytes[26]) as u64) << 32)
        | (((bytes[27]) as u64) << 24)
        | (((bytes[28]) as u64) << 16)
        | (((bytes[29]) as u64) << 8)
        | ((bytes[30]) as u64);
    values[4] = (((bytes[31]) as u64) << 54)
        | (((bytes[32]) as u64) << 46)
        | (((bytes[33]) as u64) << 38)
        | (((bytes[34]) as u64) << 30)
        | (((bytes[35]) as u64) << 22)
        | (((bytes[36]) as u64) << 14)
        | (((bytes[37]) as u64) << 6)
        | (((bytes[38] >> 2) & 0x3f) as u64);
    values[5] = ((((bytes[38]) & 0x3) as u64) << 60)
        | (((bytes[39]) as u64) << 52)
        | (((bytes[40]) as u64) << 44)
        | (((bytes[41]) as u64) << 36)
        | (((bytes[42]) as u64) << 28)
        | (((bytes[43]) as u64) << 20)
        | (((bytes[44]) as u64) << 12)
        | (((bytes[45]) as u64) << 4)
        | (((bytes[46] >> 4) & 0xf) as u64);
    values[6] = ((((bytes[46]) & 0xf) as u64) << 58)
        | (((bytes[47]) as u64) << 50)
        | (((bytes[48]) as u64) << 42)
        | (((bytes[49]) as u64) << 34)
        | (((bytes[50]) as u64) << 26)
        | (((bytes[51]) as u64) << 18)
        | (((bytes[52]) as u64) << 10)
        | (((bytes[53]) as u64) << 2)
        | (((bytes[54] >> 6) & 0x3) as u64);
    values[7] = ((((bytes[54]) & 0x3f) as u64) << 56)
        | (((bytes[55]) as u64) << 48)
        | (((bytes[56]) as u64) << 40)
        | (((bytes[57]) as u64) << 32)
        | (((bytes[58]) as u64) << 24)
        | (((bytes[59]) as u64) << 16)
        | (((bytes[60]) as u64) << 8)
        | ((bytes[61]) as u64);
}

#[inline]
fn unpack_bits_63(values: &mut [u64], bytes: &[u8]) {
    debug_assert!(values.len() >= BLOCK_WIDTH);
    debug_assert!(bytes.len() >= 63);
    values[0] = (((bytes[0]) as u64) << 55)
        | (((bytes[1]) as u64) << 47)
        | (((bytes[2]) as u64) << 39)
        | (((bytes[3]) as u64) << 31)
        | (((bytes[4]) as u64) << 23)
        | (((bytes[5]) as u64) << 15)
        | (((bytes[6]) as u64) << 7)
        | (((bytes[7] >> 1) & 0x7f) as u64);
    values[1] = ((((bytes[7]) & 0x1) as u64) << 62)
        | (((bytes[8]) as u64) << 54)
        | (((bytes[9]) as u64) << 46)
        | (((bytes[10]) as u64) << 38)
        | (((bytes[11]) as u64) << 30)
        | (((bytes[12]) as u64) << 22)
        | (((bytes[13]) as u64) << 14)
        | (((bytes[14]) as u64) << 6)
        | (((bytes[15] >> 2) & 0x3f) as u64);
    values[2] = ((((bytes[15]) & 0x3) as u64) << 61)
        | (((bytes[16]) as u64) << 53)
        | (((bytes[17]) as u64) << 45)
        | (((bytes[18]) as u64) << 37)
        | (((bytes[19]) as u64) << 29)
        | (((bytes[20]) as u64) << 21)
        | (((bytes[21]) as u64) << 13)
        | (((bytes[22]) as u64) << 5)
        | (((bytes[23] >> 3) & 0x1f) as u64);
    values[3] = ((((bytes[23]) & 0x7) as u64) << 60)
        | (((bytes[24]) as u64) << 52)
        | (((bytes[25]) as u64) << 44)
        | (((bytes[26]) as u64) << 36)
        | (((bytes[27]) as u64) << 28)
        | (((bytes[28]) as u64) << 20)
        | (((bytes[29]) as u64) << 12)
        | (((bytes[30]) as u64) << 4)
        | (((bytes[31] >> 4) & 0xf) as u64);
    values[4] = ((((bytes[31]) & 0xf) as u64) << 59)
        | (((bytes[32]) as u64) << 51)
        | (((bytes[33]) as u64) << 43)
        | (((bytes[34]) as u64) << 35)
        | (((bytes[35]) as u64) << 27)
        | (((bytes[36]) as u64) << 19)
        | (((bytes[37]) as u64) << 11)
        | (((bytes[38]) as u64) << 3)
        | (((bytes[39] >> 5) & 0x7) as u64);
    values[5] = ((((bytes[39]) & 0x1f) as u64) << 58)
        | (((bytes[40]) as u64) << 50)
        | (((bytes[41]) as u64) << 42)
        | (((bytes[42]) as u64) << 34)
        | (((bytes[43]) as u64) << 26)
        | (((bytes[44]) as u64) << 18)
        | (((bytes[45]) as u64) << 10)
        | (((bytes[46]) as u64) << 2)
        | (((bytes[47] >> 6) & 0x3) as u64);
    values[6] = ((((bytes[47]) & 0x3f) as u64) << 57)
        | (((bytes[48]) as u64) << 49)
        | (((bytes[49]) as u64) << 41)
        | (((bytes[50]) as u64) << 33)
        | (((bytes[51]) as u64) << 25)
        | (((bytes[52]) as u64) << 17)
        | (((bytes[53]) as u64) << 9)
        | (((bytes[54]) as u64) << 1)
        | (((bytes[55] >> 7) & 0x1) as u64);
    values[7] = ((((bytes[55]) & 0x7f) as u64) << 56)
        | (((bytes[56]) as u64) << 48)
        | (((bytes[57]) as u64) << 40)
        | (((bytes[58]) as u64) << 32)
        | (((bytes[59]) as u64) << 24)
        | (((bytes[60]) as u64) << 16)
        | (((bytes[61]) as u64) << 8)
        | ((bytes[62]) as u64);
}

/// Packs a block of 8 values using fully-expanded fixed-width bit packing.
///
/// `bits` must be in `1..=63`.
pub(crate) fn pack_bits_block8(values: &[u64], bytes: &mut [u8], bits: u8) {
    assert_eq!(values.len(), BLOCK_WIDTH, "values length must be 8");
    assert!(
        (1..=63).contains(&bits),
        "wrong number of bits in pack_bits_block8: {bits}"
    );
    assert!(bytes.len() >= bits as usize, "output buffer too small");

    match bits {
        1 => pack_bits_1(values, bytes),
        2 => pack_bits_2(values, bytes),
        3 => pack_bits_3(values, bytes),
        4 => pack_bits_4(values, bytes),
        5 => pack_bits_5(values, bytes),
        6 => pack_bits_6(values, bytes),
        7 => pack_bits_7(values, bytes),
        8 => pack_bits_8(values, bytes),
        9 => pack_bits_9(values, bytes),
        10 => pack_bits_10(values, bytes),
        11 => pack_bits_11(values, bytes),
        12 => pack_bits_12(values, bytes),
        13 => pack_bits_13(values, bytes),
        14 => pack_bits_14(values, bytes),
        15 => pack_bits_15(values, bytes),
        16 => pack_bits_16(values, bytes),
        17 => pack_bits_17(values, bytes),
        18 => pack_bits_18(values, bytes),
        19 => pack_bits_19(values, bytes),
        20 => pack_bits_20(values, bytes),
        21 => pack_bits_21(values, bytes),
        22 => pack_bits_22(values, bytes),
        23 => pack_bits_23(values, bytes),
        24 => pack_bits_24(values, bytes),
        25 => pack_bits_25(values, bytes),
        26 => pack_bits_26(values, bytes),
        27 => pack_bits_27(values, bytes),
        28 => pack_bits_28(values, bytes),
        29 => pack_bits_29(values, bytes),
        30 => pack_bits_30(values, bytes),
        31 => pack_bits_31(values, bytes),
        32 => pack_bits_32(values, bytes),
        33 => pack_bits_33(values, bytes),
        34 => pack_bits_34(values, bytes),
        35 => pack_bits_35(values, bytes),
        36 => pack_bits_36(values, bytes),
        37 => pack_bits_37(values, bytes),
        38 => pack_bits_38(values, bytes),
        39 => pack_bits_39(values, bytes),
        40 => pack_bits_40(values, bytes),
        41 => pack_bits_41(values, bytes),
        42 => pack_bits_42(values, bytes),
        43 => pack_bits_43(values, bytes),
        44 => pack_bits_44(values, bytes),
        45 => pack_bits_45(values, bytes),
        46 => pack_bits_46(values, bytes),
        47 => pack_bits_47(values, bytes),
        48 => pack_bits_48(values, bytes),
        49 => pack_bits_49(values, bytes),
        50 => pack_bits_50(values, bytes),
        51 => pack_bits_51(values, bytes),
        52 => pack_bits_52(values, bytes),
        53 => pack_bits_53(values, bytes),
        54 => pack_bits_54(values, bytes),
        55 => pack_bits_55(values, bytes),
        56 => pack_bits_56(values, bytes),
        57 => pack_bits_57(values, bytes),
        58 => pack_bits_58(values, bytes),
        59 => pack_bits_59(values, bytes),
        60 => pack_bits_60(values, bytes),
        61 => pack_bits_61(values, bytes),
        62 => pack_bits_62(values, bytes),
        63 => pack_bits_63(values, bytes),
        _ => unreachable!(),
    }
}

/// Unpacks a block of 8 values using fully-expanded fixed-width bit unpacking.
///
/// `bits` must be in `1..=63`.
pub(crate) fn unpack_bits_block8(values: &mut [u64], bytes: &[u8], bits: u8) {
    assert_eq!(values.len(), BLOCK_WIDTH, "values length must be 8");
    assert!(
        (1..=63).contains(&bits),
        "wrong number of bits in unpack_bits_block8: {bits}"
    );
    assert!(bytes.len() >= bits as usize, "input buffer too small");

    match bits {
        1 => unpack_bits_1(values, bytes),
        2 => unpack_bits_2(values, bytes),
        3 => unpack_bits_3(values, bytes),
        4 => unpack_bits_4(values, bytes),
        5 => unpack_bits_5(values, bytes),
        6 => unpack_bits_6(values, bytes),
        7 => unpack_bits_7(values, bytes),
        8 => unpack_bits_8(values, bytes),
        9 => unpack_bits_9(values, bytes),
        10 => unpack_bits_10(values, bytes),
        11 => unpack_bits_11(values, bytes),
        12 => unpack_bits_12(values, bytes),
        13 => unpack_bits_13(values, bytes),
        14 => unpack_bits_14(values, bytes),
        15 => unpack_bits_15(values, bytes),
        16 => unpack_bits_16(values, bytes),
        17 => unpack_bits_17(values, bytes),
        18 => unpack_bits_18(values, bytes),
        19 => unpack_bits_19(values, bytes),
        20 => unpack_bits_20(values, bytes),
        21 => unpack_bits_21(values, bytes),
        22 => unpack_bits_22(values, bytes),
        23 => unpack_bits_23(values, bytes),
        24 => unpack_bits_24(values, bytes),
        25 => unpack_bits_25(values, bytes),
        26 => unpack_bits_26(values, bytes),
        27 => unpack_bits_27(values, bytes),
        28 => unpack_bits_28(values, bytes),
        29 => unpack_bits_29(values, bytes),
        30 => unpack_bits_30(values, bytes),
        31 => unpack_bits_31(values, bytes),
        32 => unpack_bits_32(values, bytes),
        33 => unpack_bits_33(values, bytes),
        34 => unpack_bits_34(values, bytes),
        35 => unpack_bits_35(values, bytes),
        36 => unpack_bits_36(values, bytes),
        37 => unpack_bits_37(values, bytes),
        38 => unpack_bits_38(values, bytes),
        39 => unpack_bits_39(values, bytes),
        40 => unpack_bits_40(values, bytes),
        41 => unpack_bits_41(values, bytes),
        42 => unpack_bits_42(values, bytes),
        43 => unpack_bits_43(values, bytes),
        44 => unpack_bits_44(values, bytes),
        45 => unpack_bits_45(values, bytes),
        46 => unpack_bits_46(values, bytes),
        47 => unpack_bits_47(values, bytes),
        48 => unpack_bits_48(values, bytes),
        49 => unpack_bits_49(values, bytes),
        50 => unpack_bits_50(values, bytes),
        51 => unpack_bits_51(values, bytes),
        52 => unpack_bits_52(values, bytes),
        53 => unpack_bits_53(values, bytes),
        54 => unpack_bits_54(values, bytes),
        55 => unpack_bits_55(values, bytes),
        56 => unpack_bits_56(values, bytes),
        57 => unpack_bits_57(values, bytes),
        58 => unpack_bits_58(values, bytes),
        59 => unpack_bits_59(values, bytes),
        60 => unpack_bits_60(values, bytes),
        61 => unpack_bits_61(values, bytes),
        62 => unpack_bits_62(values, bytes),
        63 => unpack_bits_63(values, bytes),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // inverse golden ratio (0.618.. of max uint64_t)
    const IGOLDEN64: u64 = 0x9e37_79b9_7f4a_7c13;

    #[test]
    fn pack_unpack_bits() {
        let mut value = 0xaa55_aa55_aa55_aa55u64; // arbitrary starting value
        for _m in 0..10000 {
            for bits in 1u8..=63 {
                let n = 8usize;
                let mask = (1u64 << bits) - 1;
                let mut input = vec![0u64; n];
                for item in &mut input {
                    *item = value & mask;
                    value = value.wrapping_add(IGOLDEN64);
                }

                let mut bytes = vec![0u8; n * std::mem::size_of::<u64>()];
                let mut packer = BitPacker::new(&mut bytes);
                for i in 0..n {
                    packer.pack_value(input[i], bits);
                }

                let mut output = vec![0u64; n];
                let mut unpacker = BitUnpacker::new(&bytes);
                for item in &mut output {
                    *item = unpacker.unpack_value(bits);
                }

                for i in 0..n {
                    assert_eq!(input[i], output[i]);
                }
            }
        }
    }

    #[test]
    fn pack_unpack_blocks() {
        let mut value = 0xaa55_aa55_aa55_aa55u64; // arbitrary starting value
        for _n in 0..10000 {
            for bits in 1u8..=63 {
                let mask = (1u64 << bits) - 1;
                let mut input = vec![0u64; 8];
                for item in &mut input {
                    *item = value & mask;
                    value = value.wrapping_add(IGOLDEN64);
                }

                let mut bytes = vec![0u8; bits as usize];
                pack_bits_block8(&input, &mut bytes, bits);

                let mut output = vec![0u64; 8];
                unpack_bits_block8(&mut output, &bytes, bits);

                for i in 0..8 {
                    assert_eq!(input[i], output[i]);
                }
            }
        }
    }

    #[test]
    fn pack_bits_unpack_blocks() {
        let mut value = 0u64; // arbitrary starting value
        for _m in 0..10000 {
            for bits in 1u8..=63 {
                let mask = (1u64 << bits) - 1;
                let mut input = vec![0u64; 8];
                for item in &mut input {
                    *item = value & mask;
                    value = value.wrapping_add(IGOLDEN64);
                }

                let mut bytes = vec![0u8; bits as usize];
                let mut packer = BitPacker::new(&mut bytes);
                for i in 0..8 {
                    packer.pack_value(input[i], bits);
                }

                let mut output = vec![0u64; 8];
                unpack_bits_block8(&mut output, &bytes, bits);

                for i in 0..8 {
                    assert_eq!(input[i], output[i]);
                }
            }
        }
    }

    #[test]
    fn pack_blocks_unpack_bits() {
        let mut value = 111u64; // arbitrary starting value
        for _m in 0..10000 {
            for bits in 1u8..=63 {
                let mask = (1u64 << bits) - 1;
                let mut input = vec![0u64; 8];
                for item in &mut input {
                    *item = value & mask;
                    value = value.wrapping_add(IGOLDEN64);
                }

                let mut bytes = vec![0u8; bits as usize];
                pack_bits_block8(&input, &mut bytes, bits);

                let mut output = vec![0u64; 8];
                let mut unpacker = BitUnpacker::new(&bytes);
                for item in &mut output {
                    *item = unpacker.unpack_value(bits);
                }

                for i in 0..8 {
                    assert_eq!(input[i], output[i]);
                }
            }
        }
    }

    #[test]
    fn pack_unpack_bits_64() {
        let n = 8usize;
        let mut value = 0xaa55_aa55_aa55_aa55u64;
        let mut input = vec![0u64; n];
        for item in &mut input {
            *item = value;
            value = value.wrapping_add(IGOLDEN64);
        }

        let mut bytes = vec![0u8; n * std::mem::size_of::<u64>()];
        let mut packer = BitPacker::new(&mut bytes);
        for &v in &input {
            packer.pack_value(v, 64);
        }
        assert_eq!(packer.byte_index, 64);
        assert_eq!(packer.byte_bit_used, 0);

        let mut output = vec![0u64; n];
        let mut unpacker = BitUnpacker::new(&bytes);
        for item in &mut output {
            *item = unpacker.unpack_value(64);
        }
        assert_eq!(unpacker.byte_index, 64);
        assert_eq!(unpacker.byte_bit_used, 0);
        assert_eq!(input, output);
    }

    #[test]
    fn pack_unpack_bits_zero_width() {
        let mut bytes = vec![0xabu8; 8];
        let before = bytes.clone();

        // Create packer starting at byte 3, bit 5 (simulating initial state)
        let mut packer = BitPacker::new(&mut bytes);
        // Pack 0 bits - should not change anything
        packer.pack_value(0xdead_beef, 0);
        // Since packer starts at 0, we just verify the bytes are unchanged
        assert_eq!(packer.byte_index, 0);
        assert_eq!(packer.byte_bit_used, 0);
        assert_eq!(bytes, before);

        let mut unpacker = BitUnpacker::new(&bytes);
        let decoded = unpacker.unpack_value(0);
        assert_eq!(decoded, 0);
        assert_eq!(unpacker.byte_index, 0);
        assert_eq!(unpacker.byte_bit_used, 0);
    }

    #[test]
    fn pack_unpack_bits_cursor_alignment() {
        let mut value = 0x0123_4567_89ab_cdefu64;
        for bits in 1u8..=63 {
            let mask = (1u64 << bits) - 1;
            let mut input = [0u64; 8];
            for item in &mut input {
                *item = value & mask;
                value = value.wrapping_add(IGOLDEN64);
            }

            let mut bytes = vec![0u8; bits as usize];
            let mut packer = BitPacker::new(&mut bytes);
            for &v in &input {
                packer.pack_value(v, bits);
            }
            assert_eq!(packer.byte_index, bits as usize);
            assert_eq!(packer.byte_bit_used, 0);

            let mut output = [0u64; 8];
            let mut unpacker = BitUnpacker::new(&bytes);
            for item in &mut output {
                *item = unpacker.unpack_value(bits);
            }
            assert_eq!(unpacker.byte_index, bits as usize);
            assert_eq!(unpacker.byte_bit_used, 0);
            assert_eq!(input, output);
        }
    }

    #[test]
    #[should_panic(expected = "wrong number of bits in pack_bits_block8")]
    fn pack_bits_block8_rejects_zero_bits() {
        let input = [0u64; 8];
        let mut bytes = [0u8; 1];
        pack_bits_block8(&input, &mut bytes, 0);
    }

    #[test]
    #[should_panic(expected = "wrong number of bits in unpack_bits_block8")]
    fn unpack_bits_block8_rejects_64_bits() {
        let mut output = [0u64; 8];
        let bytes = [0u8; 64];
        unpack_bits_block8(&mut output, &bytes, 64);
    }

    #[test]
    #[should_panic]
    fn packer_panics_on_buffer_overflow() {
        // Buffer is too small to hold 8 values of 8 bits each (needs 8 bytes)
        let mut bytes = [0u8; 4];
        let mut packer = BitPacker::new(&mut bytes);
        // First 4 values fit (4 bytes)
        for i in 0..4 {
            packer.pack_value(i as u64, 8);
        }
        // This should panic - trying to write beyond buffer
        packer.pack_value(0xdead_beef, 8);
    }

    #[test]
    #[should_panic]
    fn unpacker_panics_on_buffer_underflow() {
        // Buffer only has 4 bytes, but we try to unpack 8 values of 8 bits each
        let bytes = [0xabu8; 4];
        let mut unpacker = BitUnpacker::new(&bytes);
        // First 4 values succeed (4 bytes)
        for _ in 0..4 {
            unpacker.unpack_value(8);
        }
        // This should panic - trying to read beyond buffer
        unpacker.unpack_value(8);
    }
}
