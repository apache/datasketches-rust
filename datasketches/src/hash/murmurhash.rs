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

use std::hash::Hasher;

use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::read_u32_le;
use crate::hash::read_u64_le;

const C1: u64 = 0x87c37b91114253d5;
const C2: u64 = 0x4cf5ad432745937f;

/// The MurmurHash3 is a fast, non-cryptographic, 128-bit hash function that has
/// excellent avalanche and 2-way bit independence properties.
#[derive(Debug)]
pub struct MurmurHash3X64128 {
    h1: u64,
    h2: u64,
    total: u64,
    // Buffered tail bytes packed in little-endian bit order; bits at or above `buf_len * 8` are 0.
    buf: u128,
    buf_len: usize,
}

impl MurmurHash3X64128 {
    #[inline(always)]
    pub fn with_seed(seed: u64) -> Self {
        MurmurHash3X64128 {
            h1: seed,
            h2: seed,
            total: 0,
            buf: 0,
            buf_len: 0,
        }
    }

    #[inline(always)]
    pub fn finish128(&self) -> (u64, u64) {
        let mut h1 = self.h1;
        let mut h2 = self.h2;

        // tail
        if self.buf_len > 0 {
            if self.buf_len > 8 {
                let mut k2 = (self.buf >> 64) as u64;
                k2 = k2.wrapping_mul(C2);
                k2 = k2.rotate_left(33);
                k2 = k2.wrapping_mul(C1);
                h2 ^= k2;
            }

            let mut k1 = self.buf as u64;
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
        }

        h1 ^= self.total;
        h2 ^= self.total;
        h1 = h1.wrapping_add(h2);
        h2 = h2.wrapping_add(h1);
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 = h1.wrapping_add(h2);
        h2 = h2.wrapping_add(h1);
        (h1, h2)
    }

    #[inline(always)]
    fn update(&mut self, mut k1: u64, mut k2: u64) {
        // k1 *= c1; k1 = MURMUR3_ROTL64(k1, 31); k1 *= c2; out.h1 ^= k1;
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        self.h1 ^= k1;

        // out.h1 = MURMUR3_ROTL64(out.h1, 27); out.h1 += out.h2; out.h1 = out.h1*5+0x52dce729;
        self.h1 = self.h1.rotate_left(27);
        self.h1 = self.h1.wrapping_add(self.h2);
        self.h1 = self.h1.wrapping_mul(5).wrapping_add(0x52dce729);

        // k2 *= c2; k2 = MURMUR3_ROTL64(k2, 33); k2 *= c1; out.h2 ^= k2;
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        self.h2 ^= k2;

        // out.h2 = MURMUR3_ROTL64(out.h2,31); out.h2 += out.h1; out.h2 = out.h2*5+c4;
        self.h2 = self.h2.rotate_left(31);
        self.h2 = self.h2.wrapping_add(self.h1);
        self.h2 = self.h2.wrapping_mul(5).wrapping_add(0x38495ab5);
    }

    #[inline(always)]
    fn write_fixed<const N: usize>(&mut self, bytes: [u8; N]) {
        debug_assert!(N <= 16);
        if N == 16 && self.buf_len == 0 {
            self.total = self.total.wrapping_add(16);
            self.update(read_u64_le(&bytes[..8]), read_u64_le(&bytes[8..]));
        } else if self.buf_len + N < 16 {
            self.total = self.total.wrapping_add(N as u64);
            self.buf |= read_u128_le_partial(&bytes) << (self.buf_len * 8);
            self.buf_len += N;
        } else {
            self.write_buffered(&bytes);
        }
    }

    #[inline(never)]
    fn write_buffered(&mut self, bytes: &[u8]) {
        debug_assert!(self.buf_len > 0 && self.buf_len < 16);
        self.total = self.total.wrapping_add(bytes.len() as u64);

        let needed = 16 - self.buf_len;
        if bytes.len() < needed {
            self.buf |= read_u128_le_partial(bytes) << (self.buf_len * 8);
            self.buf_len += bytes.len();
            return;
        }

        let fill = read_u128_le_partial(&bytes[..needed]);
        let block = self.buf | (fill << (self.buf_len * 8));
        self.update(block as u64, (block >> 64) as u64);
        self.buf = 0;
        self.buf_len = 0;

        let mut chunks = bytes[needed..].chunks_exact(16);
        for chunk in &mut chunks {
            self.update(read_u64_le(&chunk[..8]), read_u64_le(&chunk[8..]));
        }

        let rem = chunks.remainder();
        if !rem.is_empty() {
            self.buf = read_u128_le_partial(rem);
            self.buf_len = rem.len();
        }
    }
}

impl Default for MurmurHash3X64128 {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED)
    }
}

impl Hasher for MurmurHash3X64128 {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.finish128().0
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        if self.buf_len != 0 {
            self.write_buffered(bytes);
            return;
        }

        self.total = self.total.wrapping_add(bytes.len() as u64);
        let mut chunks = bytes.chunks_exact(16);
        for chunk in &mut chunks {
            self.update(read_u64_le(&chunk[..8]), read_u64_le(&chunk[8..]));
        }

        let rem = chunks.remainder();
        if !rem.is_empty() {
            self.buf = read_u128_le_partial(rem);
            self.buf_len = rem.len();
        }
    }

    #[inline(always)]
    fn write_u8(&mut self, i: u8) {
        self.write_fixed(i.to_ne_bytes());
    }

    #[inline(always)]
    fn write_u16(&mut self, i: u16) {
        self.write_fixed(i.to_ne_bytes());
    }

    #[inline(always)]
    fn write_u32(&mut self, i: u32) {
        self.write_fixed(i.to_ne_bytes());
    }

    #[inline(always)]
    fn write_u64(&mut self, i: u64) {
        self.write_fixed(i.to_ne_bytes());
    }

    #[inline(always)]
    fn write_u128(&mut self, i: u128) {
        self.write_fixed(i.to_ne_bytes());
    }

    #[inline(always)]
    fn write_usize(&mut self, i: usize) {
        self.write_fixed(i.to_ne_bytes());
    }
}

#[inline(always)]
fn read_u128_le_partial(bytes: &[u8]) -> u128 {
    debug_assert!(bytes.len() < 16);
    if bytes.len() > 8 {
        let low = u128::from(read_u64_le(&bytes[..8]));
        let high = u128::from(read_u64_le_partial(&bytes[8..]));
        low | (high << 64)
    } else {
        u128::from(read_u64_le_partial(bytes))
    }
}

#[inline(always)]
fn read_u64_le_partial(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    debug_assert!(len <= 8);
    // Overlapping endpoint loads avoid both a byte loop and copying the tail onto the stack.
    if len == 8 {
        read_u64_le(bytes)
    } else if len >= 4 {
        let low = u64::from(read_u32_le(&bytes[..4]));
        let high = u64::from(read_u32_le(&bytes[len - 4..]));
        low | (high << ((len - 4) * 8))
    } else if len >= 2 {
        let low = u64::from(u16::from_le_bytes([bytes[0], bytes[1]]));
        let high = u64::from(u16::from_le_bytes([bytes[len - 2], bytes[len - 1]]));
        low | (high << ((len - 2) * 8))
    } else {
        bytes.first().map_or(0, |&byte| u64::from(byte))
    }
}

/// Finalization mix: force all bits of a hash block to avalanche.
#[inline(always)]
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^ (k >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn murmurhash3_x64_128(key: &[u8], seed: u64) -> (u64, u64) {
        let mut hasher = MurmurHash3X64128::with_seed(seed);
        hasher.write(key);
        hasher.finish128()
    }

    #[test]
    fn streaming_writes_match_single_write() {
        let input: Vec<u8> = (0..=96).collect();
        for len in 0..=input.len() {
            let expected = murmurhash3_x64_128(&input[..len], 42);
            for split in 0..=len {
                let mut hasher = MurmurHash3X64128::with_seed(42);
                hasher.write(&input[..split]);
                assert_eq!(hasher.finish128(), murmurhash3_x64_128(&input[..split], 42));
                hasher.write(&input[split..len]);
                assert_eq!(
                    hasher.finish128(),
                    expected,
                    "hash changed for input length {len} split at {split}"
                );
            }
        }

        // Exercise repeated block wraparounds and empty writes across chunk sizes 1..=17.
        for chunk_size in 1..=17 {
            let mut hasher = MurmurHash3X64128::with_seed(DEFAULT_UPDATE_SEED);
            let mut consumed = 0;
            for chunk in input.chunks(chunk_size) {
                hasher.write(&[]);
                hasher.write(chunk);
                consumed += chunk.len();
                assert_eq!(
                    hasher.finish128(),
                    murmurhash3_x64_128(&input[..consumed], DEFAULT_UPDATE_SEED),
                    "hash changed at prefix {consumed} with chunk size {chunk_size}"
                );
            }
        }
    }

    #[test]
    fn write_integers_match_write_of_native_bytes() {
        let input: Vec<u8> = (0..=32).collect();

        macro_rules! check_write {
            ($method:ident, $values:expr) => {
                for prefix in 0..=input.len() {
                    for value in $values {
                        let mut expected = MurmurHash3X64128::with_seed(DEFAULT_UPDATE_SEED);
                        expected.write(&input[..prefix]);
                        expected.write(&value.to_ne_bytes());
                        expected.write(&input[..prefix]);

                        let mut hasher = MurmurHash3X64128::with_seed(DEFAULT_UPDATE_SEED);
                        hasher.write(&input[..prefix]);
                        hasher.$method(value);
                        hasher.write(&input[..prefix]);

                        assert_eq!(
                            hasher.finish128(),
                            expected.finish128(),
                            "{} prefix {prefix}, value {value:#x}",
                            stringify!($method)
                        );
                    }
                }
            };
        }

        check_write!(write_u8, [0u8, 1, 0x7f, u8::MAX]);
        check_write!(write_u16, [0u16, 1, 0x1234, u16::MAX]);
        check_write!(write_u32, [0u32, 1, 0x0123_4567, u32::MAX]);
        check_write!(write_u64, [0u64, 1, 0x0123_4567_89ab_cdef, u64::MAX]);
        check_write!(
            write_u128,
            [
                0u128,
                1,
                0x0123_4567_89ab_cdef_fedc_ba98_7654_3210,
                u128::MAX
            ]
        );
        check_write!(write_usize, [0usize, 1, 0x1234_5678, usize::MAX]);
    }

    #[test]
    fn test_remainder() {
        // remainder > 8
        let key = "The quick brown fox jumps over the lazy dog";
        let (h1, h2) = murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0xe34bbc7bbc071b6c);
        assert_eq!(h2, 0x7a433ca9c49a9347);

        // change one bit
        let key = "The quick brown fox jumps over the lazy eog";
        let (h1, h2) = murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0x362108102c62d1c9);
        assert_eq!(h2, 0x3285cd100292b305);

        // test a remainder < 8
        let key = "The quick brown fox jumps over the lazy dogdogdog";
        let (h1, h2) = murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0x9c8205300e612fc4);
        assert_eq!(h2, 0xcbc0af6136aa3df9);

        // test a remainder = 8
        let key = "The quick brown fox jumps over the lazy1";
        let (h1, h2) = murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0xe3301a827e5cdfe3);
        assert_eq!(h2, 0xbdbf05f8da0f0392);

        // test a remainder = 0
        let key = "The quick brown fox jumps over t";
        let (h1, h2) = murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0xdf6af91bb29bdacf);
        assert_eq!(h2, 0x91a341c58df1f3a6);

        // test a ones byte and a zeros byte
        let key = [
            0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77,
            0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20, 0x6f, 0x76,
            0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79, 0x20, 0x64, 0x6f,
            0x67, 0xff, 0x64, 0x6f, 0x67, 0x00,
        ];
        let (h1, h2) = murmurhash3_x64_128(&key, 0);
        assert_eq!(h1, 0xe88abda785929c9e);
        assert_eq!(h2, 0x96b98587cacc83d6);
    }
}
