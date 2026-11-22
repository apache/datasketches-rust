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

use crate::hash::read_u32_le;
use crate::hash::read_u64_le;

const DEFAULT_SEED: u64 = 0;

// Unsigned 64-bit primes from xxhash64.
const P1: u64 = 0x9E3779B185EBCA87;
const P2: u64 = 0xC2B2AE3D27D4EB4F;
const P3: u64 = 0x165667B19E3779F9;
const P4: u64 = 0x85EBCA77C2B2AE63;
const P5: u64 = 0x27D4EB2F165667C5;

/// The XxHash64 is a fast, non-cryptographic, 64-bit hash function that has
/// excellent avalanche and 2-way bit independence properties.
#[derive(Debug)]
pub struct XxHash64 {
    seed: u64,
    total_len: u64,
    v1: u64,
    v2: u64,
    v3: u64,
    v4: u64,
    // Buffered tail bytes packed in little-endian bit order across two 128-bit halves;
    // bits at or above `buffer_len * 8` are always zero.
    lo: u128,
    hi: u128,
    buffer_len: usize,
}

impl XxHash64 {
    #[inline(always)]
    pub fn with_seed(seed: u64) -> Self {
        XxHash64 {
            seed,
            total_len: 0,
            v1: seed.wrapping_add(P1).wrapping_add(P2),
            v2: seed.wrapping_add(P2),
            v3: seed,
            v4: seed.wrapping_sub(P1),
            lo: 0,
            hi: 0,
            buffer_len: 0,
        }
    }

    #[inline(always)]
    pub fn finish64(&self) -> u64 {
        let mut hash = if self.total_len >= 32 {
            let mut acc = self
                .v1
                .rotate_left(1)
                .wrapping_add(self.v2.rotate_left(7))
                .wrapping_add(self.v3.rotate_left(12))
                .wrapping_add(self.v4.rotate_left(18));
            acc = merge_round(acc, self.v1);
            acc = merge_round(acc, self.v2);
            acc = merge_round(acc, self.v3);
            acc = merge_round(acc, self.v4);
            acc
        } else {
            self.seed.wrapping_add(P5)
        };

        hash = hash.wrapping_add(self.total_len);

        let rem = self.buffer_len;
        if rem > 0 {
            let b0 = self.lo as u64;
            let b1 = (self.lo >> 64) as u64;
            let b2 = self.hi as u64;
            let b3 = (self.hi >> 64) as u64;
            let mut tail = b0;
            if rem >= 8 {
                hash = mix_u64(hash, b0);
                tail = b1;
                if rem >= 16 {
                    hash = mix_u64(hash, b1);
                    tail = b2;
                    if rem >= 24 {
                        hash = mix_u64(hash, b2);
                        tail = b3;
                    }
                }
            }

            if (rem & 4) != 0 {
                hash = mix_u32(hash, tail as u32);
                tail >>= 32;
            }
            if (rem & 2) != 0 {
                hash = mix_u8(hash, tail as u8);
                hash = mix_u8(hash, (tail >> 8) as u8);
                tail >>= 16;
            }
            if (rem & 1) != 0 {
                hash = mix_u8(hash, tail as u8);
            }
        }

        finalize(hash)
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn hash_u64(input: u64, seed: u64) -> u64 {
        let hash = seed.wrapping_add(P5).wrapping_add(8);
        finalize(mix_u64(hash, input))
    }

    #[inline(always)]
    fn update(&mut self, chunk: &[u8]) {
        self.update_words(
            read_u64_le(&chunk[0..8]),
            read_u64_le(&chunk[8..16]),
            read_u64_le(&chunk[16..24]),
            read_u64_le(&chunk[24..32]),
        );
    }

    #[inline(always)]
    fn update_words(&mut self, w0: u64, w1: u64, w2: u64, w3: u64) {
        self.v1 = round(self.v1, w0);
        self.v2 = round(self.v2, w1);
        self.v3 = round(self.v3, w2);
        self.v4 = round(self.v4, w3);
    }

    #[inline(always)]
    fn load_tail(&mut self, rem: &[u8]) {
        debug_assert!((1..32).contains(&rem.len()));
        let (lo, hi) = read_u256_le_partial(rem);
        self.lo = lo;
        self.hi = hi;
        self.buffer_len = rem.len();
    }

    #[inline(always)]
    fn append_u256(&mut self, in_lo: u128, in_hi: u128) {
        if self.buffer_len < 16 {
            let shift = self.buffer_len * 8;
            self.lo |= in_lo << shift;
            self.hi = (in_lo >> (128 - shift)) | (in_hi << shift);
        } else if self.buffer_len == 16 {
            self.hi = in_lo;
        } else {
            let shift = (self.buffer_len - 16) * 8;
            self.hi |= in_lo << shift;
        }
    }

    /// Same result as `write(&bytes)`, keeping single-scalar and multi-field composite writes
    /// folded to register arithmetic at the call site.
    #[inline(always)]
    fn write_fixed<const N: usize>(&mut self, bytes: [u8; N]) {
        debug_assert!(N <= 16);
        self.total_len = self.total_len.wrapping_add(N as u64);
        let val = if N == 16 {
            u128::from(read_u64_le(&bytes[..8])) | (u128::from(read_u64_le(&bytes[8..])) << 64)
        } else {
            read_u128_le_partial(&bytes)
        };

        if self.buffer_len == 0 {
            self.lo = val;
            self.buffer_len = N;
        } else if self.buffer_len + N <= 16 {
            self.lo |= val << (self.buffer_len * 8);
            self.buffer_len += N;
        } else if self.buffer_len < 16 {
            // `1 <= buffer_len <= 15` and `buffer_len + N > 16`: straddles `lo` and `hi`.
            self.lo |= val << (self.buffer_len * 8);
            self.hi = val >> ((16 - self.buffer_len) * 8);
            self.buffer_len += N;
        } else if self.buffer_len + N < 32 {
            let hi_len = self.buffer_len - 16;
            self.hi |= val << (hi_len * 8);
            self.buffer_len += N;
        } else {
            // `16 <= buffer_len <= 31` and `buffer_len + N >= 32`: completes a 32-byte block.
            let hi_len = self.buffer_len - 16;
            let full_hi = if hi_len == 0 {
                val
            } else {
                self.hi | (val << (hi_len * 8))
            };
            self.update_words(
                self.lo as u64,
                (self.lo >> 64) as u64,
                full_hi as u64,
                (full_hi >> 64) as u64,
            );
            self.lo = if hi_len == 0 {
                0
            } else {
                val >> ((16 - hi_len) * 8)
            };
            self.hi = 0;
            self.buffer_len = self.buffer_len + N - 32;
        }
    }

    #[inline(always)]
    fn write_buffered(&mut self, bytes: &[u8]) {
        debug_assert!((1..32).contains(&self.buffer_len));
        if bytes.is_empty() {
            return;
        }
        self.total_len = self.total_len.wrapping_add(bytes.len() as u64);

        let needed = 32 - self.buffer_len;
        if bytes.len() < needed {
            let (in_lo, in_hi) = read_u256_le_partial(bytes);
            self.append_u256(in_lo, in_hi);
            self.buffer_len += bytes.len();
            return;
        }

        let (in_lo, in_hi) = read_u256_le_partial(&bytes[..needed]);
        self.append_u256(in_lo, in_hi);
        self.update_words(
            self.lo as u64,
            (self.lo >> 64) as u64,
            self.hi as u64,
            (self.hi >> 64) as u64,
        );
        self.lo = 0;
        self.hi = 0;
        self.buffer_len = 0;

        let mut chunks = bytes[needed..].chunks_exact(32);
        for chunk in &mut chunks {
            self.update(chunk);
        }

        let rem = chunks.remainder();
        if !rem.is_empty() {
            self.load_tail(rem);
        }
    }
}

impl Default for XxHash64 {
    #[inline(always)]
    fn default() -> Self {
        Self::with_seed(DEFAULT_SEED)
    }
}

impl Hasher for XxHash64 {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.finish64()
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        if self.buffer_len != 0 {
            self.write_buffered(bytes);
            return;
        }

        self.total_len = self.total_len.wrapping_add(bytes.len() as u64);
        let mut chunks = bytes.chunks_exact(32);
        for chunk in &mut chunks {
            self.update(chunk);
        }

        let rem = chunks.remainder();
        if !rem.is_empty() {
            self.load_tail(rem);
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
fn read_u256_le_partial(bytes: &[u8]) -> (u128, u128) {
    debug_assert!(bytes.len() < 32);
    if bytes.len() >= 16 {
        let lo =
            u128::from(read_u64_le(&bytes[..8])) | (u128::from(read_u64_le(&bytes[8..16])) << 64);
        let hi = read_u128_le_partial(&bytes[16..]);
        (lo, hi)
    } else {
        (read_u128_le_partial(bytes), 0)
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

#[inline(always)]
fn mix_u64(mut hash: u64, mut k1: u64) -> u64 {
    k1 = k1.wrapping_mul(P2);
    k1 = k1.rotate_left(31);
    k1 = k1.wrapping_mul(P1);
    hash ^= k1;
    hash.rotate_left(27).wrapping_mul(P1).wrapping_add(P4)
}

#[inline(always)]
fn mix_u32(mut hash: u64, k1: u32) -> u64 {
    hash ^= u64::from(k1).wrapping_mul(P1);
    hash.rotate_left(23).wrapping_mul(P2).wrapping_add(P3)
}

#[inline(always)]
fn mix_u8(mut hash: u64, k1: u8) -> u64 {
    hash ^= u64::from(k1).wrapping_mul(P5);
    hash.rotate_left(11).wrapping_mul(P1)
}

#[inline(always)]
fn round(mut acc: u64, input: u64) -> u64 {
    acc = acc.wrapping_add(input.wrapping_mul(P2));
    acc = acc.rotate_left(31);
    acc.wrapping_mul(P1)
}

#[inline(always)]
fn merge_round(mut acc: u64, val: u64) -> u64 {
    let mut v = val;
    v = v.wrapping_mul(P2);
    v = v.rotate_left(31);
    v = v.wrapping_mul(P1);
    acc ^= v;
    acc.wrapping_mul(P1).wrapping_add(P4)
}

#[inline(always)]
fn finalize(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(P2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(P3);
    hash ^ (hash >> 32)
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use super::*;

    const PRIME32: u64 = 0x9E3779B1;
    const PRIME64: u64 = 0x9E3779B185EBCA8D;

    fn fill_test_buffer(len: usize) -> Vec<u8> {
        let mut buffer = vec![0u8; len];
        let mut byte_gen = PRIME32;
        for byte in &mut buffer {
            *byte = (byte_gen >> 56) as u8;
            byte_gen = byte_gen.wrapping_mul(PRIME64);
        }
        buffer
    }

    fn xxhash64(data: &[u8], seed: u64) -> u64 {
        let mut hasher = XxHash64::with_seed(seed);
        hasher.write(data);
        hasher.finish64()
    }

    #[test]
    fn streaming_writes_match_single_write() {
        let input: Vec<u8> = (0..=128).collect();
        for len in 0..=input.len() {
            let expected = xxhash64(&input[..len], PRIME32);
            for split in 0..=len {
                let mut hasher = XxHash64::with_seed(PRIME32);
                hasher.write(&input[..split]);
                assert_eq!(hasher.finish64(), xxhash64(&input[..split], PRIME32));
                hasher.write(&input[split..len]);
                assert_eq!(
                    hasher.finish64(),
                    expected,
                    "hash changed for input length {len} split at {split}"
                );
            }
        }

        // Exercise repeated 32-byte block wraparounds and empty writes across chunk sizes 1..=33.
        for chunk_size in 1..=33 {
            let mut hasher = XxHash64::with_seed(PRIME32);
            let mut consumed = 0;
            for chunk in input.chunks(chunk_size) {
                hasher.write(&[]);
                hasher.write(chunk);
                consumed += chunk.len();
                assert_eq!(
                    hasher.finish64(),
                    xxhash64(&input[..consumed], PRIME32),
                    "hash changed at prefix {consumed} with chunk size {chunk_size}"
                );
            }
        }
    }

    #[test]
    fn test_vectors_seed_zero() {
        let buf = fill_test_buffer(101);
        assert_eq!(xxhash64(&buf[..0], 0), 0xEF46DB3751D8E999);
        assert_eq!(xxhash64(&buf[..1], 0), 0xE934A84ADB052768);
        assert_eq!(xxhash64(&buf[..32], 0), 0x18B216492BB44B70);
        assert_eq!(xxhash64(&buf[..33], 0), 0x55C8DC3E578F5B59);
        assert_eq!(xxhash64(&buf[..100], 0), 0x4BFE019CD91D9EA4);
    }

    #[test]
    fn test_vectors_seed_prime32() {
        let buf = fill_test_buffer(101);
        assert_eq!(xxhash64(&buf[..0], PRIME32), 0xAC75FDA2929B17EF);
        assert_eq!(xxhash64(&buf[..1], PRIME32), 0x5014607643A9B4C3);
        assert_eq!(xxhash64(&buf[..32], PRIME32), 0xB3F33BDF93ADE409);
        assert_eq!(xxhash64(&buf[..100], PRIME32), 0x4853706DC9625CAE);
    }

    #[test]
    fn test_long_check() {
        let seed = 0;
        let hash1 = XxHash64::hash_u64(123, seed);
        let mut hasher = XxHash64::with_seed(seed);
        hasher.write(&123u64.to_le_bytes());
        let hash2 = hasher.finish64();
        assert_eq!(hash2, hash1);
    }

    #[test]
    fn write_integers_match_write_of_native_bytes() {
        let input: Vec<u8> = (0..=40).collect();

        macro_rules! check_write {
            ($method:ident, $values:expr) => {
                for prefix in 0..=input.len() {
                    for value in $values {
                        let mut expected = XxHash64::with_seed(PRIME32);
                        expected.write(&input[..prefix]);
                        expected.write(&value.to_ne_bytes());
                        expected.write(&input[..prefix]);

                        let mut hasher = XxHash64::with_seed(PRIME32);
                        hasher.write(&input[..prefix]);
                        hasher.$method(value);
                        hasher.write(&input[..prefix]);

                        assert_eq!(
                            hasher.finish64(),
                            expected.finish64(),
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

        // Composite fixed-width tuples straddling 16-byte halves and completing 32-byte blocks.
        let a = 0x0123_4567_89ab_cdef_u64;
        let b = 0xfedc_ba98_7654_3210_u64;
        let c = 0xdead_beef_u32;
        let d = 0x0011_2233_4455_6677_8899_aabb_ccdd_eeff_u128;

        let mut tuple_bytes = Vec::new();
        tuple_bytes.extend_from_slice(&a.to_ne_bytes());
        tuple_bytes.extend_from_slice(&b.to_ne_bytes());
        let mut pair_hasher = XxHash64::with_seed(PRIME32);
        (a, b).hash(&mut pair_hasher);
        assert_eq!(pair_hasher.finish64(), xxhash64(&tuple_bytes, PRIME32));

        tuple_bytes.extend_from_slice(&c.to_ne_bytes());
        tuple_bytes.extend_from_slice(&d.to_ne_bytes());
        let mut mixed_hasher = XxHash64::with_seed(PRIME32);
        (a, b, c, d).hash(&mut mixed_hasher);
        assert_eq!(mixed_hasher.finish64(), xxhash64(&tuple_bytes, PRIME32));
    }
}
