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

use std::ops::BitXor;

use crate::error::Error;
use crate::error::ErrorKind;
use crate::xor::XorFilterBuilder;

const LOAD_FACTOR: f64 = 1.23;
const EXTRA_SPACE: usize = 32;

pub(super) trait Fingerprint: Copy + Default + BitXor<Output = Self> + PartialEq {
    fn from_hash(hash: u64) -> Self;
}

impl Fingerprint for u8 {
    fn from_hash(hash: u64) -> Self {
        fingerprint(hash) as u8
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct XorFilter<Fp> {
    seed: u64,
    block_length: usize,
    fingerprints: Vec<Fp>,
}

impl<Fp: Fingerprint> XorFilter<Fp> {
    fn contains(&self, key: u64) -> bool {
        if self.fingerprints.is_empty() {
            return false;
        }

        let hash = mix(key, self.seed);
        let fp = Fp::from_hash(hash);
        let [h0, h1, h2] = hash_indices(hash, self.block_length);

        fp == self.fingerprints[h0]
            ^ self.fingerprints[h1 + self.block_length]
            ^ self.fingerprints[h2 + 2 * self.block_length]
    }

    fn len(&self) -> usize {
        self.fingerprints.len()
    }

    fn is_empty(&self) -> bool {
        self.fingerprints.is_empty()
    }

    fn seed(&self) -> u64 {
        self.seed
    }

    fn block_length(&self) -> usize {
        self.block_length
    }

    pub(super) fn build_from_keys(
        keys: &[u64],
        seed: u64,
        max_attempts: u32,
    ) -> Result<Self, Error> {
        if keys.is_empty() {
            return Ok(Self {
                seed,
                block_length: 0,
                fingerprints: Vec::new(),
            });
        }

        if max_attempts == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "max_attempts must be at least 1",
            ));
        }

        debug_assert_all_distinct(keys);

        let capacity = compute_capacity(keys.len())?;
        let block_length = capacity / 3;
        if block_length == 0 {
            return Ok(Self {
                seed,
                block_length: 0,
                fingerprints: Vec::new(),
            });
        }
        if block_length > u32::MAX as usize {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "block_length must not exceed u32::MAX",
            ));
        }

        let mut rng_state = seed;
        let mut attempt_seed = seed;
        for attempt in 0..max_attempts {
            if attempt > 0 {
                attempt_seed = splitmix64(&mut rng_state);
            }

            if let Some(fingerprints) =
                try_build_fingerprints::<Fp>(keys, attempt_seed, block_length, capacity)
            {
                return Ok(Self {
                    seed: attempt_seed,
                    block_length,
                    fingerprints,
                });
            }
        }

        Err(Error::new(
            ErrorKind::InvalidArgument,
            "failed to construct xor filter; keys may contain duplicates",
        )
        .with_context("attempts", max_attempts)
        .with_context("keys", keys.len()))
    }
}

/// Xor8 filter with 8-bit fingerprints.
///
/// Xor filters provide fast membership checks with no false negatives and a small
/// false positive rate. They are built from a set of distinct 64-bit keys and are
/// immutable once constructed.
///
/// # Examples
///
/// ```
/// use datasketches::xor::Xor8;
///
/// let keys: Vec<u64> = (0..10_000).collect();
/// let filter = Xor8::builder().build(&keys).unwrap();
///
/// assert!(filter.contains(42));
/// assert!(!filter.contains(1_000_000));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Xor8 {
    pub(super) core: XorFilter<u8>,
}

impl Xor8 {
    /// Creates a builder for Xor8 filters.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::Xor8;
    ///
    /// let keys: Vec<u64> = (0..1_000).collect();
    /// let filter = Xor8::builder().build(&keys).unwrap();
    /// assert!(filter.contains(42));
    /// ```
    pub fn builder() -> XorFilterBuilder {
        XorFilterBuilder::default()
    }

    /// Returns `true` if the filter probably contains the specified key.
    ///
    /// There are no false negatives, but false positives are possible.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::Xor8;
    ///
    /// let keys: Vec<u64> = (0..1_000).collect();
    /// let filter = Xor8::builder().build(&keys).unwrap();
    /// assert!(filter.contains(7));
    /// assert!(!filter.contains(10_000));
    /// ```
    pub fn contains(&self, key: u64) -> bool {
        self.core.contains(key)
    }

    /// Returns the number of fingerprints stored by the filter.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::Xor8;
    ///
    /// let keys: Vec<u64> = (0..1_000).collect();
    /// let filter = Xor8::builder().build(&keys).unwrap();
    /// assert!(filter.len() >= keys.len());
    /// ```
    pub fn len(&self) -> usize {
        self.core.len()
    }

    /// Returns true if the filter is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::Xor8;
    ///
    /// let filter = Xor8::builder().build(&[]).unwrap();
    /// assert!(filter.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.core.is_empty()
    }

    /// Returns the hash seed used by the filter.
    pub fn seed(&self) -> u64 {
        self.core.seed()
    }

    /// Returns the length of each block.
    pub fn block_length(&self) -> usize {
        self.core.block_length()
    }
}

#[derive(Default, Copy, Clone)]
struct KeyIndex {
    hash: u64,
    index: usize,
}

#[derive(Default, Copy, Clone)]
struct HSet {
    count: u32,
    mask: u64,
}

fn try_build_fingerprints<Fp: Fingerprint>(
    keys: &[u64],
    seed: u64,
    block_length: usize,
    capacity: usize,
) -> Option<Vec<Fp>> {
    let mut h: [Vec<HSet>; 3] = [
        vec![HSet::default(); block_length],
        vec![HSet::default(); block_length],
        vec![HSet::default(); block_length],
    ];
    let mut q: [Vec<KeyIndex>; 3] = [
        vec![KeyIndex::default(); block_length],
        vec![KeyIndex::default(); block_length],
        vec![KeyIndex::default(); block_length],
    ];
    let mut q_sizes = [0usize; 3];
    let mut stack: Vec<KeyIndex> = vec![KeyIndex::default(); keys.len()];
    let mut stack_size = 0;

    for &key in keys {
        let hash = mix(key, seed);
        let hset = hash_indices(hash, block_length);
        for b in 0..3 {
            let idx = hset[b];
            h[b][idx].mask ^= hash;
            h[b][idx].count += 1;
        }
    }

    for b in 0..3 {
        for (idx, set) in h[b].iter().enumerate() {
            if set.count == 1 {
                q[b][q_sizes[b]] = KeyIndex {
                    hash: set.mask,
                    index: idx,
                };
                q_sizes[b] += 1;
            }
        }
    }

    while q_sizes.iter().any(|&size| size > 0) {
        for block in 0..3 {
            while q_sizes[block] > 0 {
                q_sizes[block] -= 1;
                let ki = q[block][q_sizes[block]];
                if h[block][ki.index].count == 0 {
                    continue;
                }

                stack[stack_size] = KeyIndex {
                    hash: ki.hash,
                    index: ki.index + block * block_length,
                };
                stack_size += 1;

                let hset = hash_indices(ki.hash, block_length);
                for other in 0..3 {
                    if other == block {
                        continue;
                    }
                    let idx = hset[other];
                    h[other][idx].mask ^= ki.hash;
                    h[other][idx].count -= 1;
                    if h[other][idx].count == 1 {
                        q[other][q_sizes[other]] = KeyIndex {
                            hash: h[other][idx].mask,
                            index: idx,
                        };
                        q_sizes[other] += 1;
                    }
                }
            }
        }
    }

    if stack_size != keys.len() {
        return None;
    }

    let mut fingerprints = vec![Fp::default(); capacity];
    for ki in stack[..stack_size].iter().rev() {
        let hset = hash_indices(ki.hash, block_length);
        let fp = Fp::from_hash(ki.hash);
        let idx0 = hset[0];
        let idx1 = hset[1] + block_length;
        let idx2 = hset[2] + 2 * block_length;
        fingerprints[ki.index] = fp ^ fingerprints[idx0] ^ fingerprints[idx1] ^ fingerprints[idx2];
    }

    Some(fingerprints)
}

fn compute_capacity(num_keys: usize) -> Result<usize, Error> {
    let estimated = (num_keys as f64) * LOAD_FACTOR;
    if estimated > (usize::MAX as f64) {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            "key set too large to allocate xor filter",
        ));
    }

    let base = estimated as usize;
    let capacity = base
        .checked_add(EXTRA_SPACE)
        .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "xor filter size overflow"))?;
    Ok(capacity / 3 * 3)
}

fn hash_indices(hash: u64, block_length: usize) -> [usize; 3] {
    let mut out = [0usize; 3];
    for (i, slot) in out.iter_mut().enumerate() {
        let rotated = hash.rotate_left((i * 21) as u32) as u32;
        *slot = reduce(rotated, block_length);
    }
    out
}

#[inline]
fn reduce(hash: u32, n: usize) -> usize {
    ((hash as u64 * n as u64) >> 32) as usize
}

#[inline]
fn fingerprint(hash: u64) -> u64 {
    hash ^ (hash >> 32)
}

#[inline]
fn mix(key: u64, seed: u64) -> u64 {
    fmix64(key.wrapping_add(seed))
}

#[inline]
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^ (k >> 33)
}

#[inline]
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(debug_assertions)]
fn debug_assert_all_distinct(keys: &[u64]) {
    use std::collections::HashSet;

    let mut set = HashSet::with_capacity(keys.len());
    for &key in keys {
        assert!(set.insert(key), "xor filter requires distinct keys");
    }
}

#[cfg(not(debug_assertions))]
fn debug_assert_all_distinct(_keys: &[u64]) {}
