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
use std::hash::Hasher;
use std::ops::BitXor;

use crate::error::Error;
use crate::hash::XxHash64;

const NUM_HASHES: u8 = 3;
const HASH_SEED: u64 = 0;
const DEFAULT_CONSTRUCTION_SEED: u64 = 0;
const LOAD_FACTOR: f64 = 1.23;
const CAPACITY_OFFSET: u64 = 32;
const MAX_CONSTRUCTION_ATTEMPTS: usize = 100;

const MURMUR_C1: u64 = 0xff51_afd7_ed55_8ccd;
const MURMUR_C2: u64 = 0xc4ce_b9fe_1a85_ec53;

const SPLITMIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const SPLITMIX_MUL1: u64 = 0xbf58_476d_1ce4_e5b9;
const SPLITMIX_MUL2: u64 = 0x94d0_49bb_1331_11eb;

/// Fingerprint representation used by an [`XorFilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum XorFilterType {
    /// Uses 8-bit fingerprints and has an expected false-positive probability of about `1 / 256`.
    Xor8,
    /// Uses 16-bit fingerprints and has an expected false-positive probability of about
    /// `1 / 65_536`.
    Xor16,
}

impl XorFilterType {
    /// Returns the number of bits in each fingerprint.
    pub const fn bits_per_fingerprint(self) -> u8 {
        match self {
            Self::Xor8 => 8,
            Self::Xor16 => 16,
        }
    }

    pub(super) const fn bytes_per_fingerprint(self) -> usize {
        (self.bits_per_fingerprint() / 8) as usize
    }

    pub(super) fn from_bits(bits: u8) -> Result<Self, Error> {
        match bits {
            8 => Ok(Self::Xor8),
            16 => Ok(Self::Xor16),
            _ => Err(Error::deserial(format!(
                "invalid fingerprint width: expected 8 or 16, got {bits}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Fingerprints {
    Xor8(Box<[u8]>),
    Xor16(Box<[u16]>),
}

impl Fingerprints {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Xor8(values) => values.len(),
            Self::Xor16(values) => values.len(),
        }
    }

    pub(super) fn byte_len(&self) -> usize {
        match self {
            Self::Xor8(values) => size_of_val::<[u8]>(values),
            Self::Xor16(values) => size_of_val::<[u16]>(values),
        }
    }
}

/// An immutable xor filter for probabilistic set membership.
///
/// A query that returns `false` proves that the item was not in the input set. A query that returns
/// `true` may be a false positive, with probability determined by [`XorFilterType`]. Values cannot
/// be added after construction; rebuild the filter when the set changes.
///
/// Ordinary values passed to [`contains`](Self::contains) are reduced to 64-bit hashes with
/// xxHash64 and seed `0`. Use [`contains_hash`](Self::contains_hash) only with the same hashes that
/// were supplied to [`XorFilter::from_hashes`] or [`XorFilterBuilder::update_hash`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XorFilter {
    pub(super) filter_type: XorFilterType,
    pub(super) segment_length: usize,
    pub(super) num_items: usize,
    pub(super) seed: u64,
    pub(super) fingerprints: Fingerprints,
}

impl XorFilter {
    /// Builds a filter from precomputed 64-bit hashes.
    ///
    /// Duplicate hashes are removed before construction. The same hash values must be passed to
    /// [`contains_hash`](Self::contains_hash) when querying the result.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is too large for the portable serialization format or if no
    /// peelable construction is found within the bounded retry limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::XorFilter;
    /// use datasketches::xor::XorFilterType;
    ///
    /// let filter = XorFilter::from_hashes([10, 20, 30], XorFilterType::Xor8).unwrap();
    /// assert!(filter.contains_hash(20));
    /// ```
    pub fn from_hashes(
        hashes: impl IntoIterator<Item = u64>,
        filter_type: XorFilterType,
    ) -> Result<Self, Error> {
        let mut builder = XorFilterBuilder::new(filter_type);
        builder.extend_hashes(hashes);
        builder.build()
    }

    /// Returns `true` if an item is possibly in the input set.
    ///
    /// A `false` result means the item was definitely absent; a `true` result may be a false
    /// positive.
    #[inline]
    pub fn contains<T: Hash + ?Sized>(&self, item: &T) -> bool {
        self.contains_hash(hash_item(item))
    }

    /// Returns `true` if a precomputed 64-bit hash is possibly in the input set.
    ///
    /// This method bypasses xxHash64. The caller must use the same hash value during construction
    /// and queries; mixing raw values with precomputed hashes can introduce false negatives.
    #[inline]
    pub fn contains_hash(&self, hash: u64) -> bool {
        let mixed = mix(hash, self.seed);
        let [h0, h1, h2] = indexes(mixed, self.segment_length);

        match &self.fingerprints {
            Fingerprints::Xor8(fingerprints) => {
                fingerprint(mixed) as u8 == fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]
            }
            Fingerprints::Xor16(fingerprints) => {
                fingerprint(mixed) == fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]
            }
        }
    }

    /// Returns `true` if no distinct hashes were used to build the filter.
    pub fn is_empty(&self) -> bool {
        self.num_items == 0
    }

    /// Returns the number of distinct 64-bit hashes used to build the filter.
    pub fn num_items(&self) -> usize {
        self.num_items
    }

    /// Returns the fingerprint representation.
    pub fn filter_type(&self) -> XorFilterType {
        self.filter_type
    }

    /// Returns the number of bits in each fingerprint.
    pub fn bits_per_fingerprint(&self) -> u8 {
        self.filter_type.bits_per_fingerprint()
    }

    /// Returns the number of hash locations read by every query.
    pub fn num_hashes(&self) -> u8 {
        NUM_HASHES
    }

    /// Returns the number of fingerprint slots in the filter.
    pub fn capacity(&self) -> usize {
        self.fingerprints.len()
    }

    /// Returns the construction seed stored with the filter.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the number of fingerprint bits allocated per distinct input hash.
    ///
    /// Returns `0.0` for an empty filter. For sufficiently large inputs the value approaches `9.84`
    /// for [`XorFilterType::Xor8`] and `19.68` for [`XorFilterType::Xor16`].
    pub fn bits_per_item(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        self.capacity() as f64 * f64::from(self.bits_per_fingerprint()) / self.num_items as f64
    }

    /// Returns the estimated in-memory size of the filter in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.fingerprints.byte_len()
    }

    fn build(keys: &[u64], filter_type: XorFilterType, base_seed: u64) -> Result<Self, Error> {
        let num_items = keys.len();
        let capacity = compute_capacity(num_items)?;
        let payload_bytes = capacity
            .checked_mul(filter_type.bytes_per_fingerprint())
            .ok_or_else(|| Error::invalid_argument("xor filter fingerprint size overflow"))?;
        if payload_bytes > i32::MAX as usize {
            return Err(Error::invalid_argument(format!(
                "xor filter requires {payload_bytes} fingerprint bytes, exceeding the portable limit of {}",
                i32::MAX
            )));
        }

        let segment_length = capacity / usize::from(NUM_HASHES);
        let mut xor_mask = vec![0_u64; capacity];
        let mut count = vec![0_u32; capacity];
        let mut queue = vec![0_u32; capacity];
        let mut stack_hash = vec![0_u64; num_items];
        let mut stack_index = vec![0_u32; num_items];

        let mut rng_state = base_seed;
        let mut construction_seed = 0;
        let mut stack_size = 0;
        for _ in 0..MAX_CONSTRUCTION_ATTEMPTS {
            rng_state = rng_state.wrapping_add(SPLITMIX_GAMMA);
            construction_seed = splitmix64(rng_state);
            stack_size = map(
                keys,
                construction_seed,
                segment_length,
                &mut xor_mask,
                &mut count,
                &mut queue,
                &mut stack_hash,
                &mut stack_index,
            );
            if stack_size == num_items {
                break;
            }
        }

        if stack_size != num_items {
            return Err(Error::invalid_argument(format!(
                "xor filter construction failed after {MAX_CONSTRUCTION_ATTEMPTS} attempts"
            ))
            .with_context("num_items", num_items));
        }

        let fingerprints = match filter_type {
            XorFilterType::Xor8 => {
                let mut values = vec![0_u8; capacity].into_boxed_slice();
                assign_fingerprints(
                    &mut values,
                    segment_length,
                    &stack_hash[..stack_size],
                    &stack_index[..stack_size],
                    |hash| fingerprint(hash) as u8,
                );
                Fingerprints::Xor8(values)
            }
            XorFilterType::Xor16 => {
                let mut values = vec![0_u16; capacity].into_boxed_slice();
                assign_fingerprints(
                    &mut values,
                    segment_length,
                    &stack_hash[..stack_size],
                    &stack_index[..stack_size],
                    fingerprint,
                );
                Fingerprints::Xor16(values)
            }
        };

        Ok(Self {
            filter_type,
            segment_length,
            num_items,
            seed: construction_seed,
            fingerprints,
        })
    }
}

/// Builder for accumulating values and creating an immutable [`XorFilter`].
///
/// Values are reduced to 64-bit hashes as they are added, so the builder retains one `u64` per
/// update regardless of the original value size. Duplicate hashes are removed by
/// [`build`](Self::build).
#[derive(Debug, Clone)]
pub struct XorFilterBuilder {
    filter_type: XorFilterType,
    seed: u64,
    hashes: Vec<u64>,
}

impl XorFilterBuilder {
    /// Creates a builder for the requested fingerprint representation.
    ///
    /// The default base construction seed is `0`.
    pub fn new(filter_type: XorFilterType) -> Self {
        Self {
            filter_type,
            seed: DEFAULT_CONSTRUCTION_SEED,
            hashes: Vec::new(),
        }
    }

    /// Sets the base seed used to derive construction attempts.
    ///
    /// A fixed seed makes construction deterministic for a given set of hashes. The filter stores
    /// the successful derived seed, which is returned by [`XorFilter::seed`].
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Updates the builder with a value hashed by xxHash64 with seed `0`.
    pub fn update<T: Hash>(&mut self, item: T) {
        self.hashes.push(hash_item(&item));
    }

    /// Updates the builder with a precomputed 64-bit hash.
    ///
    /// This method bypasses xxHash64. Query the resulting filter with
    /// [`XorFilter::contains_hash`] using hashes from the same source.
    pub fn update_hash(&mut self, hash: u64) {
        self.hashes.push(hash);
    }

    /// Extends the builder with values hashed by xxHash64 with seed `0`.
    pub fn extend<T: Hash>(&mut self, items: impl IntoIterator<Item = T>) {
        self.hashes
            .extend(items.into_iter().map(|item| hash_item(&item)));
    }

    /// Extends the builder with precomputed 64-bit hashes.
    pub fn extend_hashes(&mut self, hashes: impl IntoIterator<Item = u64>) {
        self.hashes.extend(hashes);
    }

    /// Returns the number of updates accumulated so far, including duplicates.
    pub fn num_items(&self) -> usize {
        self.hashes.len()
    }

    /// Returns `true` if no updates have been accumulated.
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Builds an immutable filter after removing duplicate hashes.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is too large for the portable serialization format or if no
    /// peelable construction is found within the bounded retry limit.
    pub fn build(mut self) -> Result<XorFilter, Error> {
        self.hashes.sort_unstable();
        self.hashes.dedup();
        XorFilter::build(&self.hashes, self.filter_type, self.seed)
    }
}

fn hash_item<T: Hash + ?Sized>(item: &T) -> u64 {
    let mut hasher = XxHash64::with_seed(HASH_SEED);
    item.hash(&mut hasher);
    hasher.finish()
}

fn compute_capacity(num_items: usize) -> Result<usize, Error> {
    if num_items > i32::MAX as usize {
        return Err(Error::invalid_argument(format!(
            "xor filter item count exceeds portable limit: {num_items}"
        )));
    }

    let scaled = (LOAD_FACTOR * num_items as f64) as u64;
    let capacity = CAPACITY_OFFSET
        .checked_add(scaled)
        .ok_or_else(|| Error::invalid_argument("xor filter capacity overflow"))?;
    let capacity = capacity / u64::from(NUM_HASHES) * u64::from(NUM_HASHES);
    let capacity = capacity.max(u64::from(NUM_HASHES));
    if capacity > i32::MAX as u64 {
        return Err(Error::invalid_argument(format!(
            "xor filter capacity exceeds portable limit: {capacity}"
        )));
    }
    Ok(capacity as usize)
}

fn map(
    keys: &[u64],
    seed: u64,
    segment_length: usize,
    xor_mask: &mut [u64],
    count: &mut [u32],
    queue: &mut [u32],
    stack_hash: &mut [u64],
    stack_index: &mut [u32],
) -> usize {
    xor_mask.fill(0);
    count.fill(0);

    for &key in keys {
        let hash = mix(key, seed);
        for index in indexes(hash, segment_length) {
            xor_mask[index] ^= hash;
            count[index] += 1;
        }
    }

    let mut queue_length = 0;
    for (index, &value) in count.iter().enumerate() {
        if value == 1 {
            queue[queue_length] = index as u32;
            queue_length += 1;
        }
    }

    let mut stack_size = 0;
    while queue_length > 0 {
        queue_length -= 1;
        let index = queue[queue_length] as usize;
        if count[index] != 1 {
            continue;
        }

        let hash = xor_mask[index];
        stack_hash[stack_size] = hash;
        stack_index[stack_size] = index as u32;
        stack_size += 1;

        for hash_index in indexes(hash, segment_length) {
            count[hash_index] -= 1;
            xor_mask[hash_index] ^= hash;
            if count[hash_index] == 1 {
                queue[queue_length] = hash_index as u32;
                queue_length += 1;
            }
        }
    }

    stack_size
}

fn assign_fingerprints<T: Copy + BitXor<Output = T>>(
    fingerprints: &mut [T],
    segment_length: usize,
    stack_hash: &[u64],
    stack_index: &[u32],
    fingerprint_of: impl Fn(u64) -> T,
) {
    for (&hash, &index) in stack_hash.iter().zip(stack_index).rev() {
        let index = index as usize;
        let [h0, h1, h2] = indexes(hash, segment_length);
        fingerprints[index] =
            fingerprint_of(hash) ^ fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2];
    }
}

#[inline]
fn indexes(hash: u64, segment_length: usize) -> [usize; 3] {
    [
        reduce(hash as u32, segment_length),
        reduce(hash.rotate_left(21) as u32, segment_length) + segment_length,
        reduce(hash.rotate_left(42) as u32, segment_length) + 2 * segment_length,
    ]
}

#[inline]
fn reduce(hash: u32, range: usize) -> usize {
    ((u64::from(hash) * range as u64) >> 32) as usize
}

#[inline]
fn fingerprint(hash: u64) -> u16 {
    (hash ^ (hash >> 32)) as u16
}

#[inline]
fn mix(key: u64, seed: u64) -> u64 {
    let mut hash = key.wrapping_add(seed);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(MURMUR_C1);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(MURMUR_C2);
    hash ^ (hash >> 33)
}

fn splitmix64(state: u64) -> u64 {
    let mut value = state;
    value = (value ^ (value >> 30)).wrapping_mul(SPLITMIX_MUL1);
    value = (value ^ (value >> 27)).wrapping_mul(SPLITMIX_MUL2);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_matches_reference_values() {
        assert_eq!(compute_capacity(0).unwrap(), 30);
        assert_eq!(compute_capacity(1).unwrap(), 33);
        assert_eq!(compute_capacity(5).unwrap(), 36);
        assert_eq!(compute_capacity(10_000).unwrap(), 12_330);
    }

    #[test]
    fn reduce_stays_in_range() {
        for hash in [0, 1, u32::MAX / 2, u32::MAX] {
            assert!(reduce(hash, 17) < 17);
        }
    }
}
