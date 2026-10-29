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
    /// Uses 32-bit fingerprints and has an expected false-positive probability of about `2^-32`.
    Xor32,
}

impl XorFilterType {
    /// Returns the number of bits in each fingerprint.
    pub const fn bits_per_fingerprint(self) -> u8 {
        match self {
            Self::Xor8 => 8,
            Self::Xor16 => 16,
            Self::Xor32 => 32,
        }
    }

    pub(crate) const fn bytes_per_fingerprint(self) -> usize {
        (self.bits_per_fingerprint() / 8) as usize
    }
}

/// Owned fingerprint slots for importing and exporting an application-defined format.
///
/// Slots are ordered as three consecutive, equally sized segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XorFingerprints {
    /// 8-bit fingerprint slots.
    Xor8(Box<[u8]>),
    /// 16-bit fingerprint slots.
    Xor16(Box<[u16]>),
    /// 32-bit fingerprint slots.
    Xor32(Box<[u32]>),
}

impl XorFingerprints {
    fn len(&self) -> usize {
        match self {
            Self::Xor8(values) => values.len(),
            Self::Xor16(values) => values.len(),
            Self::Xor32(values) => values.len(),
        }
    }

    pub(crate) fn byte_len(&self) -> usize {
        match self {
            Self::Xor8(values) => size_of_val::<[u8]>(values),
            Self::Xor16(values) => size_of_val::<[u16]>(values),
            Self::Xor32(values) => size_of_val::<[u32]>(values),
        }
    }
}

/// Raw state of an [`XorFilter`] for application-defined storage.
///
/// Created by [`XorFilter::into_parts`] or [`XorFilterParts::new`], and consumed by
/// [`XorFilter::from_parts`], which validates the layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XorFilterParts {
    seed: u64,
    segment_length: usize,
    fingerprints: XorFingerprints,
    num_items: Option<usize>,
}

impl XorFilterParts {
    /// Creates parts from a construction seed, segment length, and fingerprint slots.
    ///
    /// The item count is unknown unless set with [`with_num_items`](Self::with_num_items).
    pub fn new(seed: u64, segment_length: usize, fingerprints: XorFingerprints) -> Self {
        Self {
            seed,
            segment_length,
            fingerprints,
            num_items: None,
        }
    }

    /// Records the number of distinct hashes in the original input set.
    ///
    /// Standard serialization and emptiness checks require a known item count.
    pub fn with_num_items(mut self, num_items: usize) -> Self {
        self.num_items = Some(num_items);
        self
    }

    /// Returns the construction seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the number of fingerprint slots in each of the three segments.
    pub fn segment_length(&self) -> usize {
        self.segment_length
    }

    /// Returns the fingerprint slots.
    pub fn fingerprints(&self) -> &XorFingerprints {
        &self.fingerprints
    }

    /// Returns the number of distinct input hashes, or `None` if unknown.
    pub fn num_items(&self) -> Option<usize> {
        self.num_items
    }

    /// Returns the fingerprint slots without copying.
    pub fn into_fingerprints(self) -> XorFingerprints {
        self.fingerprints
    }
}

/// An immutable xor filter for probabilistic set membership.
///
/// A query that returns `false` proves that the item was not in the input set. A query that returns
/// `true` may be a false positive, with probability determined by [`XorFilterType`]. A filter
/// known to be empty always returns `false`. Values cannot
/// be added after construction; rebuild the filter when the set changes.
///
/// Ordinary values passed to [`contains`](Self::contains) are reduced to 64-bit hashes with
/// xxHash64 and seed `0`. Use [`contains_hash`](Self::contains_hash) only with the same hashes that
/// were supplied to [`XorFilterBuilder::build_from_hashes`] or
/// [`XorFilterBuilder::build_from_distinct_hashes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XorFilter {
    pub(crate) segment_length: usize,
    pub(crate) num_items: Option<usize>,
    pub(crate) seed: u64,
    pub(crate) fingerprints: XorFingerprints,
}

impl XorFilter {
    /// Imports raw parts without requiring a DataSketches serialization header.
    ///
    /// The caller must preserve the original construction seed and fingerprint order, and query
    /// with the original input hashes. A supplied item count must describe the original set.
    /// A valid layout alone cannot establish these invariants.
    ///
    /// # Errors
    ///
    /// Returns an error unless the segment length is positive, the payload has exactly three
    /// segments, and a supplied item count fits the capacity. Segment lengths above `u32::MAX`
    /// are rejected because the index reduction uses 32-bit hash words.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::XorFilter;
    /// use datasketches::xor::XorFilterBuilder;
    /// use datasketches::xor::XorFilterParts;
    /// use datasketches::xor::XorFilterType;
    ///
    /// let filter = XorFilterBuilder::new(XorFilterType::Xor8).build_from_hashes([10, 20, 30])?;
    /// let parts = filter.into_parts();
    /// // A storage format that does not record the item count keeps only these fields.
    /// let parts = XorFilterParts::new(
    ///     parts.seed(),
    ///     parts.segment_length(),
    ///     parts.into_fingerprints(),
    /// );
    /// let imported = XorFilter::from_parts(parts)?;
    /// assert!(imported.contains_hash(20));
    /// assert_eq!(imported.num_items(), None);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn from_parts(parts: XorFilterParts) -> Result<Self, Error> {
        let XorFilterParts {
            seed,
            segment_length,
            fingerprints,
            num_items,
        } = parts;
        if segment_length == 0
            || segment_length > u32::MAX as usize
            || segment_length.checked_mul(3) != Some(fingerprints.len())
        {
            return Err(Error::invalid_argument(
                "invalid xor filter segment or payload length",
            ));
        }
        if num_items.is_some_and(|count| count > fingerprints.len()) {
            return Err(Error::invalid_argument(
                "item count exceeds xor filter capacity",
            ));
        }
        Ok(Self {
            segment_length,
            num_items,
            seed,
            fingerprints,
        })
    }

    /// Returns the number of fingerprint slots in each of the three segments.
    pub fn segment_length(&self) -> usize {
        self.segment_length
    }

    /// Returns the fingerprint slots for application-defined serialization.
    pub fn fingerprints(&self) -> &XorFingerprints {
        &self.fingerprints
    }

    /// Converts the filter into raw parts without copying the fingerprint slots.
    pub fn into_parts(self) -> XorFilterParts {
        XorFilterParts {
            seed: self.seed,
            segment_length: self.segment_length,
            fingerprints: self.fingerprints,
            num_items: self.num_items,
        }
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
        if self.num_items == Some(0) {
            return false;
        }
        let mixed = mix(hash, self.seed);
        let [h0, h1, h2] = indexes(mixed, self.segment_length);

        match &self.fingerprints {
            XorFingerprints::Xor8(fingerprints) => {
                fingerprint(mixed) as u8 == fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]
            }
            XorFingerprints::Xor16(fingerprints) => {
                fingerprint(mixed) as u16 == fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]
            }
            XorFingerprints::Xor32(fingerprints) => {
                fingerprint(mixed) as u32 == fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]
            }
        }
    }

    /// Returns whether the input set was empty, or `None` if imported without an item count.
    ///
    /// Fingerprint contents cannot reliably establish whether the original set was empty.
    pub fn is_empty(&self) -> Option<bool> {
        self.num_items.map(|count| count == 0)
    }

    /// Returns the number of distinct input hashes, or `None` if imported without a count.
    pub fn num_items(&self) -> Option<usize> {
        self.num_items
    }

    /// Returns the fingerprint representation.
    pub fn filter_type(&self) -> XorFilterType {
        match &self.fingerprints {
            XorFingerprints::Xor8(_) => XorFilterType::Xor8,
            XorFingerprints::Xor16(_) => XorFilterType::Xor16,
            XorFingerprints::Xor32(_) => XorFilterType::Xor32,
        }
    }

    /// Returns the number of bits in each fingerprint.
    pub fn bits_per_fingerprint(&self) -> u8 {
        self.filter_type().bits_per_fingerprint()
    }

    /// Returns the number of hash locations used by non-empty filter queries.
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
    /// Returns `None` for an unknown item count and `Some(0.0)` for an empty filter. For
    /// sufficiently large inputs the value approaches `9.84` for [`XorFilterType::Xor8`],
    /// `19.68` for [`XorFilterType::Xor16`], and `39.36` for [`XorFilterType::Xor32`].
    pub fn bits_per_item(&self) -> Option<f64> {
        self.num_items.map(|count| {
            if count == 0 {
                0.0
            } else {
                self.capacity() as f64 * f64::from(self.bits_per_fingerprint()) / count as f64
            }
        })
    }

    /// Returns the estimated in-memory size of the filter in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.fingerprints.byte_len()
    }

    fn build(
        keys: &[u64],
        filter_type: XorFilterType,
        base_seed: u64,
        max_attempts: usize,
    ) -> Result<Self, Error> {
        debug_assert!(max_attempts > 0);
        let num_items = keys.len();
        let capacity = compute_capacity(num_items)?;
        let segment_length = capacity / usize::from(NUM_HASHES);
        let mut xor_mask = vec![0_u64; capacity];
        let mut count = vec![0_u32; capacity];
        let mut queue = vec![0_u32; capacity];
        let mut stack_hash = vec![0_u64; num_items];
        let mut stack_index = vec![0_u32; num_items];

        let mut rng_state = base_seed;
        let mut construction_seed = 0;
        let mut stack_size = 0;
        for attempt in 0..max_attempts {
            if attempt != 0 {
                xor_mask.fill(0);
                count.fill(0);
            }
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
            // For a fixed seed, mixing is a bijection, so two remaining hashes that cancel
            // out are the same input hash. Reseeding cannot separate duplicates.
            if count.iter().zip(&xor_mask).any(|(&c, &m)| c == 2 && m == 0) {
                return Err(
                    Error::invalid_argument("xor filter input contains duplicate hashes")
                        .with_context("num_items", num_items),
                );
            }
        }

        if stack_size != num_items {
            return Err(Error::invalid_argument(format!(
                "xor filter construction failed after {max_attempts} attempts"
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
                XorFingerprints::Xor8(values)
            }
            XorFilterType::Xor16 => {
                let mut values = vec![0_u16; capacity].into_boxed_slice();
                assign_fingerprints(
                    &mut values,
                    segment_length,
                    &stack_hash[..stack_size],
                    &stack_index[..stack_size],
                    |hash| fingerprint(hash) as u16,
                );
                XorFingerprints::Xor16(values)
            }
            XorFilterType::Xor32 => {
                let mut values = vec![0_u32; capacity].into_boxed_slice();
                assign_fingerprints(
                    &mut values,
                    segment_length,
                    &stack_hash[..stack_size],
                    &stack_index[..stack_size],
                    |hash| fingerprint(hash) as u32,
                );
                XorFingerprints::Xor32(values)
            }
        };

        Ok(Self {
            segment_length,
            num_items: Some(num_items),
            seed: construction_seed,
            fingerprints,
        })
    }
}

/// Builder for configuring and creating an immutable [`XorFilter`].
///
/// Stores only the fingerprint type and construction seed. Each build consumes the configuration
/// and takes the complete input set. Ordinary values and precomputed hashes are deduplicated in
/// temporary storage; already-distinct hashes can be borrowed without copying or sorting.
#[derive(Debug, Clone)]
pub struct XorFilterBuilder {
    filter_type: XorFilterType,
    seed: u64,
}

impl XorFilterBuilder {
    /// Creates a builder for the requested fingerprint representation.
    ///
    /// The default base construction seed is `0`.
    pub fn new(filter_type: XorFilterType) -> Self {
        Self {
            filter_type,
            seed: DEFAULT_CONSTRUCTION_SEED,
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

    /// Builds a filter from values hashed by xxHash64 with seed `0`.
    ///
    /// Duplicate hashes are removed before construction. The input is consumed as an iterator;
    /// pass borrowed values to retain them for a later build, including after an error.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`build_from_hashes`](Self::build_from_hashes).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::XorFilterBuilder;
    /// use datasketches::xor::XorFilterType;
    ///
    /// let filter = XorFilterBuilder::new(XorFilterType::Xor8)
    ///     .seed(42)
    ///     .build(["apple", "banana"])?;
    /// assert!(filter.contains("apple"));
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn build<T: Hash>(self, items: impl IntoIterator<Item = T>) -> Result<XorFilter, Error> {
        self.build_from_hashes(items.into_iter().map(|item| hash_item(&item)))
    }

    /// Builds a filter from precomputed 64-bit hashes, removing duplicates.
    ///
    /// This method bypasses xxHash64. Query the resulting filter with
    /// [`XorFilter::contains_hash`] using hashes from the same source. Temporary input storage is
    /// released on success or failure. Pass `hashes.iter().copied()` to retain the original input
    /// for a later build, including after an error.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if the fingerprint
    /// slot count exceeds `u32::MAX` or a scratch allocation would exceed `isize::MAX` bytes,
    /// or if no peelable construction is found within 100 attempts. Valid input can also
    /// exhaust the construction attempts. A successful build may still exceed the limits of
    /// [`XorFilter::serialize`].
    pub fn build_from_hashes(
        self,
        hashes: impl IntoIterator<Item = u64>,
    ) -> Result<XorFilter, Error> {
        let mut hashes: Vec<_> = hashes.into_iter().collect();
        hashes.sort_unstable();
        hashes.dedup();
        self.build_from_distinct_hashes(&hashes)
    }

    /// Builds a filter from distinct precomputed hashes without copying or sorting them.
    ///
    /// Borrows the input without modifying it, so the same slice can be retried with another
    /// construction seed after an error. Changing the seed does not guarantee success.
    /// Input order does not affect the result.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) under the size limits
    /// described by [`build_from_hashes`](Self::build_from_hashes), or if no peelable
    /// construction is found within 100 attempts, even for valid input. Duplicate hashes
    /// prevent peeling; a failed attempt usually exposes them and stops construction early,
    /// but detection is not guaranteed. Use [`build_from_hashes`](Self::build_from_hashes) to
    /// remove duplicates.
    pub fn build_from_distinct_hashes(self, hashes: &[u64]) -> Result<XorFilter, Error> {
        self.build_with_attempts(hashes, MAX_CONSTRUCTION_ATTEMPTS)
    }

    // A private attempt bound makes exhaustion and recovery deterministic in unit tests.
    fn build_with_attempts(self, hashes: &[u64], max_attempts: usize) -> Result<XorFilter, Error> {
        XorFilter::build(hashes, self.filter_type, self.seed, max_attempts)
    }
}

fn hash_item<T: Hash + ?Sized>(item: &T) -> u64 {
    let mut hasher = XxHash64::with_seed(HASH_SEED);
    item.hash(&mut hasher);
    hasher.finish()
}

fn compute_capacity(num_items: usize) -> Result<usize, Error> {
    if num_items > u32::MAX as usize {
        return Err(Error::invalid_argument(format!(
            "xor filter item count exceeds construction limit: {num_items}"
        )));
    }

    let scaled = (LOAD_FACTOR * num_items as f64) as u64;
    let capacity = CAPACITY_OFFSET + scaled;
    let capacity = capacity / u64::from(NUM_HASHES) * u64::from(NUM_HASHES);
    if capacity > u32::MAX as u64 || capacity > (isize::MAX as u64 / size_of::<u64>() as u64) {
        return Err(Error::invalid_argument(format!(
            "xor filter capacity exceeds construction limit: {capacity}"
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
    // Separate queues keep each drain within one segment and make the two neighbor
    // segments statically known. Scratch space remains one slot per fingerprint.
    let mut masks = split_segments(xor_mask, segment_length);
    let mut counts = split_segments(count, segment_length);
    let mut queues = split_segments(queue, segment_length);
    // Reuse the peel stack for mixed hashes while mapping one segment at a time.
    // This bounds the random-access working set without allocating another buffer.
    for (&key, hash) in keys.iter().zip(stack_hash.iter_mut()) {
        *hash = mix(key, seed);
    }
    for block in 0..3 {
        for &hash in stack_hash.iter() {
            let index = reduce(hash.rotate_left(block as u32 * 21) as u32, segment_length);
            masks[block][index] ^= hash;
            counts[block][index] += 1;
        }
    }
    let mut queue_sizes = [0; 3];
    for block in 0..3 {
        for (index, &value) in counts[block].iter().enumerate() {
            // Avoid a poorly predicted singleton branch. Before this write, the number
            // retained cannot exceed the current scan index, so the queue has room.
            queues[block][queue_sizes[block]] = index as u32;
            queue_sizes[block] += usize::from(value == 1);
        }
    }
    let mut stack_size = 0;
    while queue_sizes != [0; 3] {
        peel_block::<0>(
            &mut masks,
            &mut counts,
            &mut queues,
            &mut queue_sizes,
            segment_length,
            stack_hash,
            stack_index,
            &mut stack_size,
        );
        peel_block::<1>(
            &mut masks,
            &mut counts,
            &mut queues,
            &mut queue_sizes,
            segment_length,
            stack_hash,
            stack_index,
            &mut stack_size,
        );
        peel_block::<2>(
            &mut masks,
            &mut counts,
            &mut queues,
            &mut queue_sizes,
            segment_length,
            stack_hash,
            stack_index,
            &mut stack_size,
        );
    }
    stack_size
}

fn split_segments<T>(values: &mut [T], segment_length: usize) -> [&mut [T]; 3] {
    let (first, rest) = values.split_at_mut(segment_length);
    let (second, third) = rest.split_at_mut(segment_length);
    [first, second, third]
}

#[inline]
fn peel_block<const BLOCK: usize>(
    masks: &mut [&mut [u64]; 3],
    counts: &mut [&mut [u32]; 3],
    queues: &mut [&mut [u32]; 3],
    queue_sizes: &mut [usize; 3],
    segment_length: usize,
    stack_hash: &mut [u64],
    stack_index: &mut [u32],
    stack_size: &mut usize,
) {
    while queue_sizes[BLOCK] > 0 {
        queue_sizes[BLOCK] -= 1;
        let index = queues[BLOCK][queue_sizes[BLOCK]] as usize;
        if counts[BLOCK][index] == 0 {
            continue;
        }
        let hash = masks[BLOCK][index];
        stack_hash[*stack_size] = hash;
        stack_index[*stack_size] = (index + BLOCK * segment_length) as u32;
        *stack_size += 1;
        // The owned slot has no other live edge and is queued at most once. Only
        // the other two slots need updating; stale entries there are skipped above.
        for other in [(BLOCK + 1) % 3, (BLOCK + 2) % 3] {
            let index = reduce(hash.rotate_left(other as u32 * 21) as u32, segment_length);
            masks[other][index] ^= hash;
            counts[other][index] -= 1;
            if counts[other][index] == 1 {
                queues[other][queue_sizes[other]] = index as u32;
                queue_sizes[other] += 1;
            }
        }
    }
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
fn fingerprint(hash: u64) -> u64 {
    hash ^ (hash >> 32)
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
    use crate::error::ErrorKind;

    #[test]
    fn oversized_input_is_rejected_before_allocating() {
        for count in [usize::MAX, u32::MAX as usize] {
            assert_eq!(
                compute_capacity(count).unwrap_err().kind(),
                ErrorKind::InvalidArgument
            );
        }
    }

    #[test]
    fn borrowed_input_survives_exhaustion_and_can_retry() {
        // This synthetic set fails its first peeling attempt with base seed 1.
        // Limiting the private engine to one attempt exercises the public build failure path
        // without relying on a rare failure across all 100 production attempts.
        let keys: Vec<_> = (917_504..917_504 + 16_384u64)
            .map(|i| splitmix64(i.wrapping_add(SPLITMIX_GAMMA)))
            .collect();
        for kind in [
            XorFilterType::Xor8,
            XorFilterType::Xor16,
            XorFilterType::Xor32,
        ] {
            let original = keys.clone();
            let error = XorFilterBuilder::new(kind)
                .seed(1)
                .build_with_attempts(&keys, 1)
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert_eq!(keys, original);

            let builder = XorFilterBuilder::new(kind).seed(1_u64.wrapping_add(SPLITMIX_GAMMA));
            let filter = builder.clone().build_from_distinct_hashes(&keys).unwrap();
            assert_eq!(filter.num_items(), Some(keys.len()));
            for &key in &keys {
                assert!(filter.contains_hash(key));
            }
            // A failed peel overwrites the mixed hashes in the scratch stack. An automatic
            // retry must remap the original input before attempting the next seed.
            assert_eq!(
                XorFilterBuilder::new(kind)
                    .seed(1)
                    .build_from_distinct_hashes(&keys)
                    .unwrap(),
                filter,
            );
            assert_eq!(
                builder
                    .build_from_hashes(keys.iter().copied().chain(keys.iter().copied()))
                    .unwrap(),
                filter,
            );
        }
    }
}
