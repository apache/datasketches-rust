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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in_range;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::hash::XxHash64;

const NUM_HASHES: u8 = 3;
const SERIAL_VERSION: u8 = 1;
const PREAMBLE_BYTES: usize = 2 * size_of::<u64>();
const HASH_SEED: u64 = 0;
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
    /// Uses 8-bit fingerprints and has an expected false-positive probability of about `2^-8`.
    Xor8,
    /// Uses 16-bit fingerprints and has an expected false-positive probability of about `2^-16`.
    Xor16,
    /// Uses 32-bit fingerprints and has an expected false-positive probability of about `2^-32`.
    Xor32,
}

impl XorFilterType {
    const fn bits_per_fingerprint(self) -> u8 {
        match self {
            Self::Xor8 => 8,
            Self::Xor16 => 16,
            Self::Xor32 => 32,
        }
    }

    const fn bytes_per_fingerprint(self) -> usize {
        match self {
            Self::Xor8 => 1,
            Self::Xor16 => 2,
            Self::Xor32 => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Fingerprints {
    Xor8(Box<[u8]>),
    Xor16(Box<[u16]>),
    Xor32(Box<[u32]>),
}

impl Fingerprints {
    fn len(&self) -> usize {
        match self {
            Self::Xor8(values) => values.len(),
            Self::Xor16(values) => values.len(),
            Self::Xor32(values) => values.len(),
        }
    }

    fn byte_len(&self) -> usize {
        match self {
            Self::Xor8(values) => size_of_val::<[u8]>(values),
            Self::Xor16(values) => size_of_val::<[u16]>(values),
            Self::Xor32(values) => size_of_val::<[u32]>(values),
        }
    }
}

/// An [`XorFilter`] builder for values or precomputed hashes.
///
/// Inputs may repeat. Temporary memory use can grow linearly with the total input count.
///
/// Query values with [`XorFilter::contains`] and precomputed hashes with
/// [`XorFilter::contains_hash`].
#[derive(Debug)]
pub struct XorFilterBuilder {
    filter_type: XorFilterType,
    seed: u64,
    hashes: Vec<u64>,
}

impl XorFilterBuilder {
    /// Creates a builder with the given fingerprint width and initial construction seed.
    ///
    /// The `seed` determines which layouts to try during construction. Values are hashed
    /// independently of it.
    pub fn new(filter_type: XorFilterType, seed: u64) -> Self {
        Self {
            filter_type,
            seed,
            hashes: Vec::new(),
        }
    }

    /// Adds a value.
    #[inline]
    pub fn update<T: Hash + ?Sized>(&mut self, item: &T) {
        self.update_hash(hash_item(item));
    }

    /// Adds a precomputed 64-bit hash.
    #[inline]
    pub fn update_hash(&mut self, hash: u64) {
        self.hashes.push(hash);
    }

    /// Adds a batch of values.
    pub fn update_values<T: Hash>(&mut self, items: impl IntoIterator<Item = T>) {
        self.update_hashes(items.into_iter().map(|item| hash_item(&item)));
    }

    /// Adds a batch of precomputed 64-bit hashes.
    pub fn update_hashes(&mut self, hashes: impl IntoIterator<Item = u64>) {
        self.hashes.extend(hashes);
    }

    /// Consumes the builder and creates a filter from the distinct hashes.
    ///
    /// # Errors
    ///
    /// Returns an error if the set of distinct hashes is too large or no valid layout is found.
    pub fn build(mut self) -> Result<XorFilter, Error> {
        self.hashes.sort_unstable();
        self.hashes.dedup();
        XorFilter::from_hashes(&self.hashes, self.filter_type, self.seed)
    }
}

/// An immutable xor filter for probabilistic set membership.
///
/// A query that returns `false` proves that the item was not in the input set. A query that returns
/// `true` may be a false positive, with probability determined by [`XorFilterType`], even for an
/// empty input set. Values cannot be added after construction; rebuild the filter when the set
/// changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XorFilter {
    // Cache the derived length to avoid division on every query.
    segment_length: usize,
    seed: u64,
    fingerprints: Fingerprints,
}

impl XorFilter {
    /// Creates a filter from distinct precomputed 64-bit hashes.
    ///
    /// The initial `seed` determines which layouts to try during construction. The same set of
    /// hashes, filter type, and seed produce the same filter regardless of input order.
    /// Query with [`contains_hash`](Self::contains_hash) using the same hash function.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) in these cases:
    ///
    /// - The input contains duplicate hashes.
    /// - The input is too large to build a filter.
    /// - Construction cannot find a valid layout. For distinct hashes, try another `seed`.
    pub fn from_hashes(keys: &[u64], filter_type: XorFilterType, seed: u64) -> Result<Self, Error> {
        let num_items = keys.len();
        let capacity = compute_capacity(num_items)?;
        let segment_length = capacity / usize::from(NUM_HASHES);
        let mut xor_mask = vec![0_u64; capacity];
        let mut count = vec![0_u32; capacity];
        // One spare slot per segment allows a speculative enqueue even when full.
        let mut queue = vec![0_u32; capacity + 3];
        let mut stack_hash = vec![0_u64; num_items];
        let mut stack_index = vec![0_u32; num_items];

        let mut rng_state = seed;
        let mut construction_seed = 0;
        let mut stack_size = 0;
        for attempt in 0..MAX_CONSTRUCTION_ATTEMPTS {
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
            return Err(Error::invalid_argument(
                "xor filter construction could not find a valid layout",
            )
            .with_context("num_items", num_items));
        }

        // Assignment only needs the peel stack. Release the graph before allocating the
        // fingerprint payload so these allocations do not overlap at peak memory usage.
        drop(xor_mask);
        drop(count);
        drop(queue);

        let fingerprints = match filter_type {
            XorFilterType::Xor8 => Fingerprints::Xor8(assign_fingerprints(
                capacity,
                segment_length,
                &stack_hash,
                &stack_index,
                |hash| fingerprint(hash) as u8,
            )),
            XorFilterType::Xor16 => Fingerprints::Xor16(assign_fingerprints(
                capacity,
                segment_length,
                &stack_hash,
                &stack_index,
                |hash| fingerprint(hash) as u16,
            )),
            XorFilterType::Xor32 => Fingerprints::Xor32(assign_fingerprints(
                capacity,
                segment_length,
                &stack_hash,
                &stack_index,
                |hash| fingerprint(hash) as u32,
            )),
        };

        Ok(Self {
            segment_length,
            seed: construction_seed,
            fingerprints,
        })
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
    /// Use the same hash function for construction and queries.
    // Keep the width dispatch in the caller's hot loop, including across crates.
    #[inline(always)]
    pub fn contains_hash(&self, hash: u64) -> bool {
        let mixed = mix(hash, self.seed);
        let [h0, h1, h2] = indexes(mixed, self.segment_length);

        match &self.fingerprints {
            Fingerprints::Xor8(fingerprints) => {
                fingerprint(mixed) as u8 == fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]
            }
            Fingerprints::Xor16(fingerprints) => {
                fingerprint(mixed) as u16 == fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]
            }
            Fingerprints::Xor32(fingerprints) => {
                fingerprint(mixed) as u32 == fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]
            }
        }
    }

    fn filter_type(&self) -> XorFilterType {
        match &self.fingerprints {
            Fingerprints::Xor8(_) => XorFilterType::Xor8,
            Fingerprints::Xor16(_) => XorFilterType::Xor16,
            Fingerprints::Xor32(_) => XorFilterType::Xor32,
        }
    }

    /// Returns the estimated in-memory size of the filter in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.fingerprints.byte_len()
    }

    /// Serializes the filter using DataSketches family `22`, version `1`.
    ///
    /// Supports 8-, 16-, and 32-bit fingerprints, recorded in the header's fingerprint width
    /// field. Integers and fingerprints are little-endian. Readers must support the encoded
    /// fingerprint width; implementations limited to 8/16 bits cannot read 32-bit images.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if the fingerprint
    /// slot count exceeds `i32::MAX` or the serialized size overflows `usize`. Construction allows
    /// larger capacities than the standard format.
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        if self.fingerprints.len() > i32::MAX as usize {
            return Err(Error::invalid_argument(
                "xor filter exceeds standard format limits",
            ));
        }
        let size = PREAMBLE_BYTES
            .checked_add(self.fingerprints.byte_len())
            .ok_or_else(|| Error::invalid_argument("xor filter serialized size overflow"))?;
        let mut bytes = SketchBytes::with_capacity(size);

        bytes.write_u8(Family::XORFILTER.min_pre_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::XORFILTER.id);
        bytes.write_u8(self.filter_type().bits_per_fingerprint());
        bytes.write_i32_le(self.segment_length as i32);
        bytes.write_u64_le(self.seed);

        match &self.fingerprints {
            Fingerprints::Xor8(fingerprints) => bytes.write(fingerprints),
            Fingerprints::Xor16(fingerprints) => {
                for &fingerprint in fingerprints.iter() {
                    bytes.write_u16_le(fingerprint);
                }
            }
            Fingerprints::Xor32(fingerprints) => {
                for &fingerprint in fingerprints.iter() {
                    bytes.write_u32_le(fingerprint);
                }
            }
        }

        Ok(bytes.into_bytes())
    }

    /// Deserializes an owned filter from bytes.
    ///
    /// Accepts 8-, 16-, and 32-bit images within the limits of [`serialize`](Self::serialize).
    /// Trailing bytes are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if the image is truncated, belongs to another sketch family or version, or
    /// contains metadata that could make a membership query index outside the fingerprint payload.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        let mut cursor = SketchSlice::new(bytes);

        let preamble_longs = cursor
            .read_u8()
            .map_err(insufficient_data("preamble_longs"))?;
        let serial_version = cursor
            .read_u8()
            .map_err(insufficient_data("serial_version"))?;
        let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
        let bits_per_fingerprint = cursor
            .read_u8()
            .map_err(insufficient_data("bits_per_fingerprint"))?;

        Family::XORFILTER.validate_id(family_id)?;
        ensure_serial_version_is(SERIAL_VERSION, serial_version)?;
        ensure_preamble_longs_in_range(
            Family::XORFILTER.min_pre_longs..=Family::XORFILTER.max_pre_longs,
            preamble_longs,
        )?;
        let filter_type = match bits_per_fingerprint {
            8 => XorFilterType::Xor8,
            16 => XorFilterType::Xor16,
            32 => XorFilterType::Xor32,
            _ => {
                return Err(Error::deserial(
                    "standard xor format requires 8-, 16-, or 32-bit fingerprints",
                ));
            }
        };
        let segment_length = cursor
            .read_i32_le()
            .map_err(insufficient_data("segment_length"))?;
        if segment_length <= 0 {
            return Err(Error::deserial(format!(
                "invalid segment length: expected a positive value, got {segment_length}"
            )));
        }
        let segment_length = segment_length as usize;

        let seed = cursor.read_u64_le().map_err(insufficient_data("seed"))?;

        let capacity = segment_length
            .checked_mul(usize::from(NUM_HASHES))
            .ok_or_else(|| Error::deserial("xor filter capacity overflow"))?;
        if capacity > i32::MAX as usize {
            return Err(Error::deserial(format!(
                "invalid xor filter capacity: maximum is {}, got {capacity}",
                i32::MAX
            )));
        }

        let fingerprint_bytes = capacity
            .checked_mul(filter_type.bytes_per_fingerprint())
            .ok_or_else(|| Error::deserial("xor filter fingerprint size overflow"))?;
        if cursor.remaining().len() < fingerprint_bytes {
            return Err(Error::insufficient_data_of(
                "fingerprints",
                format!(
                    "expected {fingerprint_bytes} bytes, got {}",
                    cursor.remaining().len()
                ),
            ));
        }

        let fingerprints = match filter_type {
            XorFilterType::Xor8 => Fingerprints::Xor8(cursor.remaining()[..capacity].into()),
            XorFilterType::Xor16 => {
                let mut values = Vec::with_capacity(capacity);
                for _ in 0..capacity {
                    values.push(
                        cursor
                            .read_u16_le()
                            .map_err(insufficient_data("fingerprints"))?,
                    );
                }
                Fingerprints::Xor16(values.into_boxed_slice())
            }
            XorFilterType::Xor32 => {
                let mut values = Vec::with_capacity(capacity);
                for _ in 0..capacity {
                    values.push(
                        cursor
                            .read_u32_le()
                            .map_err(insufficient_data("fingerprints"))?,
                    );
                }
                Fingerprints::Xor32(values.into_boxed_slice())
            }
        };

        Ok(Self {
            segment_length,
            seed,
            fingerprints,
        })
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
    // segments statically known.
    let mut masks = split_segments(xor_mask, segment_length);
    let mut counts = split_segments(count, segment_length);
    let mut queues = split_segments(queue, segment_length + 1);
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
            // A slot enters its queue at most once. The extra slot accommodates
            // this write when every real slot is already queued; retain it only
            // when the neighbor becomes a singleton.
            queues[other][queue_sizes[other]] = index as u32;
            queue_sizes[other] += usize::from(counts[other][index] == 1);
        }
    }
}

fn assign_fingerprints<T: Copy + Default + BitXor<Output = T>>(
    capacity: usize,
    segment_length: usize,
    stack_hash: &[u64],
    stack_index: &[u32],
    fingerprint_of: impl Fn(u64) -> T,
) -> Box<[T]> {
    let mut fingerprints = vec![T::default(); capacity].into_boxed_slice();
    for (&hash, &index) in stack_hash.iter().zip(stack_index).rev() {
        let index = index as usize;
        let [h0, h1, h2] = indexes(hash, segment_length);
        fingerprints[index] =
            fingerprint_of(hash) ^ fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2];
    }
    fingerprints
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
    // Construction and import validate the 32-bit segment bound. Keep both inputs
    // narrow so mapping, peeling, and queries can use widening 32-bit multiplies.
    ((u64::from(hash) * u64::from(range as u32)) >> 32) as usize
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
    fn peeling_handles_full_neighbor_queues() {
        // All three singleton queues are full. Removing the only edge makes
        // both neighbors stale, so speculative enqueues must not retain them.
        let mut masks = [0; 3];
        let mut counts = [0; 3];
        let mut queues = [0; 6];
        let mut hashes = [0; 1];
        let mut indexes = [0; 1];
        assert_eq!(
            map(
                &[42],
                1,
                1,
                &mut masks,
                &mut counts,
                &mut queues,
                &mut hashes,
                &mut indexes
            ),
            1,
        );
        assert_eq!(hashes, [mix(42, 1)]);
        assert_eq!(indexes, [0]);
    }
}
