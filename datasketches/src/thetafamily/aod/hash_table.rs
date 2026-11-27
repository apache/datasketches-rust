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
use std::iter::FusedIterator;

use crate::aod::ArrayOfDoublesEntry;
use crate::aod::policy::validate_num_values;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::MurmurHash3X64128;
use crate::hash::compute_seed_hash;
use crate::thetafamily::common::constants::MAX_LG_K;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::constants::MIN_LG_K;
use crate::thetafamily::common::constants::STRIDE_MASK;
use crate::thetafamily::common::hash_table::starting_retention_theta;
use crate::thetafamily::common::hash_table::starting_sub_multiple;

/// Flat open-addressed hash table storing `u64` keys alongside a strided `Vec<f64>` summary buffer.
///
/// Slot `i` holds key `keys[i]` (`0` when unoccupied) and values
/// `values[i * stride..(i + 1) * stride]`, where `stride = usize::from(num_values)`.
#[derive(Debug, Clone)]
pub(super) struct AodHashTable {
    lg_cur_size: u8,
    lg_nom_size: u8,
    lg_max_size: u8,
    num_values: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    seed_hash: u16,
    retention_theta: u64,
    num_retained: usize,
    keys: Vec<u64>,
    values: Vec<f64>,
}

impl AodHashTable {
    /// Creates a new mutable hash table with validated Theta-family and `num_values` parameters.
    pub fn new(
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
        num_values: u8,
    ) -> Result<Self, Error> {
        validate_num_values(num_values)?;
        if !(MIN_LG_K..=MAX_LG_K).contains(&lg_nom_size) {
            return Err(Error::invalid_argument(format!(
                "lg_k must be in [{MIN_LG_K}, {MAX_LG_K}], got {lg_nom_size}"
            )));
        }
        if !(sampling_probability > 0.0 && sampling_probability <= 1.0) {
            return Err(Error::invalid_argument(format!(
                "sampling_probability must be in (0.0, 1.0], got {sampling_probability}"
            )));
        }
        let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
        let lg_max_size = lg_nom_size + 1;
        let max_slots = 1usize
            .checked_shl(u32::from(lg_max_size))
            .ok_or_else(|| Error::invalid_argument("table capacity overflows platform usize"))?;
        max_slots
            .checked_mul(usize::from(num_values))
            .and_then(|len| len.checked_mul(size_of::<f64>()))
            .filter(|&bytes| bytes <= isize::MAX as usize)
            .ok_or_else(|| {
                Error::invalid_argument(format!(
                    "lg_k={lg_nom_size} with num_values={num_values} exceeds addressable allocation size"
                ))
            })?;
        let lg_cur_size = starting_sub_multiple(lg_max_size, MIN_LG_K, resize_factor.lg_value());
        Ok(Self::allocate_empty(
            lg_cur_size,
            lg_nom_size,
            resize_factor,
            sampling_probability,
            starting_retention_theta(sampling_probability),
            seed,
            seed_hash,
            num_values,
        ))
    }

    /// Creates a table sized for an intersection operator (`lg_cur_size == 0` allocates nothing).
    pub fn for_intersection(
        lg_cur_size: u8,
        lg_nom_size: u8,
        retention_theta: u64,
        seed: u64,
        seed_hash: u16,
        num_values: u8,
    ) -> Self {
        Self::allocate_empty(
            lg_cur_size,
            lg_nom_size,
            ResizeFactor::X1,
            1.0,
            retention_theta,
            seed,
            seed_hash,
            num_values,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn allocate_empty(
        lg_cur_size: u8,
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        retention_theta: u64,
        seed: u64,
        seed_hash: u16,
        num_values: u8,
    ) -> Self {
        let lg_max_size = lg_nom_size + 1;
        assert!(
            lg_cur_size <= lg_max_size,
            "lg_cur_size must be <= lg_nom_size + 1, got lg_cur_size={lg_cur_size}, lg_nom_size={lg_nom_size}"
        );
        let size = if lg_cur_size > 0 {
            1usize << lg_cur_size
        } else {
            0
        };
        let stride = usize::from(num_values);
        Self {
            lg_cur_size,
            lg_nom_size,
            lg_max_size,
            num_values,
            resize_factor,
            sampling_probability,
            seed,
            seed_hash,
            retention_theta,
            num_retained: 0,
            keys: vec![0; size],
            values: vec![0.0; size * stride],
        }
    }

    #[inline]
    pub fn stride(&self) -> usize {
        usize::from(self.num_values)
    }

    #[inline]
    pub fn num_values(&self) -> u8 {
        self.num_values
    }

    #[inline]
    pub fn lg_nom_size(&self) -> u8 {
        self.lg_nom_size
    }

    #[inline]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    #[inline]
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    #[inline]
    pub fn retention_theta(&self) -> u64 {
        self.retention_theta
    }

    pub fn set_retention_theta(&mut self, retention_theta: u64) {
        assert!(
            (1..=MAX_THETA).contains(&retention_theta),
            "theta must be in [1, {MAX_THETA}], got {retention_theta}"
        );
        self.retention_theta = retention_theta;
    }

    #[inline]
    pub fn num_retained(&self) -> usize {
        self.num_retained
    }

    #[inline]
    pub fn keys(&self) -> &[u64] {
        &self.keys
    }

    #[inline]
    pub fn slot_values(&self, slot: usize) -> &[f64] {
        let stride = self.stride();
        &self.values[slot * stride..(slot + 1) * stride]
    }

    #[inline]
    pub fn hash<T: Hash>(&self, value: T) -> u64 {
        let mut hasher = MurmurHash3X64128::with_seed(self.seed);
        value.hash(&mut hasher);
        let (h1, _) = hasher.finish128();
        h1 >> 1
    }

    #[inline]
    pub fn capacity_threshold(&self) -> usize {
        if self.lg_cur_size <= self.lg_nom_size {
            self.keys.len() / 2
        } else {
            self.keys.len() - self.keys.len() / 16
        }
    }

    /// Inserts `incoming` for `hash` if new, or adds `incoming` element-by-element if already
    /// retained. Hashes equal to `0` or `>= retention_theta` are ignored.
    pub fn insert_or_add(&mut self, hash: u64, incoming: &[f64]) {
        if hash == 0 || hash >= self.retention_theta {
            return;
        }
        let idx = self.find_slot(hash).expect("mutable table has a free slot");
        let stride = self.stride();
        let slot = &mut self.values[idx * stride..(idx + 1) * stride];
        if self.keys[idx] == hash {
            for (value, delta) in slot.iter_mut().zip(incoming) {
                *value += delta;
            }
            return;
        }

        slot.copy_from_slice(incoming);
        self.keys[idx] = hash;
        self.num_retained += 1;
        if self.num_retained > self.capacity_threshold() {
            if self.lg_cur_size <= self.lg_nom_size {
                self.resize();
            } else {
                self.rebuild();
            }
        }
    }

    /// Inserts a known-unique entry without combining or resizing, returning a mutable slice to
    /// the inserted slot's values, or `None` if `hash == 0`, `hash >= retention_theta`, the table
    /// has no free slot, or `hash` already exists in the table.
    pub fn insert_unique(&mut self, hash: u64, incoming: &[f64]) -> Option<&mut [f64]> {
        if hash == 0 || hash >= self.retention_theta {
            return None;
        }
        let idx = self.find_slot(hash)?;
        if self.keys[idx] == hash {
            return None;
        }
        let stride = self.stride();
        let slot = &mut self.values[idx * stride..(idx + 1) * stride];
        slot.copy_from_slice(incoming);
        self.keys[idx] = hash;
        self.num_retained += 1;
        Some(slot)
    }

    /// Looks up the retained value slice for `hash`, returning `None` if absent.
    #[inline]
    pub fn find_values(&self, hash: u64) -> Option<&[f64]> {
        if hash == 0 {
            return None;
        }
        let idx = self.find_slot(hash)?;
        (self.keys[idx] == hash).then(|| self.slot_values(idx))
    }

    #[inline]
    fn find_slot(&self, hash: u64) -> Option<usize> {
        let mask = self.keys.len().checked_sub(1)?;
        let step = (2 * ((hash >> self.lg_cur_size) & STRIDE_MASK) + 1) as usize;
        let start = hash as usize & mask;
        let mut idx = start;
        loop {
            if self.keys[idx] == hash || self.keys[idx] == 0 {
                return Some(idx);
            }
            idx = (idx + step) & mask;
            if idx == start {
                return None;
            }
        }
    }

    /// Trims retained entries down to nominal capacity `1 << lg_nom_size` via QuickSelect rebuild.
    pub fn trim(&mut self) {
        if self.num_retained > (1usize << self.lg_nom_size) {
            self.rebuild();
        }
    }

    /// Resets the table to its initial capacity, retention theta, and zero retained entries.
    pub fn reset(&mut self) {
        let init_lg_cur = starting_sub_multiple(
            self.lg_nom_size + 1,
            MIN_LG_K,
            self.resize_factor.lg_value(),
        );
        let size = 1usize << init_lg_cur;
        let stride = self.stride();
        self.keys.clear();
        self.keys.resize(size, 0);
        self.values.clear();
        self.values.resize(size * stride, 0.0);
        self.num_retained = 0;
        self.retention_theta = starting_retention_theta(self.sampling_probability);
        self.lg_cur_size = init_lg_cur;
    }

    /// Estimates heap memory usage in bytes across `keys` and `values` buffers.
    pub fn estimated_size(&self) -> usize {
        self.keys.capacity() * size_of::<u64>() + self.values.capacity() * size_of::<f64>()
    }

    /// Iterates over retained `(hash, values)` entries in hash-table slot order.
    pub fn iter(&self) -> AodTableIter<'_> {
        AodTableIter {
            keys: &self.keys,
            values: &self.values,
            stride: self.stride(),
            index: 0,
            remaining: self.num_retained,
        }
    }

    /// Materializes a non-empty [`AodCompactData`] from the table's current retained entries.
    pub fn to_non_empty_compact_data(&self, ordered: bool) -> AodCompactData {
        let mut hashes = Vec::with_capacity(self.num_retained);
        let mut values = Vec::with_capacity(self.num_retained * self.stride());
        if ordered && self.num_retained > 1 {
            let mut pairs = self.retained_slots();
            order_indices_by_hash(&mut pairs);
            for (hash, slot) in pairs {
                hashes.push(hash);
                values.extend_from_slice(self.slot_values(slot as usize));
            }
        } else {
            for entry in self.iter() {
                hashes.push(entry.hash());
                values.extend_from_slice(entry.values());
            }
        }
        let is_ordered = ordered || (hashes.len() == 1 && self.retention_theta == MAX_THETA);
        AodCompactData::from_raw_parts(
            false,
            is_ordered,
            self.seed_hash,
            self.retention_theta,
            self.num_values,
            hashes,
            values,
        )
    }

    fn rebuild(&mut self) {
        let k = 1usize << self.lg_nom_size;
        let mut pairs = self.retained_slots();
        let (_, kth, _) = pairs.select_nth_unstable_by_key(k, |&(hash, _)| hash);
        self.retention_theta = kth.0;
        pairs.truncate(k);

        let stride = self.stride();
        let (_, old_values) = self.replace_storage(self.lg_cur_size);
        for (hash, slot) in pairs {
            let start = slot as usize * stride;
            self.insert_unique(hash, &old_values[start..start + stride])
                .expect("selected entries are unique and below theta");
        }
        assert_eq!(self.num_retained, k);
    }

    fn resize(&mut self) {
        let new_lg_size = (self.lg_cur_size + self.resize_factor.lg_value()).min(self.lg_max_size);
        let stride = self.stride();
        let (old_keys, old_values) = self.replace_storage(new_lg_size);
        for (slot, hash) in old_keys.into_iter().enumerate() {
            if hash != 0 {
                self.insert_unique(hash, &old_values[slot * stride..(slot + 1) * stride])
                    .expect("resized table has room for every retained entry");
            }
        }
    }

    fn replace_storage(&mut self, lg_size: u8) -> (Vec<u64>, Vec<f64>) {
        let size = 1usize << lg_size;
        let values = vec![0.0; size * self.stride()];
        let old_keys = std::mem::replace(&mut self.keys, vec![0; size]);
        let old_values = std::mem::replace(&mut self.values, values);
        self.lg_cur_size = lg_size;
        self.num_retained = 0;
        (old_keys, old_values)
    }

    fn retained_slots(&self) -> Vec<(u64, u32)> {
        let mut pairs = Vec::with_capacity(self.num_retained);
        for (slot, &hash) in self.keys.iter().enumerate() {
            if hash != 0 {
                pairs.push((hash, slot as u32));
            }
        }
        pairs
    }
}

#[derive(Clone)]
pub(super) struct AodTableIter<'a> {
    keys: &'a [u64],
    values: &'a [f64],
    stride: usize,
    index: usize,
    remaining: usize,
}

impl<'a> Iterator for AodTableIter<'a> {
    type Item = ArrayOfDoublesEntry<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        while self.index < self.keys.len() {
            let slot = self.index;
            self.index += 1;
            let hash = self.keys[slot];
            if hash != 0 {
                self.remaining -= 1;
                let values = &self.values[slot * self.stride..(slot + 1) * self.stride];
                return Some(ArrayOfDoublesEntry::new(hash, values));
            }
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for AodTableIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

impl FusedIterator for AodTableIter<'_> {}

/// Flat Structure-of-Arrays backing storage for [`CompactArrayOfDoublesSketch`](crate::aod::CompactArrayOfDoublesSketch).
#[derive(Clone, Debug)]
pub(super) struct AodCompactData {
    is_empty: bool,
    is_ordered: bool,
    seed_hash: u16,
    theta: u64,
    num_values: u8,
    hashes: Vec<u64>,
    values: Vec<f64>,
}

impl AodCompactData {
    pub fn empty(seed_hash: u16, num_values: u8) -> Self {
        Self {
            is_empty: true,
            is_ordered: true,
            seed_hash,
            theta: MAX_THETA,
            num_values,
            hashes: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn from_raw_parts(
        is_empty: bool,
        is_ordered: bool,
        seed_hash: u16,
        theta: u64,
        num_values: u8,
        hashes: Vec<u64>,
        values: Vec<f64>,
    ) -> Self {
        debug_assert_eq!(values.len(), hashes.len() * usize::from(num_values));
        debug_assert!(!is_empty || (hashes.is_empty() && theta == MAX_THETA && is_ordered));
        Self {
            is_empty,
            is_ordered,
            seed_hash,
            theta,
            num_values,
            hashes,
            values,
        }
    }

    /// Canonicalizes non-empty flat `hashes` and `values`, sorting by ascending hash when
    /// `ordered && !already_ordered && hashes.len() > 1`. Exact singletons (`hashes.len() == 1`
    /// with `theta == MAX_THETA`) are always marked ordered.
    pub fn compact_non_empty(
        hashes: Vec<u64>,
        values: Vec<f64>,
        theta: u64,
        seed_hash: u16,
        num_values: u8,
        already_ordered: bool,
        ordered: bool,
    ) -> Self {
        let stride = usize::from(num_values);
        assert_eq!(values.len(), hashes.len() * stride);
        let is_ordered = ordered || already_ordered || (hashes.len() == 1 && theta == MAX_THETA);
        if ordered && !already_ordered && hashes.len() > 1 {
            let mut pairs = hashes
                .iter()
                .enumerate()
                .map(|(slot, &hash)| (hash, slot as u32))
                .collect();
            order_indices_by_hash(&mut pairs);
            let mut sorted_hashes = Vec::with_capacity(hashes.len());
            let mut sorted_values = Vec::with_capacity(values.len());
            for (hash, slot) in pairs {
                let start = slot as usize * stride;
                sorted_hashes.push(hash);
                sorted_values.extend_from_slice(&values[start..start + stride]);
            }
            return Self::from_raw_parts(
                false,
                is_ordered,
                seed_hash,
                theta,
                num_values,
                sorted_hashes,
                sorted_values,
            );
        }
        Self::from_raw_parts(
            false, is_ordered, seed_hash, theta, num_values, hashes, values,
        )
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    #[inline]
    pub fn is_ordered(&self) -> bool {
        self.is_ordered
    }

    #[inline]
    pub fn is_estimation_mode(&self) -> bool {
        !self.is_empty && self.theta < MAX_THETA
    }

    #[inline]
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    #[inline]
    pub fn theta(&self) -> u64 {
        self.theta
    }

    #[inline]
    pub fn num_values(&self) -> u8 {
        self.num_values
    }

    #[inline]
    pub fn hashes(&self) -> &[u64] {
        &self.hashes
    }

    #[inline]
    pub fn values(&self) -> &[f64] {
        &self.values
    }

    #[inline]
    pub fn num_retained(&self) -> usize {
        self.hashes.len()
    }

    #[inline]
    pub fn estimated_size(&self) -> usize {
        self.hashes.capacity() * size_of::<u64>() + self.values.capacity() * size_of::<f64>()
    }

    pub fn iter(&self) -> AodCompactIter<'_> {
        AodCompactIter {
            hashes: &self.hashes,
            values: &self.values,
            stride: usize::from(self.num_values),
            index: 0,
        }
    }
}

#[derive(Clone)]
pub(super) struct AodCompactIter<'a> {
    hashes: &'a [u64],
    values: &'a [f64],
    stride: usize,
    index: usize,
}

impl<'a> Iterator for AodCompactIter<'a> {
    type Item = ArrayOfDoublesEntry<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let hash = *self.hashes.get(self.index)?;
        let slot = self.index;
        self.index += 1;
        let values = &self.values[slot * self.stride..(slot + 1) * self.stride];
        Some(ArrayOfDoublesEntry::new(hash, values))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.hashes.len() - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for AodCompactIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.hashes.len() - self.index
    }
}

impl FusedIterator for AodCompactIter<'_> {}

/// Sorts `(hash, source_index)` pairs in ascending hash order using a top-bit bucket pass for
/// `pairs.len() >= 256` with `sort_unstable_by_key` fallback for small or skewed inputs.
pub(super) fn order_indices_by_hash(pairs: &mut Vec<(u64, u32)>) {
    const MIN_BUCKET_SORT_LEN: usize = 256;
    const MAX_BUCKET_LEN: u32 = 32;

    let len = pairs.len();
    if len < MIN_BUCKET_SORT_LEN {
        if len > 1 {
            pairs.sort_unstable_by_key(|&(hash, _)| hash);
        }
        return;
    }

    let bits = pairs.iter().fold(0u64, |acc, &(hash, _)| acc | hash);
    let lg_buckets = len.ilog2();
    let num_buckets = 1usize << lg_buckets;
    let mask = num_buckets - 1;
    let shift = (u64::BITS - bits.leading_zeros()).saturating_sub(lg_buckets);
    let bucket = |hash: u64| (hash >> shift) as usize & mask;

    let mut stack_10;
    let mut stack_12;
    let mut heap_starts;
    let starts: &mut [u32] = if lg_buckets <= 10 {
        stack_10 = [0u32; (1 << 10) + 1];
        &mut stack_10[..=num_buckets]
    } else if lg_buckets <= 12 {
        stack_12 = [0u32; (1 << 12) + 1];
        &mut stack_12[..=num_buckets]
    } else {
        heap_starts = vec![0u32; num_buckets + 1];
        &mut heap_starts
    };

    for &(hash, _) in pairs.iter() {
        starts[bucket(hash) + 1] += 1;
    }
    let mut largest = 0;
    for i in 1..starts.len() {
        largest = largest.max(starts[i]);
        starts[i] += starts[i - 1];
    }
    if largest > MAX_BUCKET_LEN {
        pairs.sort_unstable_by_key(|&(hash, _)| hash);
        return;
    }

    let mut sorted = vec![pairs[0]; len];
    for &pair in pairs.iter() {
        let next = &mut starts[bucket(pair.0)];
        sorted[*next as usize] = pair;
        *next += 1;
    }
    for i in 1..len {
        let pair = sorted[i];
        let mut j = i;
        while j > 0 && sorted[j - 1].0 > pair.0 {
            sorted[j] = sorted[j - 1];
            j -= 1;
        }
        sorted[j] = pair;
    }
    *pairs = sorted;
}
