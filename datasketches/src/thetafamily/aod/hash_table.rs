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
use crate::aod::CompactArrayOfDoublesSketch;
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
use crate::thetafamily::common::sort::sort_by_hash_key;

/// Flat open-addressed hash table storing `u64` keys alongside a strided `Vec<f64>` summary buffer.
///
/// Slot `i` holds key `keys[i]` (`0` when unoccupied) and values
/// `values[i * stride..(i + 1) * stride]`, where `stride = usize::from(num_values)`.
#[derive(Debug, Clone)]
pub(super) struct AodHashTable {
    lg_cur_size: u8,
    pub(super) lg_nom_size: u8,
    lg_max_size: u8,
    pub(super) num_values: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    pub(super) seed: u64,
    pub(super) seed_hash: u16,
    pub(super) retention_theta: u64,
    pub(super) num_retained: usize,
    pub(super) keys: Vec<u64>,
    pub(super) values: Vec<f64>,
}

impl AodHashTable {
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
        assert!(lg_cur_size <= lg_max_size);
        let size = if lg_cur_size > 0 {
            1usize << lg_cur_size
        } else {
            0
        };
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
            values: vec![0.0; size * usize::from(num_values)],
        }
    }

    #[inline]
    pub fn stride(&self) -> usize {
        usize::from(self.num_values)
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
        hasher.finish128().0 >> 1
    }

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
        let threshold = if self.lg_cur_size <= self.lg_nom_size {
            self.keys.len() / 2
        } else {
            self.keys.len() - self.keys.len() / 16
        };
        if self.num_retained > threshold {
            if self.lg_cur_size <= self.lg_nom_size {
                self.resize();
            } else {
                self.rebuild();
            }
        }
    }

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

    pub fn trim(&mut self) {
        if self.num_retained > (1usize << self.lg_nom_size) {
            self.rebuild();
        }
    }

    pub fn reset(&mut self) {
        let init_lg_cur = starting_sub_multiple(
            self.lg_nom_size + 1,
            MIN_LG_K,
            self.resize_factor.lg_value(),
        );
        let size = 1usize << init_lg_cur;
        self.keys.clear();
        self.keys.resize(size, 0);
        self.values.clear();
        self.values.resize(size * self.stride(), 0.0);
        self.num_retained = 0;
        self.retention_theta = starting_retention_theta(self.sampling_probability);
        self.lg_cur_size = init_lg_cur;
    }

    pub fn estimated_size(&self) -> usize {
        self.keys.capacity() * size_of::<u64>() + self.values.capacity() * size_of::<f64>()
    }

    pub fn iter(&self) -> AodIter<'_> {
        AodIter::new(&self.keys, &self.values, self.num_values, self.num_retained)
    }

    pub fn materialize_compact(
        &self,
        theta: u64,
        is_ordered: bool,
        slots: &[(u64, u32)],
    ) -> CompactArrayOfDoublesSketch {
        let mut hashes = Vec::with_capacity(slots.len());
        let mut values = Vec::with_capacity(slots.len() * self.stride());
        for &(hash, slot) in slots {
            hashes.push(hash);
            values.extend_from_slice(self.slot_values(slot as usize));
        }
        CompactArrayOfDoublesSketch::from_raw_parts(
            false,
            is_ordered,
            self.seed_hash,
            theta,
            self.num_values,
            hashes,
            values,
        )
    }

    pub fn to_non_empty_compact(&self, ordered: bool) -> CompactArrayOfDoublesSketch {
        if ordered && self.num_retained > 1 {
            let mut slots = self.retained_slots();
            sort_by_hash_key(&mut slots, |&(hash, _)| hash);
            return self.materialize_compact(self.retention_theta, true, &slots);
        }
        let mut hashes = Vec::with_capacity(self.num_retained);
        let mut values = Vec::with_capacity(self.num_retained * self.stride());
        for entry in self.iter() {
            hashes.push(entry.hash());
            values.extend_from_slice(entry.values());
        }
        let is_ordered = ordered || (hashes.len() == 1 && self.retention_theta == MAX_THETA);
        CompactArrayOfDoublesSketch::from_raw_parts(
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
pub(super) struct AodIter<'a> {
    keys: &'a [u64],
    values: &'a [f64],
    stride: usize,
    index: usize,
    remaining: usize,
}

impl<'a> AodIter<'a> {
    #[inline]
    pub fn new(keys: &'a [u64], values: &'a [f64], num_values: u8, remaining: usize) -> Self {
        Self {
            keys,
            values,
            stride: usize::from(num_values),
            index: 0,
            remaining,
        }
    }
}

impl<'a> Iterator for AodIter<'a> {
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

impl ExactSizeIterator for AodIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

impl FusedIterator for AodIter<'_> {}
