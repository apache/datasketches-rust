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

use std::iter::FusedIterator;

use crate::aod::ArrayOfDoublesEntry;
use crate::aod::CompactArrayOfDoublesSketch;
use crate::aod::policy::validate_num_values;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::thetafamily::common::constants::MAX_LG_K;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::constants::MIN_LG_K;
use crate::thetafamily::common::hash_table::RawHashTable;
use crate::thetafamily::common::hash_table::TableSlots;
use crate::thetafamily::common::sort::sort_by_hash_key;

/// Contiguous Structure-of-Arrays slot storage for [`AodHashTable`].
#[derive(Debug, Clone)]
pub(super) struct AodSlots {
    pub(super) num_values: u8,
    pub(super) keys: Vec<u64>,
    pub(super) values: Vec<f64>,
}

impl AodSlots {
    fn new(size: usize, num_values: u8) -> Self {
        Self {
            num_values,
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
}

impl TableSlots for AodSlots {
    #[inline]
    fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    fn key_at(&self, slot: usize) -> u64 {
        self.keys[slot]
    }

    fn allocate_like(&self, size: usize) -> Self {
        Self::new(size, self.num_values)
    }

    fn clear_and_resize(&mut self, size: usize) {
        self.keys.clear();
        self.keys.resize(size, 0);
        self.values.clear();
        self.values.resize(size * self.stride(), 0.0);
    }

    #[inline]
    fn move_slot(&mut self, src_slot: usize, dst: &mut Self, dst_slot: usize) {
        let stride = self.stride();
        dst.keys[dst_slot] = self.keys[src_slot];
        dst.values[dst_slot * stride..(dst_slot + 1) * stride]
            .copy_from_slice(&self.values[src_slot * stride..(src_slot + 1) * stride]);
    }

    fn estimated_size(&self) -> usize {
        self.keys.capacity() * size_of::<u64>() + self.values.capacity() * size_of::<f64>()
    }
}

pub(super) type AodHashTable = RawHashTable<AodSlots>;

impl AodHashTable {
    pub fn new_aod(
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
        num_values: u8,
    ) -> Result<Self, Error> {
        validate_num_values(num_values)?;
        if (MIN_LG_K..=MAX_LG_K).contains(&lg_nom_size) {
            let max_slots = 1usize
                .checked_shl(u32::from(lg_nom_size + 1))
                .ok_or_else(|| {
                    Error::invalid_argument("table capacity overflows platform usize")
                })?;
            max_slots
                .checked_mul(usize::from(num_values))
                .and_then(|len| len.checked_mul(size_of::<f64>()))
                .filter(|&bytes| bytes <= isize::MAX as usize)
                .ok_or_else(|| {
                    Error::invalid_argument(format!(
                        "lg_k={lg_nom_size} with num_values={num_values} exceeds addressable allocation size"
                    ))
                })?;
        }
        Self::new_with_slots(
            lg_nom_size,
            resize_factor,
            sampling_probability,
            seed,
            |size| AodSlots::new(size, num_values),
        )
    }

    pub fn for_intersection(
        lg_cur_size: u8,
        lg_nom_size: u8,
        retention_theta: u64,
        seed: u64,
        seed_hash: u16,
        num_values: u8,
    ) -> Self {
        Self::for_set_operation_with_slots(
            lg_cur_size,
            lg_nom_size,
            retention_theta,
            seed,
            seed_hash,
            |size| AodSlots::new(size, num_values),
        )
    }

    #[inline]
    pub fn num_values(&self) -> u8 {
        self.slots().num_values
    }

    pub fn insert_or_add(&mut self, hash: u64, incoming: &[f64]) {
        let Some((idx, occupied)) = self.probe_slot(hash) else {
            return;
        };
        let slots = self.slots_mut();
        let stride = slots.stride();
        let dst = &mut slots.values[idx * stride..(idx + 1) * stride];
        if occupied {
            for (value, delta) in dst.iter_mut().zip(incoming) {
                *value += delta;
            }
            return;
        }
        dst.copy_from_slice(incoming);
        slots.keys[idx] = hash;
        self.finish_insert();
    }

    pub fn insert_unique(&mut self, hash: u64, incoming: &[f64]) -> bool {
        let Some((idx, false)) = self.probe_slot(hash) else {
            return false;
        };
        let slots = self.slots_mut();
        let stride = slots.stride();
        slots.values[idx * stride..(idx + 1) * stride].copy_from_slice(incoming);
        slots.keys[idx] = hash;
        self.finish_insert();
        true
    }

    #[inline]
    pub fn find_values(&self, hash: u64) -> Option<&[f64]> {
        let slot = self.find_occupied_slot(hash)?;
        Some(self.slots().slot_values(slot))
    }

    pub fn iter(&self) -> AodIter<'_> {
        let slots = self.slots();
        AodIter::new(
            &slots.keys,
            &slots.values,
            slots.num_values,
            self.num_retained(),
        )
    }

    pub fn materialize_compact(
        &self,
        theta: u64,
        is_ordered: bool,
        selected: &[(u64, u32)],
    ) -> CompactArrayOfDoublesSketch {
        let slots = self.slots();
        let mut hashes = Vec::with_capacity(selected.len());
        let mut values = Vec::with_capacity(selected.len() * slots.stride());
        for &(hash, slot) in selected {
            hashes.push(hash);
            values.extend_from_slice(slots.slot_values(slot as usize));
        }
        CompactArrayOfDoublesSketch::from_raw_parts(
            false,
            is_ordered,
            self.seed_hash(),
            theta,
            slots.num_values,
            hashes,
            values,
        )
    }

    pub fn to_non_empty_compact(&self, ordered: bool) -> CompactArrayOfDoublesSketch {
        let num_retained = self.num_retained();
        let slots = self.slots();
        if ordered && num_retained > 1 {
            let mut pairs = Vec::with_capacity(num_retained);
            for (slot, &hash) in slots.keys.iter().enumerate() {
                if hash != 0 {
                    pairs.push((hash, slot as u32));
                }
            }
            sort_by_hash_key(&mut pairs, |&(hash, _)| hash);
            return self.materialize_compact(self.retention_theta(), true, &pairs);
        }
        let mut hashes = Vec::with_capacity(num_retained);
        let mut values = Vec::with_capacity(num_retained * slots.stride());
        for entry in self.iter() {
            hashes.push(entry.hash());
            values.extend_from_slice(entry.values());
        }
        let is_ordered = ordered || (hashes.len() == 1 && self.retention_theta() == MAX_THETA);
        CompactArrayOfDoublesSketch::from_raw_parts(
            false,
            is_ordered,
            self.seed_hash(),
            self.retention_theta(),
            slots.num_values,
            hashes,
            values,
        )
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
        if self.keys.len() - self.index == self.remaining {
            let slot = self.index;
            self.index += 1;
            self.remaining -= 1;
            let values = &self.values[slot * self.stride..(slot + 1) * self.stride];
            return Some(ArrayOfDoublesEntry::new(self.keys[slot], values));
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
