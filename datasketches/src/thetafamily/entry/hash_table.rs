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
use std::slice;

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::thetafamily::common::hash_table::RawHashTable;
use crate::thetafamily::common::hash_table::TableSlots;
use crate::thetafamily::common::hash_table::lg_size_from_count_for_rebuild;
use crate::thetafamily::entry::SketchEntry;
use crate::thetafamily::entry::sketch_state::CompactSketchState;

impl<E: SketchEntry> TableSlots for Vec<Option<E>> {
    #[inline]
    fn len(&self) -> usize {
        Vec::len(self)
    }

    #[inline]
    fn key_at(&self, slot: usize) -> u64 {
        self[slot].as_ref().map_or(0, SketchEntry::hash)
    }

    fn allocate_like(&self, size: usize) -> Self {
        std::iter::repeat_with(|| None).take(size).collect()
    }

    fn clear_and_resize(&mut self, size: usize) {
        self.clear();
        self.resize_with(size, || None);
    }

    #[inline]
    fn move_slot(&mut self, src_slot: usize, dst: &mut Self, dst_slot: usize) {
        dst[dst_slot] = self[src_slot].take();
    }

    fn estimated_size(&self) -> usize {
        self.capacity() * size_of::<Option<E>>()
    }
}

pub struct SketchHashTableIter<'a, E> {
    iter: slice::Iter<'a, Option<E>>,
    remaining: usize,
}

impl<'a, E> Iterator for SketchHashTableIter<'a, E> {
    type Item = &'a E;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let entry = self.iter.find_map(Option::as_ref)?;
        self.remaining -= 1;
        Some(entry)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<E> ExactSizeIterator for SketchHashTableIter<'_, E> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

impl<E> FusedIterator for SketchHashTableIter<'_, E> {}

/// AoS hash-table instantiation used by Theta and Tuple sketches.
pub type SketchHashTable<E> = RawHashTable<Vec<Option<E>>>;

impl<E: SketchEntry> SketchHashTable<E> {
    pub fn new(
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
    ) -> Result<Self, Error> {
        Self::new_with_slots(
            lg_nom_size,
            resize_factor,
            sampling_probability,
            seed,
            |n| std::iter::repeat_with(|| None).take(n).collect(),
        )
    }

    pub fn for_set_operation(
        lg_cur_size: u8,
        lg_nom_size: u8,
        retention_theta: u64,
        seed: u64,
        seed_hash: u16,
    ) -> Self {
        Self::for_set_operation_with_slots(
            lg_cur_size,
            lg_nom_size,
            retention_theta,
            seed,
            seed_hash,
            |n| std::iter::repeat_with(|| None).take(n).collect(),
        )
    }

    pub fn upsert_entry<F>(&mut self, hash: u64, f: F) -> bool
    where
        F: FnOnce(Option<&mut E>) -> Option<E>,
    {
        let Some((index, occupied)) = self.probe_slot(hash) else {
            return false;
        };
        if occupied {
            f(self.slots_mut()[index].as_mut());
            return false;
        }
        let Some(entry) = f(None) else {
            return false;
        };
        debug_assert_eq!(entry.hash(), hash, "entry hash must match insertion hash");
        self.slots_mut()[index] = Some(entry);
        self.finish_insert();
        true
    }

    #[inline]
    pub fn entry(&self, hash: u64) -> Option<&E> {
        let index = self.find_occupied_slot(hash)?;
        self.slots()[index].as_ref()
    }

    pub fn iter_entries(&self) -> SketchHashTableIter<'_, E> {
        SketchHashTableIter {
            iter: self.slots().iter(),
            remaining: self.num_retained(),
        }
    }

    pub fn to_non_empty_compact_state(&self, ordered: bool) -> CompactSketchState<E>
    where
        E: Clone,
    {
        let retained_entries = self.iter_entries().cloned().collect();
        CompactSketchState::compact(
            retained_entries,
            self.retention_theta(),
            self.seed_hash(),
            false,
            ordered,
        )
    }

    #[cfg(feature = "theta")]
    pub fn to_non_empty_compact_hashes(&self, ordered: bool) -> CompactSketchState<u64> {
        let hashes = self.retained_hashes();
        CompactSketchState::compact(
            hashes,
            self.retention_theta(),
            self.seed_hash(),
            false,
            ordered,
        )
    }

    #[cfg(feature = "theta")]
    pub fn retained_hashes(&self) -> Vec<u64> {
        if self.num_retained() * 16 <= self.slots().len() {
            return self.iter_entries().map(SketchEntry::hash).collect();
        }
        let mut hashes = vec![0u64; self.num_retained() + 1];
        let mut len = 0;
        for slot in self.slots() {
            let hash = slot.as_ref().map_or(0, SketchEntry::hash);
            hashes[len] = hash;
            len += usize::from(hash != 0);
        }
        debug_assert_eq!(len, self.num_retained());
        hashes.truncate(len);
        hashes
    }

    pub fn lg_size_from_count_for_rebuild(count: usize, load_factor: f64) -> u8 {
        lg_size_from_count_for_rebuild(count, load_factor)
    }
}
