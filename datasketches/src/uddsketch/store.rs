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

use std::sync::OnceLock;

use crate::uddsketch::mapping::collapse_key;

const EMPTY_SLOT: u32 = u32::MAX;
const INDEX_THRESHOLD: usize = 16;
const MIN_SLOTS_CAP: usize = 64;
const HASH_MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

#[derive(Clone, Copy, Debug)]
pub struct Bucket {
    pub key: i64,
    pub count: u64,
}

/// Counter buckets for one sign (positive magnitudes) with append-only out-of-order accumulation,
/// a lazy open-addressed `u32` hash index once retained buckets reach `INDEX_THRESHOLD`, and
/// cached/deferred sorting for queries, merges, collapses, and serialization.
#[derive(Debug)]
pub struct BucketStore {
    entries: Vec<Bucket>,
    total_weight: u64,
    slots: Vec<u32>,
    sorted: bool,
    sorted_cache: OnceLock<Box<[Bucket]>>,
}

impl Default for BucketStore {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            total_weight: 0,
            slots: Vec::new(),
            sorted: true,
            sorted_cache: OnceLock::new(),
        }
    }
}

impl Clone for BucketStore {
    fn clone(&self) -> Self {
        if let Some(cached) = self.sorted_cache.get() {
            Self {
                entries: cached.to_vec(),
                total_weight: self.total_weight,
                slots: Vec::new(),
                sorted: true,
                sorted_cache: OnceLock::new(),
            }
        } else {
            Self {
                entries: self.entries.clone(),
                total_weight: self.total_weight,
                slots: self.slots.clone(),
                sorted: self.sorted,
                sorted_cache: OnceLock::new(),
            }
        }
    }
}

impl BucketStore {
    pub fn from_sorted_unchecked(entries: Vec<Bucket>, total_weight: u64) -> Self {
        Self {
            entries,
            total_weight,
            slots: Vec::new(),
            sorted: true,
            sorted_cache: OnceLock::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    pub fn estimated_size(&self) -> usize {
        let cache_bytes = self
            .sorted_cache
            .get()
            .map_or(0, |cached| cached.len() * size_of::<Bucket>());
        self.entries.capacity() * size_of::<Bucket>()
            + self.slots.capacity() * size_of::<u32>()
            + cache_bytes
    }

    pub fn reset(&mut self) {
        self.entries.clear();
        self.total_weight = 0;
        // Retain allocated capacity without zeroing unused slots for subsequent small streams.
        self.slots.clear();
        self.sorted = true;
        self.sorted_cache.take();
    }

    pub fn clone_from_store(&mut self, other: &Self) {
        self.entries.clear();
        self.entries.extend_from_slice(other.sorted_buckets());
        self.total_weight = other.total_weight;
        self.slots.clear();
        self.sorted = true;
        self.sorted_cache.take();
    }

    #[inline]
    pub fn insert(&mut self, key: i64, weight: u64) {
        debug_assert!(weight > 0);
        self.sorted_cache.take();

        if let Some(last) = self.entries.last_mut() {
            if last.key == key {
                last.count += weight;
                self.total_weight += weight;
                return;
            }
            if self.sorted && last.key < key {
                let idx = self.entries.len() as u32;
                self.entries.push(Bucket { key, count: weight });
                self.total_weight += weight;
                if !self.slots.is_empty() {
                    if self.entries.len() * 2 > self.slots.len() {
                        self.rebuild_slots();
                    } else {
                        self.insert_slot(key, idx);
                    }
                }
                return;
            }
        } else {
            self.entries.push(Bucket { key, count: weight });
            self.total_weight = weight;
            self.sorted = true;
            return;
        }

        if self.slots.is_empty() && self.entries.len() < INDEX_THRESHOLD {
            for bucket in &mut self.entries {
                if bucket.key == key {
                    bucket.count += weight;
                    self.total_weight += weight;
                    return;
                }
            }
            self.entries.push(Bucket { key, count: weight });
            self.total_weight += weight;
            self.sorted = false;
            return;
        }

        if self.slots.is_empty() {
            self.rebuild_slots();
        }
        let mask = self.slots.len() - 1;
        let mut probe = hash_key(key) & mask;
        loop {
            let slot = self.slots[probe];
            if slot == EMPTY_SLOT {
                break;
            }
            let idx = slot as usize;
            if self.entries[idx].key == key {
                self.entries[idx].count += weight;
                self.total_weight += weight;
                return;
            }
            probe = (probe + 1) & mask;
        }

        let idx = self.entries.len() as u32;
        if self.sorted && self.entries[idx as usize - 1].key > key {
            self.sorted = false;
        }
        self.entries.push(Bucket { key, count: weight });
        self.total_weight += weight;

        if self.entries.len() * 2 > self.slots.len() {
            self.rebuild_slots();
        } else {
            self.slots[probe] = idx;
        }
    }

    /// Returns the retained buckets in strictly ascending `key` order, sorting lazily once into
    /// `sorted_cache` when `self` accumulated out-of-order buckets without collapsing.
    #[inline]
    pub fn sorted_buckets(&self) -> &[Bucket] {
        if self.sorted || self.entries.len() <= 1 {
            &self.entries
        } else {
            self.sorted_cache.get_or_init(|| {
                let mut sorted = self.entries.clone().into_boxed_slice();
                sorted.sort_unstable_by_key(|b| b.key);
                sorted
            })
        }
    }

    /// Collapses all bucket keys by `shift` levels in-place (`ceil(key / 2^shift)`).
    pub fn collapse(&mut self, shift: u8) {
        if shift == 0 || self.entries.is_empty() {
            return;
        }
        self.ensure_sorted();
        let mut write = 0usize;
        for read in 0..self.entries.len() {
            let mut bucket = self.entries[read];
            bucket.key = collapse_key(bucket.key, shift);
            if write > 0 && self.entries[write - 1].key == bucket.key {
                self.entries[write - 1].count += bucket.count;
            } else {
                self.entries[write] = bucket;
                write += 1;
            }
        }
        self.entries.truncate(write);
        self.slots.clear();
    }

    /// Merges `other`'s buckets (collapsed on the fly by `other_shift` levels) into `self` (first
    /// collapsed by `self_shift` levels). Caller validates combined weight overflow beforehand.
    pub fn merge_collapsed(&mut self, self_shift: u8, other: &Self, other_shift: u8) {
        if self_shift > 0 {
            self.collapse(self_shift);
        } else {
            self.ensure_sorted();
        }
        if other.is_empty() {
            return;
        }
        if self.is_empty() && other_shift == 0 {
            self.clone_from_store(other);
            return;
        }

        let right = other.sorted_buckets();
        let m = self.entries.len();
        let n = right.len();
        let total_cap = m + n;
        self.entries.resize(total_cap, Bucket { key: 0, count: 0 });

        let mut i = m;
        let mut j = n;
        let mut write = total_cap;

        // `write >= i + j` holds throughout: each step decrements `write` by 1 and `i + j` by
        // at least 1, so `write - 1 >= i` whenever `j > 0` and unread `self.entries[..i]` is
        // intact.
        while j > 0 {
            let b_key = collapse_key(right[j - 1].key, other_shift);
            let mut b_count = right[j - 1].count;
            j -= 1;
            while j > 0 && collapse_key(right[j - 1].key, other_shift) == b_key {
                b_count += right[j - 1].count;
                j -= 1;
            }

            while i > 0 && self.entries[i - 1].key > b_key {
                write -= 1;
                self.entries[write] = self.entries[i - 1];
                i -= 1;
            }

            write -= 1;
            if i > 0 && self.entries[i - 1].key == b_key {
                self.entries[write] = Bucket {
                    key: b_key,
                    count: self.entries[i - 1].count + b_count,
                };
                i -= 1;
            } else {
                self.entries[write] = Bucket {
                    key: b_key,
                    count: b_count,
                };
            }
        }

        if write > i {
            let gap = write - i;
            while i > 0 {
                write -= 1;
                self.entries[write] = self.entries[i - 1];
                i -= 1;
            }
            self.entries.copy_within(gap..total_cap, 0);
            self.entries.truncate(total_cap - gap);
        }

        self.total_weight += other.total_weight;
        // Defer index rebuild until budget enforcement finishes collapsing if needed.
        self.slots.clear();
    }

    #[inline]
    fn ensure_sorted(&mut self) {
        if let Some(cached) = self.sorted_cache.take() {
            self.entries.copy_from_slice(&cached);
            self.slots.clear();
            self.sorted = true;
        } else if !self.sorted {
            if self.entries.len() > 1 {
                self.entries.sort_unstable_by_key(|b| b.key);
            }
            self.slots.clear();
            self.sorted = true;
        }
    }

    fn rebuild_slots(&mut self) {
        let needed = (self.entries.len() * 2)
            .next_power_of_two()
            .max(MIN_SLOTS_CAP);
        self.slots.clear();
        self.slots.resize(needed, EMPTY_SLOT);
        let mask = needed - 1;
        for (idx, bucket) in self.entries.iter().enumerate() {
            let mut probe = hash_key(bucket.key) & mask;
            while self.slots[probe] != EMPTY_SLOT {
                probe = (probe + 1) & mask;
            }
            self.slots[probe] = idx as u32;
        }
    }

    #[inline]
    fn insert_slot(&mut self, key: i64, idx: u32) {
        let mask = self.slots.len() - 1;
        let mut probe = hash_key(key) & mask;
        while self.slots[probe] != EMPTY_SLOT {
            probe = (probe + 1) & mask;
        }
        self.slots[probe] = idx;
    }
}

#[inline]
pub fn key_at_rank_in_slice(entries: &[Bucket], total_weight: u64, target: u64) -> i64 {
    debug_assert!(target < total_weight);
    // Walk from the nearer end so high-percentile queries (`p90`, `p99`) touch only the tail.
    if target <= total_weight / 2 {
        let mut acc = 0u64;
        for bucket in entries {
            acc += bucket.count;
            if target < acc {
                return bucket.key;
            }
        }
        entries.last().unwrap().key
    } else {
        let mut rem = total_weight;
        for bucket in entries.iter().rev() {
            rem -= bucket.count;
            if target >= rem {
                return bucket.key;
            }
        }
        entries[0].key
    }
}

#[inline(always)]
fn hash_key(key: i64) -> usize {
    let mixed = (key as u64).wrapping_mul(HASH_MULTIPLIER);
    (mixed ^ (mixed >> 32)) as usize
}
