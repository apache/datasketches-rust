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

use super::mapping::collapse_key;

const EMPTY_SLOT: u32 = u32::MAX;
const INDEX_THRESHOLD: usize = 32;
const MIN_SLOTS_CAP: usize = 128;
const HASH_MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

#[derive(Clone, Copy, Debug)]
pub struct Bucket {
    pub key: i64,
    pub count: u64,
}

/// Sorted counter buckets for one sign (positive magnitudes) with a lazy open-addressed hash index
/// once non-monotonic lookups exceed `INDEX_THRESHOLD` retained buckets.
#[derive(Clone, Debug, Default)]
pub struct BucketStore {
    entries: Vec<Bucket>,
    total_weight: u64,
    slots: Vec<u32>,
}

impl BucketStore {
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
        self.entries.capacity() * size_of::<Bucket>() + self.slots.capacity() * size_of::<u32>()
    }

    pub fn reset(&mut self) {
        self.entries.clear();
        self.total_weight = 0;
        // Retain allocated capacity without zeroing unused slots for subsequent small streams.
        self.slots.clear();
    }

    pub fn clone_from_store(&mut self, other: &Self) {
        self.entries.clone_from(&other.entries);
        self.total_weight = other.total_weight;
        self.slots.clear();
    }

    #[inline]
    pub fn insert(&mut self, key: i64, weight: u64) {
        debug_assert!(weight > 0);
        if let Some(last) = self.entries.last_mut() {
            if last.key == key {
                last.count += weight;
                self.total_weight += weight;
                return;
            }
            if last.key < key {
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
            return;
        }

        if self.entries.len() > INDEX_THRESHOLD && self.slots.is_empty() {
            self.rebuild_slots();
        }
        if !self.slots.is_empty() {
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
        }

        let pos = match self.entries.binary_search_by_key(&key, |b| b.key) {
            Ok(idx) => {
                self.entries[idx].count += weight;
                self.total_weight += weight;
                return;
            }
            Err(pos) => pos,
        };

        self.entries.insert(pos, Bucket { key, count: weight });
        self.total_weight += weight;

        if !self.slots.is_empty() {
            if self.entries.len() * 2 > self.slots.len() {
                self.rebuild_slots();
            } else {
                let threshold = pos as u32;
                for slot in &mut self.slots {
                    if *slot != EMPTY_SLOT && *slot >= threshold {
                        *slot += 1;
                    }
                }
                self.insert_slot(key, pos as u32);
            }
        }
    }

    /// Collapses all bucket keys by `shift` levels in-place (`ceil(key / 2^shift)`).
    pub fn collapse(&mut self, shift: u8) {
        if shift == 0 || self.entries.is_empty() {
            return;
        }
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
        }
        if other.is_empty() {
            return;
        }
        if self.is_empty() && other_shift == 0 {
            self.clone_from_store(other);
            return;
        }

        let m = self.entries.len();
        let n = other.entries.len();
        let total_cap = m + n;
        self.entries.resize(total_cap, Bucket { key: 0, count: 0 });

        let right = &other.entries;
        let mut i = m;
        let mut j = n;
        let mut write = total_cap;

        // `write >= i + j` holds throughout: each step decrements `write` by 1 and `i + j` by at
        // least 1, so `write - 1 >= i` whenever `j > 0` and unread `self.entries[..i]` is intact.
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

    /// Returns the bucket key containing zero-based rank `target` (`target < self.total_weight`).
    pub fn key_at_rank(&self, target: u64) -> i64 {
        debug_assert!(target < self.total_weight);
        // Walk from the nearer end so high-percentile queries (`p90`, `p99`) touch only the tail.
        if target <= self.total_weight / 2 {
            let mut acc = 0u64;
            for bucket in &self.entries {
                acc += bucket.count;
                if target < acc {
                    return bucket.key;
                }
            }
            self.entries.last().unwrap().key
        } else {
            let mut rem = self.total_weight;
            for bucket in self.entries.iter().rev() {
                rem -= bucket.count;
                if target >= rem {
                    return bucket.key;
                }
            }
            self.entries[0].key
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

#[inline(always)]
fn hash_key(key: i64) -> usize {
    let mixed = (key as u64).wrapping_mul(HASH_MULTIPLIER);
    (mixed ^ (mixed >> 32)) as usize
}
