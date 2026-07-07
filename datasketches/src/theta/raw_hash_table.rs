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

use crate::common::ResizeFactor;
use crate::hash::MurmurHash3X64128;
use crate::hash::compute_seed_hash;
use crate::theta::HASH_TABLE_REBUILD_THRESHOLD;
use crate::theta::HASH_TABLE_RESIZE_THRESHOLD;
use crate::theta::MAX_THETA;
use crate::theta::MIN_LG_K;
use crate::theta::STRIDE_MASK;

pub(crate) trait RawHashTableEntry {
    fn hash(&self) -> u64;
}

/// Generic hash-table mechanics shared by Theta and Tuple sketches.
///
/// The entry type supplies the retained hash and any sketch-specific payload. The table owns all
/// theta screening, probing, resizing, rebuilding, trimming, and logical-empty state.
#[derive(Debug)]
pub(crate) struct RawHashTable<E> {
    pub(crate) lg_cur_size: u8,
    pub(crate) lg_nom_size: u8,
    pub(crate) lg_max_size: u8,
    pub(crate) resize_factor: ResizeFactor,
    pub(crate) sampling_probability: f32,
    pub(crate) hash_seed: u64,

    // Logical emptiness of the source set.
    //
    // * `false` if any update has been attempted (even if screened by theta)
    // * `true` if no updates have been attempted.
    //
    // This can be false even when `num_retained` is 0.
    pub(crate) is_empty: bool,

    pub(crate) theta: u64,

    pub(crate) entries: Vec<Option<E>>,

    // Number of retained non-zero hashes currently stored in `entries`.
    pub(crate) num_retained: usize,
}

impl<E> RawHashTable<E>
where
    E: RawHashTableEntry,
{
    /// Create a new hash table.
    pub(crate) fn new(
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        hash_seed: u64,
    ) -> Self {
        let lg_max_size = lg_nom_size + 1;
        let lg_cur_size = starting_sub_multiple(lg_max_size, MIN_LG_K, resize_factor.lg_value());
        Self::from_raw_parts(
            lg_cur_size,
            lg_nom_size,
            resize_factor,
            sampling_probability,
            starting_theta_from_sampling_probability(sampling_probability),
            hash_seed,
            true,
        )
    }

    /// Constructs a table from raw internal state.
    ///
    /// # Panics
    ///
    /// Panics if `lg_cur_size > lg_nom_size + 1`. (`lg_nom_size + 1 == lg_max_size`)
    pub(crate) fn from_raw_parts(
        lg_cur_size: u8,
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        theta: u64,
        hash_seed: u64,
        is_empty: bool,
    ) -> Self {
        let lg_max_size = lg_nom_size + 1;
        assert!(
            lg_cur_size <= lg_max_size,
            "lg_cur_size must be <= lg_nom_size + 1, got lg_cur_size={lg_cur_size}, lg_nom_size={lg_nom_size}"
        );
        let size = if lg_cur_size > 0 { 1 << lg_cur_size } else { 0 };
        let entries = std::iter::repeat_with(|| None).take(size).collect();
        Self {
            lg_cur_size,
            lg_nom_size,
            lg_max_size,
            resize_factor,
            sampling_probability,
            hash_seed,
            is_empty,
            theta,
            entries,
            num_retained: 0,
        }
    }

    /// Hash a value with the table seed and return the hash.
    pub(crate) fn hash<T: Hash>(&self, value: T) -> u64 {
        let mut hasher = MurmurHash3X64128::with_seed(self.hash_seed);
        value.hash(&mut hasher);
        let (h1, _) = hasher.finish128();
        h1 >> 1 // To make it compatible with Java version
    }

    /// Inserts or updates the entry slot for a pre-hashed key.
    ///
    /// Returns true if a new entry was created, false otherwise (existing key, declined insertion,
    /// or a hash screened out by theta).
    pub(crate) fn upsert_entry<F>(&mut self, hash: u64, f: F) -> bool
    where
        F: FnOnce(Option<&mut E>) -> Option<E>,
    {
        self.is_empty = false;

        if hash == 0 || hash >= self.theta {
            return false;
        }

        let Some(index) = self.find_in_curr_entries(hash) else {
            unreachable!(
                "Resize or rebuild should be called to make sure it always can find the entry."
            );
        };

        if let Some(entry) = self.entries[index].as_mut() {
            f(Some(entry));
            return false;
        }

        let Some(entry) = f(None) else {
            return false;
        };
        debug_assert_eq!(entry.hash(), hash, "entry hash must match insertion hash");
        self.entries[index] = Some(entry);
        self.num_retained += 1;

        // Check if we need to resize or rebuild.
        let capacity = self.get_capacity();
        if self.num_retained > capacity {
            if self.lg_cur_size <= self.lg_nom_size {
                self.resize();
            } else {
                self.rebuild();
            }
        }
        true
    }

    /// Returns a reference to the entry stored for `hash`, or `None` if the hash is not retained.
    pub(crate) fn get_entry(&self, hash: u64) -> Option<&E> {
        if hash == 0 {
            return None;
        }
        let index = self.find_in_curr_entries(hash)?;
        match &self.entries[index] {
            Some(entry) if entry.hash() == hash => Some(entry),
            _ => None,
        }
    }

    /// Get capacity threshold.
    pub(crate) fn get_capacity(&self) -> usize {
        let fraction = if self.lg_cur_size <= self.lg_nom_size {
            HASH_TABLE_RESIZE_THRESHOLD
        } else {
            HASH_TABLE_REBUILD_THRESHOLD
        };
        (fraction * self.entries.len() as f64) as usize
    }

    /// Trim the table to nominal size k.
    pub(crate) fn trim(&mut self) {
        if self.num_retained > (1 << self.lg_nom_size) {
            self.rebuild();
        }
    }

    /// Reset the table to empty state.
    pub(crate) fn reset(&mut self) {
        let init_theta = starting_theta_from_sampling_probability(self.sampling_probability);
        let init_lg_cur = starting_sub_multiple(
            self.lg_nom_size + 1,
            MIN_LG_K,
            self.resize_factor.lg_value(),
        );

        let size = 1 << init_lg_cur;
        self.entries.clear();
        self.entries.resize_with(size, || None);
        self.num_retained = 0;
        self.theta = init_theta;
        self.is_empty = true;
        self.lg_cur_size = init_lg_cur;
    }

    /// Return number of retained entries.
    pub(crate) fn num_retained(&self) -> usize {
        self.num_retained
    }

    /// Get theta.
    pub(crate) fn theta(&self) -> u64 {
        self.theta
    }

    /// Check logical emptiness of the source set.
    pub(crate) fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// Get iterator over retained entries.
    pub(crate) fn iter_entries(&self) -> impl Iterator<Item = &E> + '_ {
        self.entries.iter().filter_map(Option::as_ref)
    }

    /// Get log2 of nominal size.
    pub(crate) fn lg_nom_size(&self) -> u8 {
        self.lg_nom_size
    }

    /// Get the hash of the seed that was used to hash the input.
    pub(crate) fn seed_hash(&self) -> u16 {
        compute_seed_hash(self.hash_seed)
    }

    /// Set empty flag.
    pub(crate) fn set_empty(&mut self, is_empty: bool) {
        self.is_empty = is_empty;
    }

    /// Get the hash seed used by this table.
    pub(crate) fn hash_seed(&self) -> u64 {
        self.hash_seed
    }

    /// Sets theta value.
    pub(crate) fn set_theta(&mut self, theta: u64) {
        assert!(
            (1..=MAX_THETA).contains(&theta),
            "theta must be in [1, {MAX_THETA}], got {theta}"
        );
        self.theta = theta;
    }

    /// Returns minimal lg_size where rebuild-capacity can hold `count`.
    pub(crate) fn lg_size_from_count_for_rebuild(count: usize, load_factor: f64) -> u8 {
        let log2 = |n: usize| {
            if n == 0 { 0_u8 } else { n.ilog2() as u8 }
        };
        let log2_n = log2(count);
        log2_n
            + (if count > (((1u128 << ((log2_n as u32) + 1)) as f64) * load_factor) as usize {
                2
            } else {
                1
            })
    }

    /// Returns the estimated size of the heap allocations in bytes.
    pub(crate) fn estimated_size(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<Option<E>>()
    }

    fn find_in_curr_entries(&self, key: u64) -> Option<usize> {
        Self::find_in_entries(&self.entries, key, self.lg_cur_size)
    }

    fn find_in_entries(entries: &[Option<E>], key: u64, lg_size: u8) -> Option<usize> {
        if entries.is_empty() {
            return None;
        }

        let size = entries.len();
        let mask = size - 1;
        let stride = Self::get_stride(key, lg_size);
        let mut index = (key as usize) & mask;
        let loop_index = index;

        loop {
            match &entries[index] {
                None => return Some(index),
                Some(entry) if entry.hash() == key => return Some(index),
                _ => {}
            }
            index = (index + stride) & mask;
            if index == loop_index {
                return None;
            }
        }
    }

    fn resize(&mut self) {
        let new_lg_size = std::cmp::min(
            self.lg_cur_size + self.resize_factor.lg_value(),
            self.lg_max_size,
        );
        let new_size = 1 << new_lg_size;

        let mut new_entries: Vec<Option<E>> =
            std::iter::repeat_with(|| None).take(new_size).collect();
        for entry in std::mem::take(&mut self.entries).into_iter().flatten() {
            let Some(idx) = Self::find_in_entries(&new_entries, entry.hash(), new_lg_size) else {
                unreachable!(
                    "find_in_entries should always return Some if the entry is not empty."
                );
            };
            new_entries[idx] = Some(entry);
        }

        self.entries = new_entries;
        self.lg_cur_size = new_lg_size;
    }

    fn rebuild(&mut self) {
        let k = 1usize << self.lg_nom_size;

        // Select the k-th smallest entry as new theta and keep the lesser entries.
        let mut retained: Vec<E> = std::mem::take(&mut self.entries)
            .into_iter()
            .flatten()
            .collect();
        let kth_hash = {
            let (_lesser, kth, _greater) = retained.select_nth_unstable_by_key(k, |e| e.hash());
            kth.hash()
        };
        self.theta = kth_hash;
        retained.truncate(k);

        let size = 1 << self.lg_cur_size;
        let mut new_entries: Vec<Option<E>> = std::iter::repeat_with(|| None).take(size).collect();
        let mut num_inserted = 0;
        for entry in retained {
            if let Some(idx) = Self::find_in_entries(&new_entries, entry.hash(), self.lg_cur_size) {
                new_entries[idx] = Some(entry);
                num_inserted += 1;
            } else {
                unreachable!(
                    "find_in_entries should always return Some if the entry is not empty."
                );
            }
        }

        assert_eq!(
            num_inserted, k,
            "Number of inserted entries should be equal to k."
        );
        self.num_retained = num_inserted;
        self.entries = new_entries;
    }

    fn get_stride(key: u64, lg_size: u8) -> usize {
        (2 * ((key >> (lg_size)) & STRIDE_MASK) + 1) as usize
    }
}

/// Compute initial lg_size for hash table based on target lg_size, minimum lg_size, and resize
/// factor. Make sure `lg_target = lg_init + n * lg_resize_factor`, where `n` is an integer and
/// `lg_init >= lg_min`.
pub(crate) fn starting_sub_multiple(lg_target: u8, lg_min: u8, lg_resize_factor: u8) -> u8 {
    if lg_target <= lg_min {
        lg_min
    } else if lg_resize_factor == 0 {
        lg_target
    } else {
        ((lg_target - lg_min) % lg_resize_factor) + lg_min
    }
}

/// Compute initial theta for hash table based on sampling probability.
pub(crate) fn starting_theta_from_sampling_probability(sampling_probability: f32) -> u64 {
    if sampling_probability < 1.0 {
        (MAX_THETA as f64 * sampling_probability as f64) as u64
    } else {
        MAX_THETA
    }
}
