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
use std::slice;

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::MurmurHash3X64128;
use crate::hash::compute_seed_hash;
use crate::thetacommon::SketchEntry;
use crate::thetacommon::constants::HASH_TABLE_REBUILD_THRESHOLD;
use crate::thetacommon::constants::HASH_TABLE_RESIZE_THRESHOLD;
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::constants::MIN_LG_K;
use crate::thetacommon::constants::STRIDE_MASK;
use crate::thetacommon::sketch_state::CompactSketchState;

pub struct SketchHashTableIter<'a, E>(slice::Iter<'a, Option<E>>);

impl<'a, E> Iterator for SketchHashTableIter<'a, E> {
    type Item = &'a E;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.find_map(Option::as_ref)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.0.size_hint().1)
    }
}

/// Generic hash-table mechanics shared by Theta and Tuple sketches.
///
/// The entry type supplies the retained hash and any sketch-specific payload. The table owns all
/// theta screening, probing, resizing, rebuilding, and trimming.
///
/// It maintains an array with capacity up to 2^lg_max_size:
/// * Before it reaches the max capacity, it will extend the array based on resize_factor.
/// * After it reaches the capacity bigger than 2^lg_nom_size, every time the number of entries
///   exceeds the threshold, it will rebuild the table: only keep the min 2^lg_nom_size entries and
///   update the theta to the k-th smallest entry.
#[derive(Debug)]
pub struct SketchHashTable<E> {
    lg_cur_size: u8,
    lg_nom_size: u8,
    lg_max_size: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    seed_hash: u16,

    // The operational threshold used to screen future updates. This is intentionally independent
    // of the sketch's externally visible empty state: a never-updated sketch built with p < 1.0
    // must retain this sampling threshold even though its public theta is MAX_THETA.
    retention_theta: u64,

    entries: Vec<Option<E>>,

    // Number of retained non-zero hashes currently stored in `entries`.
    num_retained: usize,
}

impl<E> SketchHashTable<E>
where
    E: SketchEntry,
{
    /// Creates a new hash table.
    ///
    /// # Errors
    ///
    /// Returns an error if `lg_nom_size` is outside `[5, 26]`, `sampling_probability` is outside
    /// `(0.0, 1.0]`, or the computed seed hash is zero.
    pub fn new(
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
    ) -> Result<Self, Error> {
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
        let lg_cur_size = starting_sub_multiple(lg_max_size, MIN_LG_K, resize_factor.lg_value());
        Ok(Self::allocate_empty(
            lg_cur_size,
            lg_nom_size,
            resize_factor,
            sampling_probability,
            starting_retention_theta(sampling_probability),
            seed,
            seed_hash,
        ))
    }

    /// Creates a table used internally by a Theta-family set operation.
    ///
    /// # Panics
    ///
    /// Panics if `lg_cur_size > lg_nom_size + 1`. (`lg_nom_size + 1 == lg_max_size`)
    pub fn for_set_operation(
        lg_cur_size: u8,
        lg_nom_size: u8,
        retention_theta: u64,
        seed: u64,
        seed_hash: u16,
    ) -> Self {
        Self::allocate_empty(
            lg_cur_size,
            lg_nom_size,
            ResizeFactor::X1,
            1.0,
            retention_theta,
            seed,
            seed_hash,
        )
    }

    fn allocate_empty(
        lg_cur_size: u8,
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        retention_theta: u64,
        seed: u64,
        seed_hash: u16,
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
            seed,
            seed_hash,
            retention_theta,
            entries,
            num_retained: 0,
        }
    }

    /// Hash a value with the table seed and return the hash.
    pub fn hash<T: Hash>(&self, value: T) -> u64 {
        let mut hasher = MurmurHash3X64128::with_seed(self.seed);
        value.hash(&mut hasher);
        let (h1, _) = hasher.finish128();
        h1 >> 1 // To make it compatible with Java version
    }

    /// Inserts or updates the entry slot for a pre-hashed key.
    ///
    /// The callback `f` is invoked with the current entry for `hash`:
    /// * `Some(existing)` if the key is already retained. The callback should modify it in place;
    ///   its return value is ignored.
    /// * `None` if the key is new. The callback returns `Some(entry)` to insert it, or `None` to
    ///   decline insertion.
    ///
    /// Using a single callback ensures any captured update value is consumed exactly once, so it
    /// works for both the update sketch (folding an update value) and set operations (merging an
    /// incoming entry) without requiring the value to be `Copy` or `Clone`.
    ///
    /// Returns true if a new entry was created, false otherwise (existing key, declined insertion,
    /// or a hash screened out by theta).
    pub fn upsert_entry<F>(&mut self, hash: u64, f: F) -> bool
    where
        F: FnOnce(Option<&mut E>) -> Option<E>,
    {
        if hash == 0 || hash >= self.retention_theta {
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
        let capacity_threshold = self.capacity_threshold();
        if self.num_retained > capacity_threshold {
            if self.lg_cur_size <= self.lg_nom_size {
                self.resize();
            } else {
                self.rebuild();
            }
        }
        true
    }

    /// Returns a reference to the entry stored for `hash`, or `None` if the hash is not retained.
    pub fn entry(&self, hash: u64) -> Option<&E> {
        if hash == 0 {
            return None;
        }
        let index = self.find_in_curr_entries(hash)?;
        match &self.entries[index] {
            Some(entry) if entry.hash() == hash => Some(entry),
            _ => None,
        }
    }

    /// Return the current resize or rebuild capacity threshold.
    pub fn capacity_threshold(&self) -> usize {
        let fraction = if self.lg_cur_size <= self.lg_nom_size {
            HASH_TABLE_RESIZE_THRESHOLD
        } else {
            HASH_TABLE_REBUILD_THRESHOLD
        };
        (fraction * self.entries.len() as f64) as usize
    }

    /// Trim the table to nominal size k.
    pub fn trim(&mut self) {
        if self.num_retained > (1 << self.lg_nom_size) {
            self.rebuild();
        }
    }

    /// Restores the table's initial capacity and retention threshold and removes all entries.
    pub fn reset(&mut self) {
        let initial_retention_theta = starting_retention_theta(self.sampling_probability);
        let init_lg_cur = starting_sub_multiple(
            self.lg_nom_size + 1,
            MIN_LG_K,
            self.resize_factor.lg_value(),
        );

        let size = 1 << init_lg_cur;
        self.entries.clear();
        self.entries.resize_with(size, || None);
        self.num_retained = 0;
        self.retention_theta = initial_retention_theta;
        self.lg_cur_size = init_lg_cur;
    }

    /// Return number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.num_retained
    }

    /// Returns the operational theta used to screen retained entries.
    pub fn retention_theta(&self) -> u64 {
        self.retention_theta
    }

    /// Get iterator over retained entries.
    pub fn iter_entries(&self) -> SketchHashTableIter<'_, E> {
        SketchHashTableIter(self.entries.iter())
    }

    /// Creates compact state for a sketch known by its owner to be non-empty.
    pub fn to_non_empty_compact_state(&self, ordered: bool) -> CompactSketchState<E>
    where
        E: Clone,
    {
        let mut retained_entries: Vec<E> = self.iter_entries().cloned().collect();
        let ordered = ordered || (retained_entries.len() == 1 && self.retention_theta == MAX_THETA);
        if ordered && retained_entries.len() > 1 {
            retained_entries.sort_unstable_by_key(SketchEntry::hash);
        }
        CompactSketchState::non_empty(
            retained_entries,
            self.retention_theta,
            self.seed_hash,
            ordered,
        )
    }

    /// Get log2 of nominal size.
    pub fn lg_nom_size(&self) -> u8 {
        self.lg_nom_size
    }

    /// Get the hash of the seed that was used to hash the input.
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    /// Get the seed used by this table.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Sets the operational theta used to screen retained entries.
    pub fn set_retention_theta(&mut self, retention_theta: u64) {
        assert!(
            (1..=MAX_THETA).contains(&retention_theta),
            "theta must be in [1, {MAX_THETA}], got {retention_theta}"
        );
        self.retention_theta = retention_theta;
    }

    /// Returns minimal lg_size where rebuild-capacity can hold `count`.
    pub fn lg_size_from_count_for_rebuild(count: usize, load_factor: f64) -> u8 {
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

    /// Returns the retained capacity of the entry allocation in bytes.
    ///
    /// The estimate includes the inline representation of `E` in every allocated slot but does not
    /// inspect heap allocations owned by active entries.
    pub fn estimated_size(&self) -> usize {
        self.entries.capacity() * size_of::<Option<E>>()
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
        self.retention_theta = kth_hash;
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
pub fn starting_sub_multiple(lg_target: u8, lg_min: u8, lg_resize_factor: u8) -> u8 {
    if lg_target <= lg_min {
        lg_min
    } else if lg_resize_factor == 0 {
        lg_target
    } else {
        ((lg_target - lg_min) % lg_resize_factor) + lg_min
    }
}

/// Computes the initial operational theta from a sampling probability.
pub fn starting_retention_theta(sampling_probability: f32) -> u64 {
    if sampling_probability < 1.0 {
        (MAX_THETA as f64 * sampling_probability as f64) as u64
    } else {
        MAX_THETA
    }
}
