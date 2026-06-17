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
use crate::theta::starting_sub_multiple;
use crate::theta::starting_theta_from_sampling_probability;

/// A retained entry: a hash key together with its associated summary.
#[derive(Debug)]
struct TupleEntry<S> {
    hash: u64,
    summary: S,
}

/// Specific hash table for tuple sketch.
///
/// This is the Theta sketch hash table extended so that each retained key carries a user-defined
/// summary. It maintains an array with capacity up to 2^lg_max_size:
/// * Before it reaches the max capacity, it will extend the array based on resize_factor.
/// * After it reaches the capacity bigger than 2^lg_nom_size, every time the number of entries
///   exceeds the threshold, it will rebuild the table: only keep the min 2^lg_nom_size entries and
///   update the theta to the k-th smallest entry.
///
/// Unlike the Theta hash table, when a key is inserted that already exists, the incoming update is
/// merged into the existing summary rather than discarded.
#[derive(Debug)]
pub(super) struct TupleHashTable<S> {
    lg_cur_size: u8,
    lg_nom_size: u8,
    lg_max_size: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    hash_seed: u64,

    // Logical emptiness of the source set.
    //
    // * `false` if any update has been attempted (even if screened by theta)
    // * `true` if no updates have been attempted.
    //
    // This can be false even when `num_retained` is 0.
    is_empty: bool,

    theta: u64,

    // Using `None` to represent zero value.
    entries: Vec<Option<TupleEntry<S>>>,

    // Number of retained non-zero hashes currently stored in `entries`.
    num_retained: usize,
}

impl<S> TupleHashTable<S> {
    /// Create a new hash table
    pub fn new(
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
    pub fn from_raw_parts(
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
    fn hash<T: Hash>(&self, value: T) -> u64 {
        let mut hasher = MurmurHash3X64128::with_seed(self.hash_seed);
        value.hash(&mut hasher);
        let (h1, _) = hasher.finish128();
        h1 >> 1 // To make it compatible with Java version
    }

    /// Find an entry in the hash table.
    ///
    /// Returns the index of the entry if found, otherwise None. The entry may have been inserted or
    /// empty.
    fn find_in_curr_entries(&self, key: u64) -> Option<usize> {
        Self::find_in_entries(&self.entries, key, self.lg_cur_size)
    }

    /// Find index in a given entries.
    ///
    /// Returns the index of the entry if found, otherwise None. The entry may have been inserted or
    /// empty.
    fn find_in_entries(entries: &[Option<TupleEntry<S>>], key: u64, lg_size: u8) -> Option<usize> {
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
                Some(entry) if entry.hash == key => return Some(index),
                _ => {}
            }
            index = (index + stride) & mask;
            if index == loop_index {
                return None;
            }
        }
    }

    /// Hashes a key and inserts or updates its summary via a single callback.
    ///
    /// See [`upsert`](Self::upsert) for the callback contract. Returns true if a new entry was
    /// created, false if the key already existed or the hash was screened out by theta.
    pub fn update<T, F>(&mut self, key: T, f: F) -> bool
    where
        T: Hash,
        F: FnOnce(Option<&mut S>) -> Option<S>,
    {
        let hash = self.hash(key);
        self.upsert(hash, f)
    }

    /// Inserts or updates the summary slot for a pre-hashed key.
    ///
    /// The callback `f` is invoked with the current summary for `hash`:
    /// * `Some(existing)` if the key is already retained. The callback should modify it in place;
    ///   its return value is ignored.
    /// * `None` if the key is new. The callback returns `Some(summary)` to insert it, or `None` to
    ///   decline insertion.
    ///
    /// Using a single callback ensures any captured update value is consumed exactly once, so it
    /// works for both the update sketch (folding an update value) and set operations (merging an
    /// incoming summary) without requiring the value to be `Copy` or `Clone`.
    ///
    /// Returns true if a new entry was created, false otherwise (existing key, declined insertion,
    /// or a hash screened out by theta).
    pub fn upsert<F>(&mut self, hash: u64, f: F) -> bool
    where
        F: FnOnce(Option<&mut S>) -> Option<S>,
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

        // Already exists: let the callback merge into the retained summary in place.
        if let Some(entry) = self.entries[index].as_mut() {
            f(Some(&mut entry.summary));
            return false;
        }

        // New key: the callback may decline by returning None.
        let Some(summary) = f(None) else {
            return false;
        };
        self.entries[index] = Some(TupleEntry { hash, summary });
        self.num_retained += 1;

        // Check if we need to resize or rebuild
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

    /// Get capacity threshold
    fn get_capacity(&self) -> usize {
        let fraction = if self.lg_cur_size <= self.lg_nom_size {
            HASH_TABLE_RESIZE_THRESHOLD
        } else {
            HASH_TABLE_REBUILD_THRESHOLD
        };
        (fraction * self.entries.len() as f64) as usize
    }

    /// Resize the hash table
    fn resize(&mut self) {
        let new_lg_size = std::cmp::min(
            self.lg_cur_size + self.resize_factor.lg_value(),
            self.lg_max_size,
        );
        let new_size = 1 << new_lg_size;

        // Get new entries and rehash all entries
        let mut new_entries: Vec<Option<TupleEntry<S>>> =
            std::iter::repeat_with(|| None).take(new_size).collect();
        for entry in std::mem::take(&mut self.entries).into_iter().flatten() {
            let Some(idx) = Self::find_in_entries(&new_entries, entry.hash, new_lg_size) else {
                unreachable!(
                    "find_in_entries should always return Some if the entry is not empty."
                );
            };
            new_entries[idx] = Some(entry);
        }

        self.entries = new_entries;
        self.lg_cur_size = new_lg_size;
    }

    /// Rebuild the hash table:
    /// The number of entries will be reduced to the nominal size k.
    fn rebuild(&mut self) {
        let k = 1usize << self.lg_nom_size;

        // Select the k-th smallest entry as new theta and keep the lesser entries.
        let mut retained: Vec<TupleEntry<S>> = std::mem::take(&mut self.entries)
            .into_iter()
            .flatten()
            .collect();
        let kth_hash = {
            let (_lesser, kth, _greater) = retained.select_nth_unstable_by_key(k, |e| e.hash);
            kth.hash
        };
        self.theta = kth_hash;
        retained.truncate(k);

        // Rebuild the table with the lesser entries.
        let size = 1 << self.lg_cur_size;
        let mut new_entries: Vec<Option<TupleEntry<S>>> =
            std::iter::repeat_with(|| None).take(size).collect();
        let mut num_inserted = 0;
        for entry in retained {
            if let Some(idx) = Self::find_in_entries(&new_entries, entry.hash, self.lg_cur_size) {
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

    /// Trim the table to nominal size k
    pub fn trim(&mut self) {
        if self.num_retained > (1 << self.lg_nom_size) {
            self.rebuild();
        }
    }

    /// Reset the table to empty state
    pub fn reset(&mut self) {
        let init_theta = starting_theta_from_sampling_probability(self.sampling_probability);
        let init_lg_cur = starting_sub_multiple(
            self.lg_nom_size + 1,
            MIN_LG_K,
            self.resize_factor.lg_value(),
        );

        // clear entries
        let size = 1 << init_lg_cur;
        self.entries.clear();
        self.entries.resize_with(size, || None);
        self.num_retained = 0;
        self.theta = init_theta;
        self.is_empty = true;
        self.lg_cur_size = init_lg_cur;
    }

    /// Return number of retained entries
    pub fn num_retained(&self) -> usize {
        self.num_retained
    }

    /// Get theta
    pub fn theta(&self) -> u64 {
        self.theta
    }

    /// Check if emptiness of the source set
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// Get iterator over retained entries as `(hash, &summary)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &S)> + '_ {
        self.entries
            .iter()
            .filter_map(|slot| slot.as_ref().map(|entry| (entry.hash, &entry.summary)))
    }

    /// Get log2 of nominal size
    pub fn lg_nom_size(&self) -> u8 {
        self.lg_nom_size
    }

    /// Get the hash of the seed that was used to hash the input.
    pub fn seed_hash(&self) -> u16 {
        compute_seed_hash(self.hash_seed)
    }

    /// Returns a reference to the summary stored for `hash`, or `None` if the hash is not retained.
    pub fn get(&self, hash: u64) -> Option<&S> {
        if hash == 0 {
            return None;
        }
        let index = self.find_in_curr_entries(hash)?;
        match &self.entries[index] {
            Some(entry) if entry.hash == hash => Some(&entry.summary),
            _ => None,
        }
    }

    /// Inserts a `(hash, summary)` pair, taking ownership of `summary`.
    ///
    /// Returns true if a new entry was created. Returns false (dropping `summary`) if the hash is
    /// already retained or is screened out by theta. This is the summary-carrying analogue of the
    /// Theta hash table's `try_insert_hash`.
    pub fn try_insert(&mut self, hash: u64, summary: S) -> bool {
        self.upsert(hash, |existing| match existing {
            Some(_) => None,
            None => Some(summary),
        })
    }

    /// Set empty flag
    pub fn set_empty(&mut self, is_empty: bool) {
        self.is_empty = is_empty;
    }

    /// Get the hash seed used by this table.
    pub fn hash_seed(&self) -> u64 {
        self.hash_seed
    }

    /// Sets theta value.
    pub fn set_theta(&mut self, theta: u64) {
        assert!(
            (1..=MAX_THETA).contains(&theta),
            "theta must be in [1, {MAX_THETA}], got {theta}"
        );
        self.theta = theta;
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

    /// Get stride for hash table probing
    fn get_stride(key: u64, lg_size: u8) -> usize {
        (2 * ((key >> (lg_size)) & STRIDE_MASK) + 1) as usize
    }

    /// Returns the estimated size of the heap allocations in bytes
    pub fn estimated_size(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<Option<TupleEntry<S>>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::DEFAULT_UPDATE_SEED;

    /// Inserts a key with count-style summary semantics: a new key starts at 1, a repeated key
    /// increments the retained count. Returns true if a new entry was created.
    fn insert(table: &mut TupleHashTable<u64>, value: impl Hash) -> bool {
        table.update(value, |existing| match existing {
            Some(count) => {
                *count += 1;
                None
            }
            None => Some(1),
        })
    }

    /// Collect retained `(hash, count)` pairs.
    fn collect(table: &TupleHashTable<u64>) -> Vec<(u64, u64)> {
        table.iter().map(|(hash, &count)| (hash, count)).collect()
    }

    #[test]
    fn test_new_hash_table() {
        let table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert_eq!(
            table.lg_cur_size,
            starting_sub_multiple(8 + 1, MIN_LG_K, ResizeFactor::X8.lg_value())
        );
        assert_eq!(table.theta, starting_theta_from_sampling_probability(1.0));
        assert_eq!(table.num_retained(), 0);
        assert!(table.is_empty());
        assert_eq!(table.iter().count(), 0);
    }

    #[test]
    fn test_hash_and_theta_screen_behavior() {
        let mut table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        // With MAX_THETA, hashes are computed normally.
        let hash1 = table.hash("test1");
        let hash2 = table.hash("test2");
        assert_ne!(hash1, 0);
        assert_ne!(hash2, 0);
        assert_ne!(hash1, hash2);

        // With low theta, update should be screened out.
        table.theta = 1;
        assert!(!insert(&mut table, "test3"));
    }

    #[test]
    fn test_insert() {
        let mut table = TupleHashTable::<u64>::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert!(insert(&mut table, "test_value"));
        assert_eq!(table.num_retained(), 1);
        assert!(!table.is_empty());

        // Insert the same value again: not a new entry, but the summary is merged.
        assert!(!insert(&mut table, "test_value"));
        assert_eq!(table.num_retained(), 1);
        assert_eq!(collect(&table), vec![(table.hash("test_value"), 2)]);

        // Force screening and verify insertion fails
        table.theta = 0;
        assert!(!insert(&mut table, "screened"));
        assert_eq!(table.num_retained(), 1);
        assert!(!table.is_empty());
    }

    #[test]
    fn test_insert_multiple_values() {
        let mut table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        let mut inserted_count = 0;
        for i in 0..10 {
            if insert(&mut table, format!("value_{}", i)) {
                inserted_count += 1;
            }
        }

        assert_eq!(table.num_retained(), inserted_count);
        assert!(!table.is_empty());
        assert_eq!(table.iter().count(), inserted_count);
    }

    #[test]
    fn test_summary_is_merged_on_collision() {
        let mut table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        for _ in 0..5 {
            insert(&mut table, "same_key");
        }

        assert_eq!(table.num_retained(), 1);
        assert_eq!(collect(&table), vec![(table.hash("same_key"), 5)]);
    }

    #[test]
    fn test_resize() {
        fn populate_values(table: &mut TupleHashTable<u64>, count: usize) -> usize {
            let mut inserted = 0;
            for i in 0..count {
                if insert(table, format!("value_{}", i)) {
                    inserted += 1;
                }
            }
            inserted
        }

        {
            let mut table =
                TupleHashTable::<u64>::new(8, ResizeFactor::X2, 1.0, DEFAULT_UPDATE_SEED);

            assert_eq!(table.entries.len(), 32);

            // Insert enough values to trigger resize (50% threshold)
            // Capacity = 32 * 0.5 = 16
            let inserted = populate_values(&mut table, 20);

            assert!(table.num_retained() > 0);
            assert_eq!(table.num_retained(), inserted);
            assert_eq!(table.entries.len(), 64);
        }

        {
            let mut table =
                TupleHashTable::<u64>::new(8, ResizeFactor::X4, 1.0, DEFAULT_UPDATE_SEED);

            assert_eq!(table.entries.len(), 32);

            let inserted = populate_values(&mut table, 20);

            assert!(table.num_retained() > 0);
            assert_eq!(table.num_retained(), inserted);
            assert_eq!(table.entries.len(), 128);
        }
    }

    #[test]
    fn test_rebuild() {
        let mut table = TupleHashTable::<u64>::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert_eq!(table.lg_cur_size, 6);
        assert_eq!(table.entries.len(), 64);
        assert_eq!(table.theta, MAX_THETA);

        // Insert many values to trigger rebuild
        for i in 0..100 {
            insert(&mut table, format!("value_{}", i));
        }

        let new_theta = table.theta();
        assert!(
            new_theta < MAX_THETA,
            "Theta should be reduced after rebuild"
        );

        // Continue to insert values to trigger rebuild again
        for i in 100..200 {
            insert(&mut table, format!("value_{}", i));
        }

        assert_eq!(table.lg_cur_size, 6);
        assert!(table.entries.len() >= 64);
        assert!(table.theta < new_theta);
    }

    #[test]
    fn test_trim() {
        let mut table = TupleHashTable::<u64>::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        for i in 0..100 {
            insert(&mut table, format!("value_{}", i));
        }

        let before_trim = table.num_retained();
        assert!(before_trim > 32);

        table.trim();
        let after_trim = table.num_retained();
        assert!(after_trim <= 32);
        assert!(table.theta() < MAX_THETA);
    }

    #[test]
    fn test_trim_when_not_needed() {
        let mut table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        for i in 0..10 {
            insert(&mut table, format!("value_{}", i));
        }

        let before_trim = table.num_retained();
        let before_theta = table.theta();
        table.trim();
        let after_trim = table.num_retained();

        assert_eq!(before_trim, after_trim);
        assert_eq!(before_theta, table.theta());
    }

    #[test]
    fn test_reset() {
        let mut table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);
        let init_theta = table.theta();
        let init_lg_cur = table.lg_cur_size;
        let init_entries = table.entries.len();

        for i in 0..10 {
            insert(&mut table, format!("value_{}", i));
        }

        assert!(!table.is_empty());
        assert!(table.num_retained() > 0);

        table.reset();

        assert!(table.is_empty());
        assert_eq!(table.num_retained(), 0);
        assert_eq!(table.theta(), init_theta);
        assert_eq!(table.lg_cur_size, init_lg_cur);
        assert_eq!(table.entries.len(), init_entries);
        assert_eq!(table.iter().count(), 0);
    }

    #[test]
    fn test_table_with_sampling() {
        let mut table = TupleHashTable::<u64>::new(
            8,
            ResizeFactor::X8,
            0.5, // sampling_probability = 0.5
            DEFAULT_UPDATE_SEED,
        );
        assert_eq!(table.theta(), (MAX_THETA as f64 * 0.5) as u64);

        for i in 0..10 {
            insert(&mut table, format!("value_{}", i));
        }

        table.reset();

        assert_eq!(table.theta(), (MAX_THETA as f64 * 0.5) as u64);
        assert!(table.is_empty());
    }

    #[test]
    fn test_iterator() {
        let mut table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        let mut inserted_hashes = vec![];
        for i in 0..10 {
            let hash = table.hash(i);
            if insert(&mut table, i) {
                inserted_hashes.push(hash);
            }
        }

        let iter_hashes: Vec<u64> = table.iter().map(|(hash, _)| hash).collect();
        assert_eq!(iter_hashes.len(), table.num_retained());
        assert_eq!(iter_hashes.len(), inserted_hashes.len());

        for hash in &inserted_hashes {
            assert!(iter_hashes.contains(hash));
        }

        assert!(!iter_hashes.contains(&0));
    }

    #[test]
    fn test_empty_table_operations() {
        let mut table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert!(table.is_empty());
        assert_eq!(table.num_retained(), 0);
        assert_eq!(table.iter().count(), 0);

        // Trim on empty table should not panic
        table.trim();
        assert!(table.is_empty());

        // Reset on empty table should not panic
        table.reset();
        assert!(table.is_empty());
    }

    #[test]
    fn test_rebuild_preserves_entries_less_than_kth() {
        let mut table = TupleHashTable::<u64>::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);
        let k = 1u64 << 5; // k = 32

        // Insert many values to trigger rebuild
        let mut i = 0;
        let mut inserted_hashes = vec![];
        loop {
            let hash = table.hash(i);
            i += 1;
            if insert(&mut table, i - 1) {
                inserted_hashes.push(hash);
            }
            if table.num_retained() >= k as usize {
                break;
            }
        }

        let rebuild_threshold = table.get_capacity();

        loop {
            let hash = table.hash(i);
            i += 1;
            if insert(&mut table, i - 1) {
                inserted_hashes.push(hash);
            }
            if table.num_retained() >= rebuild_threshold {
                break;
            }
        }

        // trigger rebuild
        loop {
            let hash = table.hash(i);
            i += 1;
            if insert(&mut table, i - 1) {
                inserted_hashes.push(hash);
                break;
            }
        }

        // assert all entries are less than kth
        inserted_hashes.sort();
        let kth = inserted_hashes[k as usize];
        assert!(table.iter().all(|(hash, _)| hash < kth));
        assert_eq!(table.theta(), kth);
    }
}
