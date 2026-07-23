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
use std::num::NonZeroU64;

use crate::thetacommon::RawHashTableEntry;
use crate::thetacommon::hash_table::RawHashTable;

/// A retained entry in a Tuple sketch: a hash key together with its associated summary.
///
/// The hash is stored as [`NonZeroU64`] (hash 0 is screened out before insertion), so
/// `Option<TupleEntry<S>>` keeps the niche and takes no more space than `TupleEntry<S>` itself —
/// the same layout the Theta table gets from its `NonZeroU64` entry.
#[derive(Debug, Clone)]
pub struct TupleEntry<S> {
    hash: NonZeroU64,
    summary: S,
}

impl<S> TupleEntry<S> {
    /// Creates an entry from a hash known to be non-zero.
    ///
    /// # Panics
    ///
    /// Panics if `hash` is zero.
    pub(crate) fn new(hash: u64, summary: S) -> Self {
        let hash = NonZeroU64::new(hash).expect("hash must be non-zero");
        Self { hash, summary }
    }

    /// Return the hash used as this entry's key.
    pub fn hash(&self) -> u64 {
        self.hash.get()
    }

    /// Returns the summary stored in this entry.
    pub fn summary(&self) -> &S {
        &self.summary
    }
}

/// Specific hash table for tuple sketch.
///
/// This is the Theta sketch hash table extended so that each retained key carries a user-defined
/// summary. Unlike the Theta hash table, when a key is inserted that already exists, the incoming
/// update is merged into the existing summary rather than discarded.
pub(super) type TupleHashTable<S> = RawHashTable<TupleEntry<S>>;

impl<S> RawHashTableEntry for TupleEntry<S> {
    fn hash(&self) -> u64 {
        self.hash.get()
    }
}

impl<S> TupleHashTable<S> {
    /// Hashes a key and inserts or updates its summary via a single callback.
    ///
    /// See [`try_insert_hash`](Self::try_insert_hash) for the callback contract. Returns true if a
    /// new entry was created, false if the key already existed or the hash was screened out by
    /// theta.
    pub fn try_insert<T, F>(&mut self, key: T, f: F) -> bool
    where
        T: Hash,
        F: FnOnce(Option<&mut S>) -> Option<S>,
    {
        let hash = self.hash(key);
        self.try_insert_hash(hash, f)
    }

    /// Inserts or updates the summary slot for a pre-hashed key.
    ///
    /// Returns true if a new entry was created, false otherwise (existing key, declined insertion,
    /// or a hash screened out by theta).
    pub fn try_insert_hash<F>(&mut self, hash: u64, f: F) -> bool
    where
        F: FnOnce(Option<&mut S>) -> Option<S>,
    {
        self.upsert_entry(hash, |existing| match existing {
            Some(entry) => {
                f(Some(&mut entry.summary));
                None
            }
            None => f(None).map(|summary| TupleEntry::new(hash, summary)),
        })
    }

    /// Get iterator over retained entries as `(hash, &summary)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &S)> + '_ {
        self.iter_entries()
            .map(|entry| (entry.hash.get(), &entry.summary))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ResizeFactor;
    use crate::hash::DEFAULT_UPDATE_SEED;
    use crate::thetacommon::constants::MAX_THETA;
    use crate::thetacommon::constants::MIN_LG_K;
    use crate::thetacommon::hash_table::starting_sub_multiple;
    use crate::thetacommon::hash_table::starting_theta_from_sampling_probability;

    impl TupleHashTable<u64> {
        /// Inserts a key with count-style summary semantics: a new key starts at 1, a repeated key
        /// increments the retained count. Returns true if a new entry was created.
        fn insert(&mut self, value: impl Hash) -> bool {
            self.try_insert(value, |existing| match existing {
                Some(count) => {
                    *count += 1;
                    None
                }
                None => Some(1),
            })
        }

        /// Returns the retained `(hash, count)` pairs.
        fn pairs(&self) -> Vec<(u64, u64)> {
            self.iter().map(|(hash, &count)| (hash, count)).collect()
        }
    }

    #[test]
    fn option_entry_keeps_nonzero_niche() {
        // The NonZeroU64 hash gives Option<TupleEntry<S>> a niche, so table slots carry no
        // discriminant overhead on top of the entry itself.
        assert_eq!(
            size_of::<Option<TupleEntry<u64>>>(),
            size_of::<TupleEntry<u64>>()
        );
    }

    #[test]
    fn test_new_hash_table() {
        let table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert_eq!(
            table.lg_cur_size(),
            starting_sub_multiple(8 + 1, MIN_LG_K, ResizeFactor::X8.lg_value())
        );
        assert_eq!(table.theta(), starting_theta_from_sampling_probability(1.0));
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
        table.set_theta(1);
        assert!(!table.insert("test3"));
    }

    #[test]
    fn test_insert() {
        let mut table = TupleHashTable::<u64>::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert!(table.insert("test_value"));
        assert_eq!(table.num_retained(), 1);
        assert!(!table.is_empty());

        // Insert the same value again: not a new entry, but the summary is merged.
        assert!(!table.insert("test_value"));
        assert_eq!(table.num_retained(), 1);
        assert_eq!(table.pairs(), vec![(table.hash("test_value"), 2)]);

        // Force screening and verify insertion fails
        table.set_theta(1);
        assert!(!table.insert("screened"));
        assert_eq!(table.num_retained(), 1);
        assert!(!table.is_empty());
    }

    #[test]
    fn test_insert_multiple_values() {
        let mut table = TupleHashTable::<u64>::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        let mut inserted_count = 0;
        for i in 0..10 {
            if table.insert(format!("value_{}", i)) {
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
            table.insert("same_key");
        }

        assert_eq!(table.num_retained(), 1);
        assert_eq!(table.pairs(), vec![(table.hash("same_key"), 5)]);
    }

    #[test]
    fn test_resize() {
        fn populate_values(table: &mut TupleHashTable<u64>, count: usize) -> usize {
            let mut inserted = 0;
            for i in 0..count {
                if table.insert(format!("value_{i}")) {
                    inserted += 1;
                }
            }
            inserted
        }

        {
            let mut table =
                TupleHashTable::<u64>::new(8, ResizeFactor::X2, 1.0, DEFAULT_UPDATE_SEED);

            assert_eq!(table.num_entries(), 32);

            // Insert enough values to trigger resize (50% threshold)
            // Capacity = 32 * 0.5 = 16
            let inserted = populate_values(&mut table, 20);

            assert!(table.num_retained() > 0);
            assert_eq!(table.num_retained(), inserted);
            assert_eq!(table.num_entries(), 64);
        }

        {
            let mut table =
                TupleHashTable::<u64>::new(8, ResizeFactor::X4, 1.0, DEFAULT_UPDATE_SEED);

            assert_eq!(table.num_entries(), 32);

            let inserted = populate_values(&mut table, 20);

            assert!(table.num_retained() > 0);
            assert_eq!(table.num_retained(), inserted);
            assert_eq!(table.num_entries(), 128);
        }
    }

    #[test]
    fn test_rebuild() {
        let mut table = TupleHashTable::<u64>::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert_eq!(table.lg_cur_size(), 6);
        assert_eq!(table.num_entries(), 64);
        assert_eq!(table.theta(), MAX_THETA);

        // Insert many values to trigger rebuild
        for i in 0..100 {
            table.insert(format!("value_{i}"));
        }

        let new_theta = table.theta();
        assert!(
            new_theta < MAX_THETA,
            "Theta should be reduced after rebuild"
        );

        // Continue to insert values to trigger rebuild again
        for i in 100..200 {
            table.insert(format!("value_{i}"));
        }

        assert_eq!(table.lg_cur_size(), 6);
        assert!(table.num_entries() >= 64);
        assert!(table.theta() < new_theta);
    }

    #[test]
    fn test_trim() {
        let mut table = TupleHashTable::<u64>::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        for i in 0..100 {
            table.insert(format!("value_{i}"));
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
            table.insert(format!("value_{i}"));
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
        let init_lg_cur = table.lg_cur_size();
        let init_entries = table.num_entries();

        for i in 0..10 {
            table.insert(format!("value_{i}"));
        }

        assert!(!table.is_empty());
        assert!(table.num_retained() > 0);

        table.reset();

        assert!(table.is_empty());
        assert_eq!(table.num_retained(), 0);
        assert_eq!(table.theta(), init_theta);
        assert_eq!(table.lg_cur_size(), init_lg_cur);
        assert_eq!(table.num_entries(), init_entries);
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
            table.insert(format!("value_{i}"));
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
            if table.insert(i) {
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
            if table.insert(i - 1) {
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
            if table.insert(i - 1) {
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
            if table.insert(i - 1) {
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
