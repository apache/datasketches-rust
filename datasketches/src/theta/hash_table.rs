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

use super::raw_hash_table::RawHashTable;
use super::raw_hash_table::RawHashTableEntry;
#[cfg(test)]
pub(crate) use super::raw_hash_table::starting_sub_multiple;
#[cfg(test)]
pub(crate) use super::raw_hash_table::starting_theta_from_sampling_probability;

/// Specific hash table for theta sketch
///
/// It maintains an array capacity max to 2^lg_max_size:
/// * Before it reaches the max capacity, it will extend the array based on resize_factor.
/// * After it reaches the capacity bigger than 2^lg_nom_size, every time the number of entries
///   exceeds the threshold, it will rebuild the table: only keep the min 2^lg_nom_size entries and
///   update the theta to the k-th smallest entry.
pub(super) type ThetaHashTable = RawHashTable<ThetaEntry>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ThetaEntry {
    hash: NonZeroU64,
}

impl ThetaEntry {
    fn new(hash: u64) -> Self {
        let hash = NonZeroU64::new(hash).expect("hash must be non-zero");
        Self { hash }
    }
}

impl RawHashTableEntry for ThetaEntry {
    fn hash(&self) -> u64 {
        self.hash.get()
    }
}

impl ThetaHashTable {
    /// Hashes and inserts a value into the table.
    ///
    /// Returns true if the value was inserted (new), false otherwise.
    pub fn try_insert<T: Hash>(&mut self, value: T) -> bool {
        let hash = self.hash(value);
        self.try_insert_hash(hash)
    }

    /// Inserts a pre-hashed value into the table.
    ///
    /// Returns true if the value was inserted (new), false otherwise.
    pub fn try_insert_hash(&mut self, hash: u64) -> bool {
        self.upsert_entry(hash, |existing| {
            if existing.is_some() {
                None
            } else {
                Some(ThetaEntry::new(hash))
            }
        })
    }

    /// Get iterator over entries.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.iter_entries().map(RawHashTableEntry::hash)
    }

    /// Returns true if the given hash exists in the table.
    pub fn contains_hash(&self, hash: u64) -> bool {
        self.get_entry(hash).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ResizeFactor;
    use crate::hash::DEFAULT_UPDATE_SEED;
    use crate::theta::MAX_THETA;
    use crate::theta::MIN_LG_K;

    #[test]
    fn test_new_hash_table() {
        let table = ThetaHashTable::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

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
        let mut table = ThetaHashTable::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        // With MAX_THETA, hashes are computed normally.
        let hash1 = table.hash("test1");
        let hash2 = table.hash("test2");
        assert_ne!(hash1, 0);
        assert_ne!(hash2, 0);
        assert_ne!(hash1, hash2);

        // With low theta, update should be screened out.
        table.theta = 1;
        assert!(!table.try_insert("test3"));
    }

    #[test]
    fn test_try_insert() {
        let mut table = ThetaHashTable::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert!(table.try_insert("test_value"));
        assert_eq!(table.num_retained(), 1);
        assert!(!table.is_empty());

        // Try to insert the same value again (should fail)
        assert!(!table.try_insert("test_value"));
        assert_eq!(table.num_retained(), 1);

        // Force screening and verify insertion fails
        table.theta = 0;
        assert!(!table.try_insert("screened"));
        assert_eq!(table.num_retained(), 1);
        assert!(!table.is_empty());
    }

    #[test]
    fn test_insert_multiple_values() {
        let mut table = ThetaHashTable::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        // Insert multiple distinct values
        let mut inserted_count = 0;
        for i in 0..10 {
            if table.try_insert(format!("value_{}", i)) {
                inserted_count += 1;
            }
        }

        assert_eq!(table.num_retained(), inserted_count);
        assert!(!table.is_empty());
        assert_eq!(table.iter().count(), inserted_count);
    }

    #[test]
    fn test_resize() {
        fn populate_values(table: &mut ThetaHashTable, count: usize) -> usize {
            let mut inserted = 0;
            for i in 0..count {
                if table.try_insert(format!("value_{}", i)) {
                    inserted += 1;
                }
            }
            inserted
        }

        {
            let mut table = ThetaHashTable::new(8, ResizeFactor::X2, 1.0, DEFAULT_UPDATE_SEED);

            assert_eq!(table.entries.len(), 32);

            // Insert enough values to trigger resize (50% threshold)
            // Capacity = 32 * 0.5 = 16
            let inserted = populate_values(&mut table, 20);

            // Table should have resized and all values should be inserted
            assert!(table.num_retained() > 0);
            assert_eq!(table.num_retained(), inserted);
            assert_eq!(table.entries.len(), 64);
        }

        // Test different resize factors
        {
            let mut table = ThetaHashTable::new(8, ResizeFactor::X4, 1.0, DEFAULT_UPDATE_SEED);

            assert_eq!(table.entries.len(), 32);

            // Insert enough values to trigger resize (50% threshold)
            // Capacity = 32 * 0.5 = 16
            let inserted = populate_values(&mut table, 20);

            // Table should have resized and all values should be inserted
            assert!(table.num_retained() > 0);
            assert_eq!(table.num_retained(), inserted);
            assert_eq!(table.entries.len(), 128);
        }
    }

    #[test]
    fn test_rebuild() {
        let mut table = ThetaHashTable::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        assert_eq!(table.lg_cur_size, 6);
        assert_eq!(table.entries.len(), 64);
        assert_eq!(table.theta, MAX_THETA);

        // Insert many values to trigger rebuild
        for i in 0..100 {
            let _ = table.try_insert(format!("value_{}", i));
        }

        // After rebuild, theta should be reduced (rebuild is called automatically during insert)
        let new_theta = table.theta();
        assert!(
            new_theta < MAX_THETA,
            "Theta should be reduced after rebuild"
        );

        // Continue to insert values to trigger rebuild again
        for i in 100..200 {
            let _ = table.try_insert(format!("value_{}", i));
        }

        assert_eq!(table.lg_cur_size, 6);
        assert!(table.entries.len() >= 64);
        assert!(table.theta < new_theta);
    }

    #[test]
    fn test_trim() {
        let mut table = ThetaHashTable::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        // Insert more than k values
        for i in 0..100 {
            let _ = table.try_insert(format!("value_{}", i));
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
        let mut table = ThetaHashTable::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        // Insert fewer than k values
        for i in 0..10 {
            let _ = table.try_insert(format!("value_{}", i));
        }

        let before_trim = table.num_retained();
        let before_theta = table.theta();
        table.trim();
        let after_trim = table.num_retained();

        // Should not change if already <= k
        assert_eq!(before_trim, after_trim);
        assert_eq!(before_theta, table.theta());
    }

    #[test]
    fn test_reset() {
        let mut table = ThetaHashTable::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);
        let init_theta = table.theta();
        let init_lg_cur = table.lg_cur_size;
        let init_entries = table.entries.len();

        // Insert some values
        for i in 0..10 {
            let _ = table.try_insert(format!("value_{}", i));
        }

        assert!(!table.is_empty());
        assert!(table.num_retained() > 0);

        // Reset
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
        let mut table = ThetaHashTable::new(
            8,
            ResizeFactor::X8,
            0.5, // sampling_probability = 0.5
            DEFAULT_UPDATE_SEED,
        );
        assert_eq!(table.theta(), (MAX_THETA as f64 * 0.5) as u64);

        // Insert some values
        for i in 0..10 {
            let _ = table.try_insert(format!("value_{}", i));
        }

        table.reset();

        // With sampling_probability = 0.5, theta should be MAX_THETA * 0.5
        assert_eq!(table.theta(), (MAX_THETA as f64 * 0.5) as u64);
        assert!(table.is_empty());
    }

    #[test]
    fn test_iterator() {
        let mut table = ThetaHashTable::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

        // Insert some values
        let mut inserted_hashes = vec![];
        for i in 0..10 {
            let hash = table.hash(i);
            if table.try_insert(i) {
                inserted_hashes.push(hash);
            }
        }

        // Check iterator
        let iter_hashes: Vec<u64> = table.iter().collect();
        assert_eq!(iter_hashes.len(), table.num_retained());
        assert_eq!(iter_hashes.len(), inserted_hashes.len());

        // All inserted hashes should be in iterator
        for hash in &inserted_hashes {
            assert!(iter_hashes.contains(hash));
        }

        // Iterator should not contain 0
        assert!(!iter_hashes.contains(&0));
    }

    #[test]
    fn test_empty_table_operations() {
        let mut table = ThetaHashTable::new(8, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);

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
        let mut table = ThetaHashTable::new(5, ResizeFactor::X8, 1.0, DEFAULT_UPDATE_SEED);
        let k = 1u64 << 5; // k = 32

        // Insert many values to trigger rebuild
        let mut i = 0;
        let mut inserted_hashes = vec![];
        loop {
            let hash = table.hash(i);
            i += 1;
            if table.try_insert(i - 1) {
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
            if table.try_insert(i - 1) {
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
            if table.try_insert(i - 1) {
                inserted_hashes.push(hash);
                break;
            }
        }

        // assert all entries are less than kth
        inserted_hashes.sort();
        let kth = inserted_hashes[k as usize];
        assert!(table.iter().all(|e| e < kth));
        assert_eq!(table.theta(), kth);
    }
}
