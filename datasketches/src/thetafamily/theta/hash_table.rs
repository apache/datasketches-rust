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

use crate::thetacommon::SketchEntry;
use crate::thetacommon::hash_table::SketchHashTable;

/// Specific hash table for theta sketch
///
/// It maintains an array capacity max to 2^lg_max_size:
/// * Before it reaches the max capacity, it will extend the array based on resize_factor.
/// * After it reaches the capacity bigger than 2^lg_nom_size, every time the number of entries
///   exceeds the threshold, it will rebuild the table: only keep the min 2^lg_nom_size entries and
///   update the theta to the k-th smallest entry.
pub(super) type ThetaHashTable = SketchHashTable<ThetaEntry>;

/// A retained entry in a Theta sketch.
#[derive(Debug, Clone, Copy)]
pub struct ThetaEntry {
    hash: NonZeroU64,
}

impl ThetaEntry {
    pub(super) fn new(hash: u64) -> Self {
        let hash = NonZeroU64::new(hash).expect("hash must be non-zero");
        Self { hash }
    }

    /// Return the hash used as this entry's key.
    pub fn hash(&self) -> u64 {
        self.hash.get()
    }
}

impl SketchEntry for ThetaEntry {
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
}
