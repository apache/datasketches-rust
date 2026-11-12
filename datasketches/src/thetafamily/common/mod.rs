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

//! Data structures and functions that may be used across all the Theta sketch family.

pub(super) mod a_not_b;
pub(super) mod binomial_bounds;
pub(super) mod constants;
pub mod hash_table;
pub(super) mod intersection;
pub mod jaccard_similarity;
pub mod sketch_state;
pub(super) mod union;

/// Minimal entry behavior required by the shared hash table, compaction, and set operations.
pub trait SketchEntry: Sized {
    fn hash(&self) -> u64;

    fn sort_by_hash(entries: &mut Vec<Self>);
}

impl SketchEntry for u64 {
    #[inline]
    fn hash(&self) -> u64 {
        *self
    }

    fn sort_by_hash(entries: &mut Vec<Self>) {
        sketch_state::sort_copy_by_hash(entries);
    }
}

impl<E: SketchEntry> SketchEntry for &E {
    #[inline]
    fn hash(&self) -> u64 {
        E::hash(self)
    }

    fn sort_by_hash(entries: &mut Vec<Self>) {
        sketch_state::sort_copy_by_hash(entries);
    }
}

pub trait KeySketch: Copy {
    fn metadata(self) -> sketch_state::ThetaFamilySketchMetadata;

    fn hashes(self) -> impl Iterator<Item = u64>;

    /// Returns the backing hash table when this sketch stores entries in one.
    fn hash_table(&self) -> Option<&hash_table::SketchHashTable<impl SketchEntry>>;
}

pub(super) trait EntrySketch: KeySketch {
    type Entry: SketchEntry;

    fn entries(self) -> impl Iterator<Item = Self::Entry>;
}
