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
pub(super) mod hash_table;
pub(super) mod intersection;
pub(super) mod jaccard_similarity;
pub(super) mod sketch_state;
pub(super) mod union;

pub use self::jaccard_similarity::JaccardSimilarity;

/// Minimal entry behavior required by the shared hash table and set operations.
pub(super) trait SketchEntry {
    fn hash(&self) -> u64;
}

pub(super) trait KeySketch: Copy {
    fn metadata(self) -> sketch_state::ThetaFamilySketchMetadata;

    fn hashes(self) -> impl Iterator<Item = u64>;
}

pub(super) trait EntrySketch: KeySketch {
    type Entry: SketchEntry;

    fn entries(self) -> impl Iterator<Item = Self::Entry>;
}
