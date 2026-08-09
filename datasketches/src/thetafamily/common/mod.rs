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

pub(crate) mod a_not_b;
pub(crate) mod binomial_bounds;
pub(crate) mod constants;
pub(crate) mod hash_table;
pub(crate) mod intersection;
pub(crate) mod jaccard_similarity;
pub(crate) mod union;

pub use self::jaccard_similarity::JaccardSimilarity;

/// Minimal entry behavior required by the shared hash table and set-operation state machines.
pub(crate) trait RetainedEntry {
    fn hash(&self) -> u64;
}

/// Sketch properties inspected by Theta-family set operations.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SketchMetadata {
    pub(crate) seed_hash: u16,
    pub(crate) theta: u64,
    pub(crate) empty: bool,
    pub(crate) ordered: bool,
    pub(crate) num_retained: usize,
}

impl SketchMetadata {
    pub(crate) fn new(
        seed_hash: u16,
        theta: u64,
        empty: bool,
        ordered: bool,
        num_retained: usize,
    ) -> Self {
        Self {
            seed_hash,
            theta,
            empty,
            ordered,
            num_retained,
        }
    }
}
