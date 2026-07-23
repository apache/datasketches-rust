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

pub(crate) mod binomial_bounds;
pub(crate) mod constants;
pub(crate) mod hash_table;
pub(crate) mod union;

/// An entry retained by a Theta sketch family hash table.
pub trait RawHashTableEntry {
    /// Return the hash used as this entry's key.
    fn hash(&self) -> u64;
}

/// Read-only input accepted by a raw Theta union.
///
/// This trait carries complete retained entries, so tuple unions can use the same state machine
/// while merging their per-key summaries.
pub trait RawThetaSketchView<E: RawHashTableEntry> {
    /// Return the 16-bit seed hash.
    fn seed_hash(&self) -> u16;

    /// Return theta as a `u64` threshold.
    fn theta(&self) -> u64;

    /// Return whether this sketch has not received any updates.
    fn is_empty(&self) -> bool;

    /// Return whether retained entries are ordered by ascending hash.
    fn is_ordered(&self) -> bool;

    /// Return an iterator over retained entries.
    fn iter(&self) -> impl Iterator<Item = E> + '_;

    /// Return the number of retained entries.
    fn num_retained(&self) -> usize;
}

/// Raw compact-sketch state from which a sketch family creates its compact result type.
#[derive(Debug)]
pub(crate) struct RawCompactParts<E> {
    pub entries: Vec<E>,
    pub theta: u64,
    pub seed_hash: u16,
    pub ordered: bool,
    pub empty: bool,
}
