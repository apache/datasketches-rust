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

pub(crate) mod sealed {
    pub struct Token;
}

/// An entry retained by a Theta sketch family hash table.
///
/// This trait is sealed because the shared set-operation state machines rely on entry invariants
/// maintained by the sketch implementations in this crate.
pub trait RetainedEntry {
    #[doc(hidden)]
    fn __private(&self, _: sealed::Token);

    /// Return the hash used as this entry's key.
    fn hash(&self) -> u64;
}

/// Read-only hash-key view shared by Theta-family sketches.
///
/// Key-only operations use this interface without requiring access to, or cloning, payloads such
/// as Tuple summaries.
///
/// This trait is sealed because set operations rely on the reported metadata, retained count, and
/// iterators describing the same sketch state.
pub trait ThetaKeySketchView {
    #[doc(hidden)]
    fn __private(&self, _: sealed::Token);

    /// Return the 16-bit seed hash.
    fn seed_hash(&self) -> u16;

    /// Return theta as a `u64` threshold.
    fn theta64(&self) -> u64;

    /// Return whether this sketch has not received any updates.
    fn is_empty(&self) -> bool;

    /// Return whether retained entries are ordered by ascending hash.
    fn is_ordered(&self) -> bool;

    /// Return an iterator over retained hash keys.
    fn iter_hashes(&self) -> impl Iterator<Item = u64> + '_;

    /// Return the number of retained entries.
    fn num_retained(&self) -> usize;
}

/// Read-only retained-entry view accepted by Theta-family set operations.
///
/// This trait extends [`ThetaKeySketchView`] with complete retained entries, so operations such as
/// union and intersection can preserve and combine Tuple summaries.
///
/// Like [`ThetaKeySketchView`], this trait is sealed and cannot be implemented outside this crate.
pub trait ThetaFamilySketchView: ThetaKeySketchView {
    /// The retained entry representation yielded by this view.
    type Entry: RetainedEntry;

    /// Return an iterator over retained entries.
    fn iter(&self) -> impl Iterator<Item = Self::Entry> + '_;
}
