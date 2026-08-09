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

use crate::thetacommon::RetainedEntry;
use crate::thetacommon::hash_table::SketchHashTable;
use crate::thetacommon::intersection::IntersectionMergePolicy;
use crate::thetacommon::sealed;
use crate::thetacommon::union::UnionMergePolicy;
use crate::tuple::SummaryCombinePolicy;

/// A retained entry in a Tuple sketch: a hash key together with its associated summary.
#[derive(Debug, Clone)]
pub struct TupleEntry<S> {
    // Note that this field is stored as `NonZeroU64` (hash 0 is screened out before insertion),
    // so `Option<TupleEntry<S>>` keeps the niche and takes no more space than `TupleEntry<S>`
    // itself.
    hash: NonZeroU64,
    summary: S,
}

impl<S> TupleEntry<S> {
    /// Creates an entry from a hash known to be non-zero.
    ///
    /// # Panics
    ///
    /// Panics if `hash` is zero.
    pub(super) fn new(hash: u64, summary: S) -> Self {
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
pub(super) type TupleHashTable<S> = SketchHashTable<TupleEntry<S>>;

impl<S> RetainedEntry for TupleEntry<S> {
    fn __private(&self, _: sealed::Token) {}

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

    /// Returns an iterator over retained entries as `(hash, &summary)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &S)> + '_ {
        self.iter_entries()
            .map(|entry| (entry.hash.get(), &entry.summary))
    }
}

impl<P: SummaryCombinePolicy> UnionMergePolicy<TupleEntry<P::Summary>> for P {
    fn merge(&self, existing: &mut TupleEntry<P::Summary>, incoming: TupleEntry<P::Summary>) {
        self.combine(&mut existing.summary, &incoming.summary);
    }
}

impl<P: SummaryCombinePolicy> IntersectionMergePolicy<TupleEntry<P::Summary>> for P {
    fn merge(&self, existing: &mut TupleEntry<P::Summary>, incoming: TupleEntry<P::Summary>) {
        self.combine(&mut existing.summary, &incoming.summary);
    }
}
