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

use std::fmt;

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::thetafamily::common::EntrySketch;
use crate::thetafamily::common::SketchEntry;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::hash_table::SketchHashTable;
use crate::thetafamily::common::sketch_state::CompactSketchState;
use crate::thetafamily::common::sketch_state::ThetaFamilySketchMetadata;

/// Merges an incoming entry into an existing entry with the same hash.
pub trait UnionMergePolicy<E> {
    fn merge(&self, existing: &mut E, incoming: E);
}

/// Generic state machine shared by Theta and Tuple unions.
///
/// `E` is the retained entry type. Ordinary Theta entries only contain a hash, while tuple
/// entries also carry a summary. `P` defines how equal-hash entries are combined.
#[derive(Debug)]
pub struct UnionState<E, P> {
    table: SketchHashTable<E>,
    policy: P,
    // None until the union receives a non-empty input sketch.
    result_theta: Option<u64>,
}

impl<E, P> UnionState<E, P>
where
    E: SketchEntry,
{
    pub fn new(
        lg_k: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
        policy: P,
    ) -> Result<Self, Error> {
        let table = SketchHashTable::new(lg_k, resize_factor, sampling_probability, seed)?;
        Ok(Self {
            result_theta: None,
            table,
            policy,
        })
    }

    /// Incorporate a sketch into the union.
    pub fn update<S>(&mut self, sketch: S) -> Result<(), Error>
    where
        S: EntrySketch<Entry = E>,
        P: UnionMergePolicy<E>,
    {
        let ThetaFamilySketchMetadata::NonEmpty {
            seed_hash,
            theta,
            ordered,
            ..
        } = sketch.metadata()
        else {
            return Ok(());
        };

        check_seed_hash(
            self.table.seed_hash(),
            seed_hash,
            "union update",
            ErrorKind::InvalidArgument,
        )?;

        let current_theta = self.result_theta.unwrap_or(self.table.retention_theta());
        let result_theta = current_theta.min(theta);
        self.result_theta = Some(result_theta);

        for entry in sketch.entries() {
            let hash = entry.hash();
            if hash < result_theta && hash < self.table.retention_theta() {
                self.table.upsert_entry(hash, |existing| match existing {
                    Some(existing) => {
                        self.policy.merge(existing, entry);
                        None
                    }
                    None => Some(entry),
                });
            } else if ordered {
                break;
            }
        }
        self.result_theta = Some(result_theta.min(self.table.retention_theta()));

        Ok(())
    }

    /// Returns the union as canonical compact-sketch state.
    #[cfg(feature = "tuple")]
    pub fn to_compact_sketch_state(&self, ordered: bool) -> CompactSketchState<E>
    where
        E: Clone,
    {
        let Some(theta) = self.result_theta else {
            return CompactSketchState::empty(self.table.seed_hash());
        };
        let entries = self.collect_retained_entries(theta, Clone::clone);
        self.finish_compaction(entries, theta, ordered)
    }

    /// Folds over the canonical retained entries and returns `(theta, output)`.
    ///
    /// Avoids allocating an intermediate vector whenever the retained entry count fits within the
    /// nominal capacity `2^lg_k`.
    pub fn fold_retained<Acc>(&self, init: Acc, mut f: impl FnMut(Acc, &E) -> Acc) -> (u64, Acc) {
        let Some(result_theta) = self.result_theta else {
            return (MAX_THETA, init);
        };
        let theta = result_theta.min(self.table.retention_theta());
        let nominal_num = 1usize << self.table.lg_nom_size();
        if self.table.num_retained() <= nominal_num {
            let acc = if theta >= self.table.retention_theta() {
                self.table.iter_entries().fold(init, &mut f)
            } else {
                self.table
                    .iter_entries()
                    .filter(|entry| entry.hash() < theta)
                    .fold(init, &mut f)
            };
            return (theta, acc);
        }

        let mut retained = self.collect_retained_entries(theta, |entry| entry);
        let final_theta = if retained.len() > nominal_num {
            let (_, kth, _) =
                retained.select_nth_unstable_by_key(nominal_num, |entry| entry.hash());
            let kth_hash = kth.hash();
            retained.truncate(nominal_num);
            kth_hash
        } else {
            theta
        };
        let acc = retained.into_iter().fold(init, f);
        (final_theta, acc)
    }

    /// Returns the union as canonical compact hash state.
    #[cfg(feature = "theta")]
    pub fn to_compact_hash_state(&self, ordered: bool) -> CompactSketchState<u64> {
        let Some(theta) = self.result_theta else {
            return CompactSketchState::empty(self.table.seed_hash());
        };
        let hashes = if theta >= self.table.retention_theta() {
            self.table.retained_hashes()
        } else {
            self.table
                .iter_entries()
                .map(SketchEntry::hash)
                .filter(|&hash| hash < theta)
                .collect()
        };
        self.finish_compaction(hashes, theta, ordered)
    }

    /// Returns the union as canonical compact-sketch state that borrows the retained entries.
    #[cfg(feature = "tuple")]
    pub fn to_borrowed_compact_sketch_state(&self, ordered: bool) -> CompactSketchState<&E> {
        let Some(theta) = self.result_theta else {
            return CompactSketchState::empty(self.table.seed_hash());
        };
        let entries = self.collect_retained_entries(theta, |entry| entry);
        self.finish_compaction(entries, theta, ordered)
    }

    fn collect_retained_entries<'a, T>(
        &'a self,
        theta: u64,
        map: impl FnMut(&'a E) -> T,
    ) -> Vec<T> {
        if theta >= self.table.retention_theta() {
            self.table.iter_entries().map(map).collect()
        } else {
            self.table
                .iter_entries()
                .filter(|entry| entry.hash() < theta)
                .map(map)
                .collect()
        }
    }

    fn finish_compaction<T: SketchEntry>(
        &self,
        mut retained_entries: Vec<T>,
        result_theta: u64,
        ordered: bool,
    ) -> CompactSketchState<T> {
        let mut theta = result_theta.min(self.table.retention_theta());
        let nominal_num = 1usize << self.table.lg_nom_size();
        if retained_entries.len() > nominal_num {
            let (_, kth, _) =
                retained_entries.select_nth_unstable_by_key(nominal_num, SketchEntry::hash);
            theta = kth.hash();
            retained_entries.truncate(nominal_num);
        }

        CompactSketchState::compact(
            retained_entries,
            theta,
            self.table.seed_hash(),
            false,
            ordered,
        )
    }

    /// Reset the union to its initial state.
    pub fn reset(&mut self) {
        self.table.reset();
        self.result_theta = None;
    }

    /// Returns an estimate of memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<P>() + self.table.estimated_size()
    }

    /// Formats a concise diagnostic summary for Theta-family union operators.
    pub fn fmt_debug(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let is_empty = self.result_theta.is_none();
        let union_theta = self
            .result_theta
            .map_or(MAX_THETA, |theta| theta.min(self.table.retention_theta()));
        let num_retained = match self.result_theta {
            None => 0,
            Some(theta) if theta >= self.table.retention_theta() => self.table.num_retained(),
            Some(theta) => self
                .table
                .iter_entries()
                .filter(|entry| entry.hash() < theta)
                .count(),
        };
        f.debug_struct(name)
            .field("lg_k", &self.table.lg_nom_size())
            .field("seed_hash", &self.table.seed_hash())
            .field("is_empty", &is_empty)
            .field("union_theta", &union_theta)
            .field("num_retained", &num_retained)
            .finish()
    }
}
