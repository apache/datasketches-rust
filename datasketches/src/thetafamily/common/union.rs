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

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::thetacommon::EntrySketch;
use crate::thetacommon::SketchEntry;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::SketchHashTable;
use crate::thetacommon::sketch_state::CompactSketchState;
use crate::thetacommon::sketch_state::ThetaFamilySketchMetadata;

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
    pub fn to_compact_sketch_state(&self, ordered: bool) -> CompactSketchState<E>
    where
        E: Clone,
    {
        let Some(result_theta) = self.result_theta else {
            return CompactSketchState::empty(self.table.seed_hash());
        };

        let mut theta = result_theta.min(self.table.retention_theta());
        let mut retained_entries = if result_theta >= self.table.retention_theta() {
            self.table.iter_entries().cloned().collect::<Vec<_>>()
        } else {
            self.table
                .iter_entries()
                .filter(|entry| entry.hash() < theta)
                .cloned()
                .collect::<Vec<_>>()
        };

        let nominal_num = 1usize << self.table.lg_nom_size();
        if retained_entries.len() > nominal_num {
            let (_, kth, _) =
                retained_entries.select_nth_unstable_by_key(nominal_num, |entry| entry.hash());
            theta = kth.hash();
            retained_entries.truncate(nominal_num);
        }

        let ordered = ordered || (retained_entries.len() == 1 && theta == MAX_THETA);
        if ordered {
            retained_entries.sort_unstable_by_key(SketchEntry::hash);
        }

        CompactSketchState::non_empty(retained_entries, theta, self.table.seed_hash(), ordered)
    }

    /// Reset the union to its initial state.
    pub fn reset(&mut self) {
        self.table.reset();
        self.result_theta = None;
    }

    /// Returns the retained capacity of the internal hash table in bytes.
    ///
    /// The estimate is shallow with respect to entries and the merge policy.
    pub fn estimated_size(&self) -> usize {
        self.table.estimated_size()
    }
}
