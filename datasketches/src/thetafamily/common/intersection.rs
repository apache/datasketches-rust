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

use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::thetacommon::EntrySketch;
use crate::thetacommon::SketchEntry;
use crate::thetacommon::constants::HASH_TABLE_REBUILD_THRESHOLD;
use crate::thetacommon::hash_table::SketchHashTable;
use crate::thetacommon::sketch_state::CompactSketchState;
use crate::thetacommon::sketch_state::ThetaSketchState;
use crate::thetacommon::sketch_state::ThetaThreshold;

/// Merges an incoming entry into an existing entry with the same hash.
///
/// For plain Theta entries there is nothing to merge (the entry is only a hash); tuple entries
/// combine their summaries.
pub trait IntersectionMergePolicy<E> {
    fn merge(&self, existing: &mut E, incoming: E);
}

/// Generic state machine shared by Theta and Tuple intersections.
///
/// `E` is the retained entry type. `P` defines how equal-hash entries are combined; it is only
/// exercised for keys present in both the running intersection and the incoming sketch.
#[derive(Debug)]
pub struct IntersectionState<E, P> {
    table: SketchHashTable<E>,
    policy: P,
    result_state: IntersectionResultState,
}

/// State of the value currently represented by an intersection operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntersectionResultState {
    Uninitialized,
    Empty,
    NonEmpty,
}

impl<E, P> IntersectionState<E, P>
where
    E: SketchEntry,
{
    /// Creates a new intersection operator for the given `seed` and entry-merge `policy`.
    pub fn new(seed: u64, policy: P) -> Result<Self, Error> {
        let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
        Ok(Self {
            result_state: IntersectionResultState::Uninitialized,
            table: SketchHashTable::for_set_operation(0, 0, ThetaThreshold::MAX, seed, seed_hash),
            policy,
        })
    }

    /// Updates the intersection with a given sketch.
    ///
    /// The intersection can be viewed as starting from the "universe" set, and every update
    /// reduces the current set to the keys it shares with `sketch`.
    pub fn update<S>(&mut self, sketch: S) -> Result<(), Error>
    where
        S: EntrySketch<Entry = E>,
        E: Clone,
        P: IntersectionMergePolicy<E>,
    {
        let metadata = sketch.metadata();
        let table_without_entries = |table: &SketchHashTable<E>, retention_theta| {
            SketchHashTable::for_set_operation(
                0,
                0,
                retention_theta,
                table.seed(),
                table.seed_hash(),
            )
        };

        if self.result_state == IntersectionResultState::Empty {
            return Ok(());
        }

        if metadata.is_empty() {
            self.result_state = IntersectionResultState::Empty;
            self.table = table_without_entries(&self.table, ThetaThreshold::MAX);
            return Ok(());
        }

        check_seed_hash(
            self.table.seed_hash(),
            metadata.seed_hash(),
            "intersection update",
            ErrorKind::InvalidArgument,
        )?;

        let result_theta = self.table.retention_theta().min(metadata.theta());
        self.table.set_retention_theta(result_theta);

        if self.result_state == IntersectionResultState::NonEmpty && self.table.num_retained() == 0
        {
            return Ok(());
        }

        if metadata.num_retained() == 0 {
            self.result_state = IntersectionResultState::NonEmpty;
            self.table = table_without_entries(&self.table, result_theta);
            return Ok(());
        }

        // first update, copy incoming entries
        if self.result_state == IntersectionResultState::Uninitialized {
            self.result_state = IntersectionResultState::NonEmpty;
            let lg_size = SketchHashTable::<E>::lg_size_from_count_for_rebuild(
                metadata.num_retained(),
                HASH_TABLE_REBUILD_THRESHOLD,
            );
            // The retained count is at least one here, so lg_size >= 1 and lg_size - 1 below
            // cannot underflow.
            debug_assert!(lg_size >= 1);
            self.table = SketchHashTable::for_set_operation(
                lg_size,
                lg_size - 1,
                result_theta,
                self.table.seed(),
                self.table.seed_hash(),
            );
            for entry in sketch.entries() {
                let hash = entry.hash();
                if !self.table.upsert_entry(hash, |existing| match existing {
                    Some(_) => None,
                    None => Some(entry),
                }) {
                    return Err(Error::invalid_argument(
                        "Insert entries from sketch fail, possibly corrupted input sketch",
                    ));
                }
            }
            // Safety check.
            if self.table.num_retained() != metadata.num_retained() {
                return Err(Error::invalid_argument(
                    "num entries mismatch, possibly corrupted input sketch",
                ));
            }
        } else {
            let max_matches = self.table.num_retained().min(metadata.num_retained());
            let mut matched_entries = Vec::with_capacity(max_matches);
            let mut count = 0;
            for entry in sketch.entries() {
                let hash = entry.hash();
                if hash < self.table.retention_theta().get() {
                    if let Some(existing) = self.table.entry(hash) {
                        if matched_entries.len() == max_matches {
                            return Err(Error::invalid_argument(
                                "max matches exceeded, possibly corrupted input sketch",
                            ));
                        }
                        let mut merged = existing.clone();
                        self.policy.merge(&mut merged, entry);
                        matched_entries.push(merged);
                    }
                } else if metadata.is_ordered() {
                    break; // early stop for ordered sketches
                }
                count += 1;
            }
            // Safety check.
            if count > metadata.num_retained() {
                return Err(Error::invalid_argument(
                    "more keys than expected, possibly corrupted input sketch",
                ));
            } else if !metadata.is_ordered() && count < metadata.num_retained() {
                return Err(Error::invalid_argument(
                    "fewer keys than expected, possibly corrupted input sketch",
                ));
            }
            if matched_entries.is_empty() {
                self.table = table_without_entries(&self.table, result_theta);
                if result_theta == ThetaThreshold::MAX {
                    self.result_state = IntersectionResultState::Empty;
                }
            } else {
                let lg_size = SketchHashTable::<E>::lg_size_from_count_for_rebuild(
                    matched_entries.len(),
                    HASH_TABLE_REBUILD_THRESHOLD,
                );
                // matched_entries is non-empty here (the empty case is handled above), so
                // lg_size >= 1 and lg_size - 1 below cannot underflow.
                debug_assert!(lg_size >= 1);
                self.table = SketchHashTable::for_set_operation(
                    lg_size,
                    lg_size - 1,
                    result_theta,
                    self.table.seed(),
                    self.table.seed_hash(),
                );
                for entry in matched_entries {
                    let hash = entry.hash();
                    if !self.table.upsert_entry(hash, |existing| match existing {
                        Some(_) => None,
                        None => Some(entry),
                    }) {
                        return Err(Error::invalid_argument(
                            "duplicate key, possibly corrupted input sketch",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Returns whether this operator has received at least one update.
    pub fn has_result(&self) -> bool {
        self.result_state != IntersectionResultState::Uninitialized
    }

    /// Returns the estimated size of the heap allocations in bytes.
    pub fn estimated_size(&self) -> usize {
        self.table.estimated_size()
    }

    /// Returns the current intersection as canonical compact-sketch state.
    pub fn to_compact_sketch_state(&self, ordered: bool) -> Option<CompactSketchState<E>>
    where
        E: Clone,
    {
        match self.result_state {
            IntersectionResultState::Uninitialized => None,
            IntersectionResultState::Empty => {
                Some(CompactSketchState::empty(self.table.seed_hash()))
            }
            IntersectionResultState::NonEmpty => Some(self.table.to_compact_sketch_state(
                ThetaSketchState::non_empty(self.table.retention_theta()),
                ordered,
            )),
        }
    }
}
