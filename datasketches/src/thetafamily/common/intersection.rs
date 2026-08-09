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
use crate::thetacommon::RetainedEntry;
use crate::thetacommon::SketchMetadata;
use crate::thetacommon::constants::HASH_TABLE_REBUILD_THRESHOLD;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::CompactSketchParts;
use crate::thetacommon::hash_table::SketchHashTable;

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
    has_result: bool,
}

impl<E, P> IntersectionState<E, P>
where
    E: RetainedEntry,
{
    /// Creates a new intersection operator for the given `seed` and entry-merge `policy`.
    pub fn new(seed: u64, policy: P) -> Self {
        Self {
            has_result: false,
            table: SketchHashTable::from_raw_parts(
                0,
                0,
                ResizeFactor::X1,
                1.0,
                MAX_THETA,
                seed,
                false,
            ),
            policy,
        }
    }

    /// Updates the intersection with a given sketch.
    ///
    /// The intersection can be viewed as starting from the "universe" set, and every update
    /// reduces the current set to the keys it shares with `sketch`.
    pub fn update<I>(&mut self, metadata: SketchMetadata, entries: I) -> Result<(), Error>
    where
        I: Iterator<Item = E>,
        E: Clone,
        P: IntersectionMergePolicy<E>,
    {
        let SketchMetadata {
            seed_hash,
            theta,
            empty,
            ordered,
            num_retained,
        } = metadata;
        let new_default_table = |table: &SketchHashTable<E>| {
            SketchHashTable::from_raw_parts(
                0,
                0,
                ResizeFactor::X1,
                1.0,
                table.theta(),
                table.seed(),
                table.is_empty(),
            )
        };

        if self.table.is_empty() {
            return Ok(());
        }

        if empty {
            self.table.set_empty(true);
        } else {
            check_seed_hash(
                self.table.seed_hash(),
                seed_hash,
                "intersection update",
                ErrorKind::InvalidArgument,
            )?;
        }

        self.table.set_theta(if self.table.is_empty() {
            MAX_THETA
        } else {
            self.table.theta().min(theta)
        });

        if self.has_result && self.table.num_retained() == 0 {
            return Ok(());
        }

        if num_retained == 0 {
            self.has_result = true;
            self.table = new_default_table(&self.table);
            return Ok(());
        }

        // first update, copy incoming entries
        if !self.has_result {
            self.has_result = true;
            let lg_size = SketchHashTable::<E>::lg_size_from_count_for_rebuild(
                num_retained,
                HASH_TABLE_REBUILD_THRESHOLD,
            );
            // num_retained >= 1 here (the zero case returned early above), so lg_size >= 1 and
            // lg_size - 1 below cannot underflow.
            debug_assert!(lg_size >= 1);
            self.table = SketchHashTable::from_raw_parts(
                lg_size,
                lg_size - 1,
                ResizeFactor::X1,
                1.0,
                self.table.theta(),
                self.table.seed(),
                self.table.is_empty(),
            );
            for entry in entries {
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
            if self.table.num_retained() != num_retained {
                return Err(Error::invalid_argument(
                    "num entries mismatch, possibly corrupted input sketch",
                ));
            }
        } else {
            let max_matches = self.table.num_retained().min(num_retained);
            let mut matched_entries = Vec::with_capacity(max_matches);
            let mut count = 0;
            for entry in entries {
                let hash = entry.hash();
                if hash < self.table.theta() {
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
                } else if ordered {
                    break; // early stop for ordered sketches
                }
                count += 1;
            }
            // Safety check.
            if count > num_retained {
                return Err(Error::invalid_argument(
                    "more keys than expected, possibly corrupted input sketch",
                ));
            } else if !ordered && count < num_retained {
                return Err(Error::invalid_argument(
                    "fewer keys than expected, possibly corrupted input sketch",
                ));
            }
            if matched_entries.is_empty() {
                self.table = new_default_table(&self.table);
                if self.table.theta() == MAX_THETA {
                    self.table.set_empty(true);
                }
            } else {
                let lg_size = SketchHashTable::<E>::lg_size_from_count_for_rebuild(
                    matched_entries.len(),
                    HASH_TABLE_REBUILD_THRESHOLD,
                );
                // matched_entries is non-empty here (the empty case is handled above), so
                // lg_size >= 1 and lg_size - 1 below cannot underflow.
                debug_assert!(lg_size >= 1);
                self.table = SketchHashTable::from_raw_parts(
                    lg_size,
                    lg_size - 1,
                    ResizeFactor::X1,
                    1.0,
                    self.table.theta(),
                    self.table.seed(),
                    self.table.is_empty(),
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
        self.has_result
    }

    /// Returns the estimated size of the heap allocations in bytes.
    pub fn estimated_size(&self) -> usize {
        self.table.estimated_size()
    }

    /// Return the current intersection state as compact-sketch parts.
    pub fn result(&self, ordered: bool) -> CompactSketchParts<E>
    where
        E: Clone,
    {
        let mut entries: Vec<E> = self.table.iter_entries().cloned().collect();
        if ordered {
            entries.sort_unstable_by_key(RetainedEntry::hash);
        }
        CompactSketchParts {
            entries,
            theta: self.table.theta(),
            seed_hash: self.table.seed_hash(),
            ordered,
            empty: self.table.is_empty(),
        }
    }
}
