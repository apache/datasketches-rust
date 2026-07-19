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
use crate::thetacommon::RawHashTableEntry;
use crate::thetacommon::RawThetaSketchView;
use crate::thetacommon::constants::HASH_TABLE_REBUILD_THRESHOLD;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::RawCompactParts;
use crate::thetacommon::hash_table::RawHashTable;

/// Merges an incoming entry into an existing entry with the same hash.
///
/// For plain Theta entries there is nothing to merge (the entry is only a hash); tuple entries
/// combine their summaries.
pub trait RawThetaIntersectionPolicy<E> {
    fn merge(&self, existing: &mut E, incoming: E);
}

/// Generic state machine shared by Theta and Tuple intersections.
///
/// `E` is the retained entry type. `P` defines how equal-hash entries are combined; it is only
/// exercised for keys present in both the running intersection and the incoming sketch.
#[derive(Debug)]
pub struct RawThetaIntersection<E, P> {
    table: RawHashTable<E>,
    policy: P,
    is_valid: bool,
}

impl<E, P> RawThetaIntersection<E, P>
where
    E: RawHashTableEntry,
{
    /// Creates a new intersection operator for the given `seed` and entry-merge `policy`.
    pub fn new(seed: u64, policy: P) -> Self {
        Self {
            is_valid: false,
            table: RawHashTable::from_raw_parts(
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
    pub fn update<S>(&mut self, sketch: &S) -> Result<(), Error>
    where
        S: RawThetaSketchView<E>,
        E: Clone,
        P: RawThetaIntersectionPolicy<E>,
    {
        let new_default_table = |table: &RawHashTable<E>| {
            RawHashTable::from_raw_parts(
                0,
                0,
                ResizeFactor::X1,
                1.0,
                table.theta(),
                table.hash_seed(),
                table.is_empty(),
            )
        };

        if self.table.is_empty() {
            return Ok(());
        }

        if !sketch.is_empty() && sketch.seed_hash() != self.table.seed_hash() {
            return Err(Error::invalid_argument(format!(
                "incompatible seed hash: expected {}, got {}",
                self.table.seed_hash(),
                sketch.seed_hash()
            )));
        }

        if sketch.is_empty() {
            self.table.set_empty(true);
        }

        self.table.set_theta(if self.table.is_empty() {
            MAX_THETA
        } else {
            self.table.theta().min(sketch.theta())
        });

        if self.is_valid && self.table.num_retained() == 0 {
            return Ok(());
        }

        if sketch.num_retained() == 0 {
            self.is_valid = true;
            self.table = new_default_table(&self.table);
            return Ok(());
        }

        // first update, copy incoming entries
        if !self.is_valid {
            self.is_valid = true;
            let lg_size = RawHashTable::<E>::lg_size_from_count_for_rebuild(
                sketch.num_retained(),
                HASH_TABLE_REBUILD_THRESHOLD,
            );
            // num_retained >= 1 here (the zero case returned early above), so lg_size >= 1 and
            // lg_size - 1 below cannot underflow.
            debug_assert!(lg_size >= 1);
            self.table = RawHashTable::from_raw_parts(
                lg_size,
                lg_size - 1,
                ResizeFactor::X1,
                1.0,
                self.table.theta(),
                self.table.hash_seed(),
                self.table.is_empty(),
            );
            for entry in sketch.iter() {
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
            if self.table.num_retained() != sketch.num_retained() {
                return Err(Error::invalid_argument(
                    "num entries mismatch, possibly corrupted input sketch",
                ));
            }
        } else {
            let max_matches = self.table.num_retained().min(sketch.num_retained());
            let mut matched_entries = Vec::with_capacity(max_matches);
            let mut count = 0;
            for entry in sketch.iter() {
                let hash = entry.hash();
                if hash < self.table.theta() {
                    if let Some(existing) = self.table.get_entry(hash) {
                        if matched_entries.len() == max_matches {
                            return Err(Error::invalid_argument(
                                "max matches exceeded, possibly corrupted input sketch",
                            ));
                        }
                        let mut merged = existing.clone();
                        self.policy.merge(&mut merged, entry);
                        matched_entries.push(merged);
                    }
                } else if sketch.is_ordered() {
                    break; // early stop for ordered sketches
                }
                count += 1;
            }
            // Safety check.
            if count > sketch.num_retained() {
                return Err(Error::invalid_argument(
                    "more keys than expected, possibly corrupted input sketch",
                ));
            } else if !sketch.is_ordered() && count < sketch.num_retained() {
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
                let lg_size = RawHashTable::<E>::lg_size_from_count_for_rebuild(
                    matched_entries.len(),
                    HASH_TABLE_REBUILD_THRESHOLD,
                );
                // matched_entries is non-empty here (the empty case is handled above), so
                // lg_size >= 1 and lg_size - 1 below cannot underflow.
                debug_assert!(lg_size >= 1);
                self.table = RawHashTable::from_raw_parts(
                    lg_size,
                    lg_size - 1,
                    ResizeFactor::X1,
                    1.0,
                    self.table.theta(),
                    self.table.hash_seed(),
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
        self.is_valid
    }

    /// Return the current intersection state as raw compact-sketch parts.
    pub fn result(&self, ordered: bool) -> RawCompactParts<E>
    where
        E: Clone,
    {
        let mut entries: Vec<E> = self.table.iter_entries().cloned().collect();
        if ordered {
            entries.sort_unstable_by_key(RawHashTableEntry::hash);
        }
        RawCompactParts {
            entries,
            theta: self.table.theta(),
            seed_hash: self.table.seed_hash(),
            ordered,
            empty: self.table.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::DEFAULT_UPDATE_SEED;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestEntry {
        hash: u64,
        summary: u64,
    }

    impl RawHashTableEntry for TestEntry {
        fn hash(&self) -> u64 {
            self.hash
        }
    }

    struct TestSketch {
        entries: Vec<TestEntry>,
    }

    impl TestSketch {
        fn of_hashes(hashes: &[u64]) -> Self {
            Self {
                entries: hashes
                    .iter()
                    .map(|&hash| TestEntry { hash, summary: 1 })
                    .collect(),
            }
        }
    }

    impl RawThetaSketchView<TestEntry> for TestSketch {
        fn seed_hash(&self) -> u16 {
            crate::hash::compute_seed_hash(DEFAULT_UPDATE_SEED)
        }

        fn theta(&self) -> u64 {
            MAX_THETA
        }

        fn is_empty(&self) -> bool {
            self.entries.is_empty()
        }

        fn is_ordered(&self) -> bool {
            false
        }

        fn iter(&self) -> impl Iterator<Item = TestEntry> + '_ {
            self.entries.iter().cloned()
        }

        fn num_retained(&self) -> usize {
            self.entries.len()
        }
    }

    struct SumPolicy;

    impl RawThetaIntersectionPolicy<TestEntry> for SumPolicy {
        fn merge(&self, existing: &mut TestEntry, incoming: TestEntry) {
            existing.summary += incoming.summary;
        }
    }

    #[test]
    fn first_update_copies_entries() {
        let mut intersection = RawThetaIntersection::new(DEFAULT_UPDATE_SEED, SumPolicy);
        assert!(!intersection.has_result());

        intersection
            .update(&TestSketch::of_hashes(&[1, 2, 3]))
            .unwrap();

        assert!(intersection.has_result());
        let parts = intersection.result(true);
        assert_eq!(
            parts.entries,
            vec![
                TestEntry {
                    hash: 1,
                    summary: 1
                },
                TestEntry {
                    hash: 2,
                    summary: 1
                },
                TestEntry {
                    hash: 3,
                    summary: 1
                },
            ]
        );
    }

    #[test]
    fn second_update_keeps_matches_and_merges_with_policy() {
        let mut intersection = RawThetaIntersection::new(DEFAULT_UPDATE_SEED, SumPolicy);
        intersection
            .update(&TestSketch::of_hashes(&[1, 2, 3]))
            .unwrap();
        intersection
            .update(&TestSketch::of_hashes(&[2, 3, 4]))
            .unwrap();

        let parts = intersection.result(true);
        assert_eq!(
            parts.entries,
            vec![
                TestEntry {
                    hash: 2,
                    summary: 2
                },
                TestEntry {
                    hash: 3,
                    summary: 2
                },
            ]
        );
    }

    #[test]
    fn disjoint_second_update_empties_intersection() {
        let mut intersection = RawThetaIntersection::new(DEFAULT_UPDATE_SEED, SumPolicy);
        intersection
            .update(&TestSketch::of_hashes(&[1, 2, 3]))
            .unwrap();
        intersection
            .update(&TestSketch::of_hashes(&[4, 5]))
            .unwrap();

        let parts = intersection.result(true);
        assert!(parts.entries.is_empty());
        assert!(parts.empty);
        assert_eq!(parts.theta, MAX_THETA);
    }
}
