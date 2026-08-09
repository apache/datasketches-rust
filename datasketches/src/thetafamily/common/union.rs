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
use crate::thetacommon::ThetaFamilySketchView;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::CompactSketchParts;
use crate::thetacommon::hash_table::SketchHashTable;

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
    union_theta: u64,
}

impl<E, P> UnionState<E, P>
where
    E: RetainedEntry,
{
    pub fn new(
        lg_k: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
        policy: P,
    ) -> Self {
        let table = SketchHashTable::new(lg_k, resize_factor, sampling_probability, seed);
        Self {
            union_theta: table.theta(),
            table,
            policy,
        }
    }

    /// Incorporate a sketch into the union.
    pub fn update<S>(&mut self, sketch: &S) -> Result<(), Error>
    where
        S: ThetaFamilySketchView<Entry = E>,
        P: UnionMergePolicy<E>,
    {
        if sketch.is_empty() {
            return Ok(());
        }

        check_seed_hash(
            self.table.seed_hash(),
            sketch.seed_hash(),
            "union update",
            ErrorKind::InvalidArgument,
        )?;

        self.table.set_empty(false);
        self.union_theta = self.union_theta.min(sketch.theta64());

        for entry in sketch.iter() {
            let hash = entry.hash();
            if hash < self.union_theta && hash < self.table.theta() {
                self.table.upsert_entry(hash, |existing| match existing {
                    Some(existing) => {
                        self.policy.merge(existing, entry);
                        None
                    }
                    None => Some(entry),
                });
            } else if sketch.is_ordered() {
                break;
            }
        }
        self.union_theta = self.union_theta.min(self.table.theta());

        Ok(())
    }

    /// Return the current compact-union state as compact-sketch parts.
    pub fn to_compact_parts(&self, ordered: bool) -> CompactSketchParts<E>
    where
        E: Clone,
    {
        let seed_hash = self.table.seed_hash();

        if self.table.is_empty() {
            return CompactSketchParts {
                entries: vec![],
                theta: self.union_theta,
                seed_hash,
                ordered: true,
                empty: true,
            };
        }

        let mut theta = self.union_theta.min(self.table.theta());
        let mut entries = if self.union_theta >= self.table.theta() {
            self.table.iter_entries().cloned().collect::<Vec<_>>()
        } else {
            self.table
                .iter_entries()
                .filter(|entry| entry.hash() < theta)
                .cloned()
                .collect::<Vec<_>>()
        };

        let nominal_num = 1usize << self.table.lg_nom_size();
        if entries.len() > nominal_num {
            let (_, kth, _) = entries.select_nth_unstable_by_key(nominal_num, |entry| entry.hash());
            theta = kth.hash();
            entries.truncate(nominal_num);
        }

        let ordered = ordered || (entries.len() == 1 && theta == MAX_THETA);
        if ordered {
            entries.sort_unstable_by_key(RetainedEntry::hash);
        }

        CompactSketchParts {
            entries,
            theta,
            seed_hash,
            ordered,
            empty: false,
        }
    }

    /// Reset the union to its initial state.
    pub fn reset(&mut self) {
        self.table.reset();
        self.union_theta = self.table.theta();
    }

    /// Returns the estimated size of the heap allocations in bytes.
    pub fn estimated_size(&self) -> usize {
        self.table.estimated_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::DEFAULT_UPDATE_SEED;
    use crate::hash::compute_seed_hash;
    use crate::thetacommon::ThetaKeySketchView;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestEntry {
        hash: u64,
        summary: u64,
    }

    impl RetainedEntry for TestEntry {
        fn __private(&self, _: crate::thetacommon::sealed::Token) {}

        fn hash(&self) -> u64 {
            self.hash
        }
    }

    struct TestSketch {
        entries: Vec<TestEntry>,
    }

    impl ThetaKeySketchView for TestSketch {
        fn __private(&self, _: crate::thetacommon::sealed::Token) {}

        fn seed_hash(&self) -> u16 {
            compute_seed_hash(DEFAULT_UPDATE_SEED)
        }

        fn theta64(&self) -> u64 {
            MAX_THETA
        }

        fn is_empty(&self) -> bool {
            false
        }

        fn is_ordered(&self) -> bool {
            false
        }

        fn iter_hashes(&self) -> impl Iterator<Item = u64> + '_ {
            self.entries.iter().map(RetainedEntry::hash)
        }

        fn num_retained(&self) -> usize {
            self.entries.len()
        }
    }

    impl ThetaFamilySketchView for TestSketch {
        type Entry = TestEntry;

        fn iter(&self) -> impl Iterator<Item = TestEntry> + '_ {
            self.entries.iter().cloned()
        }
    }

    struct SumPolicy;

    impl UnionMergePolicy<TestEntry> for SumPolicy {
        fn merge(&self, existing: &mut TestEntry, incoming: TestEntry) {
            existing.summary += incoming.summary;
        }
    }

    #[test]
    fn merges_equal_hash_entries_with_policy() {
        let mut union = UnionState::new(5, ResizeFactor::X1, 1.0, DEFAULT_UPDATE_SEED, SumPolicy);
        union
            .update(&TestSketch {
                entries: vec![TestEntry {
                    hash: 1,
                    summary: 2,
                }],
            })
            .unwrap();
        union
            .update(&TestSketch {
                entries: vec![TestEntry {
                    hash: 1,
                    summary: 3,
                }],
            })
            .unwrap();

        let parts = union.to_compact_parts(true);
        assert_eq!(
            parts.entries,
            vec![TestEntry {
                hash: 1,
                summary: 5,
            }]
        );
    }
}
