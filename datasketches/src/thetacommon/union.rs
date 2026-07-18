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
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::RawHashTable;
use crate::thetacommon::hash_table::RawHashTableEntry;
use crate::thetacommon::sketch_view::RawThetaSketchView;

/// Merges an incoming entry into an existing entry with the same hash.
pub(crate) trait RawThetaUnionPolicy<E> {
    fn merge(&self, existing: &mut E, incoming: E);
}

/// Generic state machine shared by Theta and Tuple unions.
///
/// `E` is the retained entry type. Ordinary Theta entries only contain a hash, while tuple
/// entries also carry a summary. `P` defines how equal-hash entries are combined.
#[derive(Debug)]
pub(crate) struct RawThetaUnion<E, P> {
    table: RawHashTable<E>,
    policy: P,
    union_theta: u64,
}

/// Raw compact-union state from which a sketch family creates its compact result type.
#[derive(Debug)]
pub(crate) struct RawThetaUnionResult<E> {
    pub(crate) entries: Vec<E>,
    pub(crate) theta: u64,
    pub(crate) seed_hash: u16,
    pub(crate) ordered: bool,
    pub(crate) empty: bool,
}

impl<E, P> RawThetaUnion<E, P>
where
    E: RawHashTableEntry,
{
    pub(crate) fn new(
        lg_k: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
        policy: P,
    ) -> Self {
        let table = RawHashTable::new(lg_k, resize_factor, sampling_probability, seed);
        Self {
            union_theta: table.theta(),
            table,
            policy,
        }
    }

    /// Incorporate a sketch into the union.
    pub(crate) fn update<S>(&mut self, sketch: &S) -> Result<(), Error>
    where
        S: RawThetaSketchView<E>,
        P: RawThetaUnionPolicy<E>,
    {
        if sketch.is_empty() {
            return Ok(());
        }

        if self.table.seed_hash() != sketch.seed_hash() {
            return Err(Error::invalid_argument(format!(
                "incompatible seed hash: expected {}, got {}",
                self.table.seed_hash(),
                sketch.seed_hash(),
            )));
        }

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

    /// Return the current compact-union state.
    pub(crate) fn result(&self, ordered: bool) -> RawThetaUnionResult<E>
    where
        E: Clone,
    {
        if self.table.is_empty() {
            return RawThetaUnionResult {
                entries: Vec::new(),
                theta: self.union_theta,
                seed_hash: self.table.seed_hash(),
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
            entries.sort_unstable_by_key(RawHashTableEntry::hash);
        }

        RawThetaUnionResult {
            entries,
            theta,
            seed_hash: self.table.seed_hash(),
            ordered,
            empty: false,
        }
    }

    /// Reset the union to its initial state.
    pub(crate) fn reset(&mut self) {
        self.table.reset();
        self.union_theta = self.table.theta();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::DEFAULT_UPDATE_SEED;
    use crate::thetacommon::sketch_view::RawThetaSketchView;

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

    impl crate::thetacommon::sketch_view::private::Sealed for TestSketch {}

    impl RawThetaSketchView<TestEntry> for TestSketch {
        fn seed_hash(&self) -> u16 {
            crate::hash::compute_seed_hash(DEFAULT_UPDATE_SEED)
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

        fn iter(&self) -> impl Iterator<Item = TestEntry> + '_ {
            self.entries.iter().cloned()
        }

        fn num_retained(&self) -> usize {
            self.entries.len()
        }
    }

    struct SumPolicy;

    impl RawThetaUnionPolicy<TestEntry> for SumPolicy {
        fn merge(&self, existing: &mut TestEntry, incoming: TestEntry) {
            existing.summary += incoming.summary;
        }
    }

    #[test]
    fn merges_equal_hash_entries_with_policy() {
        let mut union =
            RawThetaUnion::new(5, ResizeFactor::X1, 1.0, DEFAULT_UPDATE_SEED, SumPolicy);
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

        assert_eq!(
            union.result(true).entries,
            vec![TestEntry {
                hash: 1,
                summary: 5,
            }]
        );
    }
}
