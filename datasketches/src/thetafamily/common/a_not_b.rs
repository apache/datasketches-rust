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

use std::collections::HashSet;

use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::thetacommon::OwnedEntrySketchView;
use crate::thetacommon::RetainedEntry;
use crate::thetacommon::SetOpProps;
use crate::thetacommon::SetOperationSketchView;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::CompactSketchParts;

/// Stateless set difference (`A and not B`) operator shared by Theta and Tuple sketches.
///
/// Ordinary Theta entries only contain a hash, while tuple entries also carry a summary.
/// Surviving entries are moved from `A` unchanged, and `B` contributes only hashes, so unlike
/// the union and intersection this operation needs neither matching entry types nor an
/// entry-merge policy.
#[derive(Debug, Clone, Copy)]
pub struct ANotBOperator {
    seed_hash: u16,
}

impl ANotBOperator {
    /// Creates a new set difference operator for the given `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            seed_hash: compute_seed_hash(seed),
        }
    }

    /// Computes `a and not b`.
    ///
    /// The result retains every entry of `a` (below the combined theta) whose hash is not present
    /// in `b`. If `ordered` is true, the retained entries are sorted ascending by hash.
    ///
    /// # Errors
    ///
    /// Returns an error if either non-trivial input has a seed hash that differs from this
    /// operator's seed.
    pub fn compute<A, B>(
        &self,
        a: A,
        b: B,
        ordered: bool,
    ) -> Result<CompactSketchParts<A::Entry>, Error>
    where
        A: OwnedEntrySketchView,
        B: SetOperationSketchView,
    {
        let a_properties = a.properties();
        let b_properties = b.properties();

        // If A is empty the result is an (empty) copy of A. As with the union and intersection, an
        // empty input carries no keys, so its seed is not validated.
        if a_properties.empty {
            return Ok(Self::parts_from_sketch(a, ordered));
        }

        // A is non-empty, so its seed must be compatible.
        check_seed_hash(
            self.seed_hash,
            a_properties.seed_hash,
            "A",
            ErrorKind::InvalidArgument,
        )?;

        // An empty B subtracts nothing, so the result is simply a copy of A. This also covers the
        // "A is non-empty but has no retained keys" state: B's seed and theta must not influence
        // the result, so we return before touching them.
        if b_properties.empty {
            return Ok(Self::parts_from_sketch(a, ordered));
        }

        // B is non-empty, so its seed must be compatible.
        check_seed_hash(
            self.seed_hash,
            b_properties.seed_hash,
            "B",
            ErrorKind::InvalidArgument,
        )?;

        let SetOpProps {
            theta: a_theta,
            ordered: a_ordered,
            ..
        } = a_properties;
        let SetOpProps {
            theta: b_theta,
            ordered: b_ordered,
            num_retained: b_num_retained,
            ..
        } = b_properties;
        let theta = a_theta.min(b_theta);
        // A is non-empty here; the result only becomes empty if everything is subtracted in exact
        // mode (handled below).
        let mut is_empty = false;

        let entries: Vec<A::Entry> = if b_num_retained == 0 {
            a.entries().filter(|entry| entry.hash() < theta).collect()
        } else if a_ordered && b_ordered {
            // Both inputs are sorted ascending by hash: merge-scan without a hash set. Only
            // B hashes below theta can exclude an A entry (A entries are all < theta), so
            // unexamined B entries at or above theta are harmless.
            let mut b_hashes = b.hashes().peekable();
            let mut entries = vec![];
            for entry in a.entries() {
                let hash = entry.hash();
                if hash >= theta {
                    break;
                }
                while let Some(&b_hash) = b_hashes.peek() {
                    if b_hash < hash {
                        b_hashes.next();
                    } else {
                        break;
                    }
                }
                if b_hashes.peek() != Some(&hash) {
                    entries.push(entry);
                }
            }
            entries
        } else {
            let mut b_keys: HashSet<u64> = HashSet::with_capacity(b_num_retained);
            for hash in b.hashes() {
                if hash < theta {
                    b_keys.insert(hash);
                } else if b_ordered {
                    break;
                }
            }

            let mut entries = vec![];
            for entry in a.entries() {
                let hash = entry.hash();
                if hash < theta {
                    if !b_keys.contains(&hash) {
                        entries.push(entry);
                    }
                } else if a_ordered {
                    break;
                }
            }
            entries
        };

        if entries.is_empty() && theta == MAX_THETA {
            is_empty = true;
        }

        let out_ordered = ordered || a_ordered;
        let mut entries = entries;
        if ordered && !a_ordered && entries.len() > 1 {
            entries.sort_unstable_by_key(RetainedEntry::hash);
        }

        Ok(CompactSketchParts {
            entries,
            theta,
            seed_hash: self.seed_hash,
            ordered: out_ordered,
            empty: is_empty,
        })
    }

    /// Builds compact parts that are a copy of the view `a`.
    fn parts_from_sketch<S>(sketch: S, ordered: bool) -> CompactSketchParts<S::Entry>
    where
        S: OwnedEntrySketchView,
    {
        let properties = sketch.properties();
        let mut entries: Vec<S::Entry> = sketch.entries().collect();
        let out_ordered = ordered || properties.ordered;
        if ordered && !properties.ordered && entries.len() > 1 {
            entries.sort_unstable_by_key(RetainedEntry::hash);
        }
        CompactSketchParts {
            entries,
            theta: properties.theta,
            seed_hash: properties.seed_hash,
            ordered: out_ordered,
            empty: properties.empty,
        }
    }
}
