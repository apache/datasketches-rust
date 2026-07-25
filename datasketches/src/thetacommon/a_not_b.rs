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
use crate::hash::compute_seed_hash;
use crate::thetacommon::RawHashTableEntry;
use crate::thetacommon::RawThetaSketchView;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::RawCompactParts;

/// Stateless set difference (`A and not B`) operator shared by Theta and Tuple sketches.
///
/// `E` is the retained entry type. Ordinary Theta entries only contain a hash, while tuple
/// entries also carry a summary. Surviving entries are moved from `A` unchanged, so unlike the
/// union and intersection this operation needs no entry-merge policy.
#[derive(Debug, Clone, Copy)]
pub struct RawAnotB {
    seed_hash: u16,
}

impl RawAnotB {
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
    pub fn compute<E, A, B>(&self, a: &A, b: &B, ordered: bool) -> Result<RawCompactParts<E>, Error>
    where
        E: RawHashTableEntry,
        A: RawThetaSketchView<E>,
        B: RawThetaSketchView<E>,
    {
        // If A is empty the result is an (empty) copy of A. As with the union and intersection, an
        // empty input carries no keys, so its seed is not validated.
        if a.is_empty() {
            return Ok(Self::parts_from_view(a, ordered));
        }

        // A is non-empty, so its seed must be compatible.
        if a.seed_hash() != self.seed_hash {
            return Err(Error::invalid_argument(format!(
                "incompatible seed hash for A: expected {}, got {}",
                self.seed_hash,
                a.seed_hash()
            )));
        }

        // An empty B subtracts nothing, so the result is simply a copy of A. This also covers the
        // "A is non-empty but has no retained keys" state: B's seed and theta must not influence
        // the result, so we return before touching them.
        if b.is_empty() {
            return Ok(Self::parts_from_view(a, ordered));
        }

        // B is non-empty, so its seed must be compatible.
        if b.seed_hash() != self.seed_hash {
            return Err(Error::invalid_argument(format!(
                "incompatible seed hash for B: expected {}, got {}",
                self.seed_hash,
                b.seed_hash()
            )));
        }

        let theta = a.theta().min(b.theta());
        // A is non-empty here; the result only becomes empty if everything is subtracted in exact
        // mode (handled below).
        let mut is_empty = false;

        let entries: Vec<E> = if b.num_retained() == 0 {
            a.iter().filter(|entry| entry.hash() < theta).collect()
        } else if a.is_ordered() && b.is_ordered() {
            // Both inputs are sorted ascending by hash: merge-scan without a hash set. Only
            // b hashes below theta can exclude an a entry (a entries are all < theta), so
            // unexamined b entries at or above theta are harmless.
            let mut b_hashes = b.iter().map(|entry| entry.hash()).peekable();
            let mut entries = Vec::new();
            for entry in a.iter() {
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
            let mut b_keys: HashSet<u64> = HashSet::with_capacity(b.num_retained());
            for entry in b.iter() {
                let hash = entry.hash();
                if hash < theta {
                    b_keys.insert(hash);
                } else if b.is_ordered() {
                    break;
                }
            }

            let mut entries = Vec::new();
            for entry in a.iter() {
                let hash = entry.hash();
                if hash < theta {
                    if !b_keys.contains(&hash) {
                        entries.push(entry);
                    }
                } else if a.is_ordered() {
                    break;
                }
            }
            entries
        };

        if entries.is_empty() && theta == MAX_THETA {
            is_empty = true;
        }

        let out_ordered = ordered || a.is_ordered();
        let mut entries = entries;
        if ordered && !a.is_ordered() && entries.len() > 1 {
            entries.sort_unstable_by_key(RawHashTableEntry::hash);
        }

        Ok(RawCompactParts {
            entries,
            theta,
            seed_hash: self.seed_hash,
            ordered: out_ordered,
            empty: is_empty,
        })
    }

    /// Builds compact parts that are a copy of the view `a`.
    fn parts_from_view<E, V>(a: &V, ordered: bool) -> RawCompactParts<E>
    where
        E: RawHashTableEntry,
        V: RawThetaSketchView<E>,
    {
        let mut entries: Vec<E> = a.iter().collect();
        let out_ordered = ordered || a.is_ordered();
        if ordered && !a.is_ordered() && entries.len() > 1 {
            entries.sort_unstable_by_key(RawHashTableEntry::hash);
        }
        RawCompactParts {
            entries,
            theta: a.theta(),
            seed_hash: a.seed_hash(),
            ordered: out_ordered,
            empty: a.is_empty(),
        }
    }
}
