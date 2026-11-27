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
use crate::thetafamily::common::EntrySketch;
use crate::thetafamily::common::KeySketch;
use crate::thetafamily::common::SketchEntry;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::fold_hash::FoldHashState;
use crate::thetafamily::common::sketch_state::CompactSketchState;
use crate::thetafamily::common::sketch_state::ThetaFamilySketchMetadata;

/// Computes `a and not b` for Theta-family sketch views.
///
/// Ordinary Theta entries only contain a hash, while tuple entries also carry a summary.
/// Surviving entries are moved from `A` unchanged, and `B` contributes only hashes, so unlike
/// the union and intersection this operation needs neither matching entry types nor an
/// entry-merge policy.
pub fn compute<A, B>(
    seed_hash: u16,
    a: A,
    b: B,
    ordered: bool,
) -> Result<CompactSketchState<A::Entry>, Error>
where
    A: EntrySketch,
    B: KeySketch,
{
    // If A is empty the result is empty. As with the union and intersection, an empty input
    // carries no keys, so its seed is not validated and the result uses this operator's seed hash.
    let (a_seed_hash, a_theta, a_ordered) = match a.metadata() {
        ThetaFamilySketchMetadata::Empty => {
            return Ok(CompactSketchState::empty(seed_hash));
        }
        ThetaFamilySketchMetadata::NonEmpty {
            seed_hash,
            theta,
            ordered,
            ..
        } => (seed_hash, theta, ordered),
    };

    // A is non-empty, so its seed must be compatible.
    check_seed_hash(seed_hash, a_seed_hash, "A", ErrorKind::InvalidArgument)?;

    // An empty B subtracts nothing, so the result is simply a copy of A. This also covers the
    // "A is non-empty but has no retained keys" state: B's seed and theta must not influence
    // the result.
    let (b_seed_hash, b_theta, b_ordered, b_num_retained) = match b.metadata() {
        ThetaFamilySketchMetadata::Empty => {
            return Ok(CompactSketchState::compact(
                a.entries().collect(),
                a_theta,
                a_seed_hash,
                a_ordered,
                ordered,
            ));
        }
        ThetaFamilySketchMetadata::NonEmpty {
            seed_hash,
            theta,
            ordered,
            num_retained,
        } => (seed_hash, theta, ordered, num_retained),
    };

    // B is non-empty, so its seed must be compatible.
    check_seed_hash(seed_hash, b_seed_hash, "B", ErrorKind::InvalidArgument)?;

    let theta = a_theta.min(b_theta);

    let entries: Vec<A::Entry> = if b_num_retained == 0 {
        retain_not_in_b(a, theta, a_ordered, |_| false)
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
    } else if let Some(b_table) = b.hash_table() {
        // Mutable B sketches already own an open-addressed hash table populated through
        // MurmurHash3. Probing it directly avoids scanning B or allocating a temporary set; keys
        // in B at or above theta are harmless because A entries are screened by theta first.
        retain_not_in_b(a, theta, a_ordered, |hash| b_table.entry(hash).is_some())
    } else {
        let mut b_keys: HashSet<u64, FoldHashState> =
            HashSet::with_capacity_and_hasher(b_num_retained, FoldHashState::new());
        for hash in b.hashes() {
            if hash < theta {
                b_keys.insert(hash);
            } else if b_ordered {
                break;
            }
        }
        retain_not_in_b(a, theta, a_ordered, |hash| b_keys.contains(&hash))
    };

    if entries.is_empty() && theta == MAX_THETA {
        return Ok(CompactSketchState::empty(seed_hash));
    }

    Ok(CompactSketchState::compact(
        entries, theta, seed_hash, a_ordered, ordered,
    ))
}

fn retain_not_in_b<A>(
    a: A,
    theta: u64,
    a_ordered: bool,
    b_contains: impl Fn(u64) -> bool,
) -> Vec<A::Entry>
where
    A: EntrySketch,
{
    let mut entries = vec![];
    for entry in a.entries() {
        let hash = entry.hash();
        if hash < theta {
            if !b_contains(hash) {
                entries.push(entry);
            }
        } else if a_ordered {
            break;
        }
    }
    entries
}
