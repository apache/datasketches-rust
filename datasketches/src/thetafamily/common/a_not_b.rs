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
use crate::thetacommon::EntrySketch;
use crate::thetacommon::KeySketch;
use crate::thetacommon::SketchEntry;
use crate::thetacommon::sketch_state::CompactSketchState;
use crate::thetacommon::sketch_state::ThetaThreshold;

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
    let a_metadata = a.metadata();

    // If A is empty the result is an (empty) copy of A. As with the union and intersection, an
    // empty input carries no keys, so its seed is not validated.
    if a_metadata.is_empty() {
        return Ok(compact_state_from_sketch(a, ordered));
    }

    // A is non-empty, so its seed must be compatible.
    check_seed_hash(
        seed_hash,
        a_metadata.seed_hash(),
        "A",
        ErrorKind::InvalidArgument,
    )?;

    let b_metadata = b.metadata();

    // An empty B subtracts nothing, so the result is simply a copy of A. This also covers the
    // "A is non-empty but has no retained keys" state: B's seed and theta must not influence
    // the result.
    if b_metadata.is_empty() {
        return Ok(compact_state_from_sketch(a, ordered));
    }

    // B is non-empty, so its seed must be compatible.
    check_seed_hash(
        seed_hash,
        b_metadata.seed_hash(),
        "B",
        ErrorKind::InvalidArgument,
    )?;

    let theta = a_metadata.theta().min(b_metadata.theta());

    let entries: Vec<A::Entry> = if b_metadata.num_retained() == 0 {
        a.entries()
            .filter(|entry| entry.hash() < theta.get())
            .collect()
    } else if a_metadata.is_ordered() && b_metadata.is_ordered() {
        // Both inputs are sorted ascending by hash: merge-scan without a hash set. Only
        // B hashes below theta can exclude an A entry (A entries are all < theta), so
        // unexamined B entries at or above theta are harmless.
        let mut b_hashes = b.hashes().peekable();
        let mut entries = vec![];
        for entry in a.entries() {
            let hash = entry.hash();
            if hash >= theta.get() {
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
        let mut b_keys: HashSet<u64> = HashSet::with_capacity(b_metadata.num_retained());
        for hash in b.hashes() {
            if hash < theta.get() {
                b_keys.insert(hash);
            } else if b_metadata.is_ordered() {
                break;
            }
        }

        let mut entries = vec![];
        for entry in a.entries() {
            let hash = entry.hash();
            if hash < theta.get() {
                if !b_keys.contains(&hash) {
                    entries.push(entry);
                }
            } else if a_metadata.is_ordered() {
                break;
            }
        }
        entries
    };

    if entries.is_empty() && theta == ThetaThreshold::MAX {
        return Ok(CompactSketchState::empty(seed_hash));
    }

    let mut entries = entries;
    if ordered && !a_metadata.is_ordered() && entries.len() > 1 {
        entries.sort_unstable_by_key(SketchEntry::hash);
    }
    let out_ordered =
        ordered || a_metadata.is_ordered() || (entries.len() == 1 && theta == ThetaThreshold::MAX);

    Ok(CompactSketchState::non_empty(
        entries,
        theta,
        seed_hash,
        out_ordered,
    ))
}

fn compact_state_from_sketch<S>(sketch: S, ordered: bool) -> CompactSketchState<S::Entry>
where
    S: EntrySketch,
{
    let metadata = sketch.metadata();
    if metadata.is_empty() {
        return CompactSketchState::empty(metadata.seed_hash());
    }

    let mut entries: Vec<S::Entry> = sketch.entries().collect();
    if ordered && !metadata.is_ordered() && entries.len() > 1 {
        entries.sort_unstable_by_key(SketchEntry::hash);
    }
    let theta = metadata.theta();
    let out_ordered =
        ordered || metadata.is_ordered() || (entries.len() == 1 && theta == ThetaThreshold::MAX);
    CompactSketchState::non_empty(entries, theta, metadata.seed_hash(), out_ordered)
}
