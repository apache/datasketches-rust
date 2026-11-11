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
use std::hash::BuildHasher;
use std::hash::Hasher;

use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::thetacommon::EntrySketch;
use crate::thetacommon::KeySketch;
use crate::thetacommon::SketchEntry;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::sketch_state::CompactSketchState;
use crate::thetacommon::sketch_state::ThetaFamilySketchMetadata;

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
    // If A is empty the result is an (empty) copy of A. As with the union and intersection, an
    // empty input carries no keys, so its seed is not validated.
    let (a_seed_hash, a_theta, a_ordered) = match a.metadata() {
        ThetaFamilySketchMetadata::Empty { .. } => {
            return Ok(copy_to_compact_state(a, ordered));
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
        ThetaFamilySketchMetadata::Empty { .. } => {
            return Ok(copy_to_compact_state(a, ordered));
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
        let mut b_keys: HashSet<u64, FoldHashState> =
            HashSet::with_capacity_and_hasher(b_num_retained, FoldHashState::new());
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
        return Ok(CompactSketchState::empty(seed_hash));
    }

    let mut entries = entries;
    if ordered && !a_ordered && entries.len() > 1 {
        entries.sort_unstable_by_key(SketchEntry::hash);
    }
    let out_ordered = ordered || a_ordered || (entries.len() == 1 && theta == MAX_THETA);

    Ok(CompactSketchState::non_empty(
        entries,
        theta,
        seed_hash,
        out_ordered,
    ))
}

fn copy_to_compact_state<S>(sketch: S, ordered: bool) -> CompactSketchState<S::Entry>
where
    S: EntrySketch,
{
    let (seed_hash, theta, input_ordered) = match sketch.metadata() {
        ThetaFamilySketchMetadata::Empty { seed_hash } => {
            return CompactSketchState::empty(seed_hash);
        }
        ThetaFamilySketchMetadata::NonEmpty {
            seed_hash,
            theta,
            ordered,
            ..
        } => (seed_hash, theta, ordered),
    };

    let mut entries: Vec<S::Entry> = sketch.entries().collect();
    if ordered && !input_ordered && entries.len() > 1 {
        entries.sort_unstable_by_key(SketchEntry::hash);
    }
    let out_ordered = ordered || input_ordered || (entries.len() == 1 && theta == MAX_THETA);
    CompactSketchState::non_empty(entries, theta, seed_hash, out_ordered)
}

const FOLD_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;

#[inline]
fn folded_multiply(x: u64, y: u64) -> u64 {
    let product = u128::from(x) * u128::from(y);
    (product as u64) ^ ((product >> 64) as u64)
}

/// Per-operation random state for hashing pre-hashed 64-bit sketch keys.
///
/// Keys in Theta and Tuple sketches are already uniform MurmurHash3 outputs, so SipHash is
/// unnecessary overhead, but compact sketches can be deserialized from untrusted bytes whose raw
/// hashes were chosen to collide. Seeding a 128-bit folded multiply from the thread-local PRNG on
/// each call prevents crafted inputs from forcing worst-case probe chains.
struct FoldHashState(u64);

impl FoldHashState {
    #[inline]
    fn new() -> Self {
        Self(crate::common::random::random_u64())
    }
}

impl BuildHasher for FoldHashState {
    type Hasher = FoldHasher;

    #[inline]
    fn build_hasher(&self) -> FoldHasher {
        FoldHasher(self.0)
    }
}

struct FoldHasher(u64);

impl Hasher for FoldHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = folded_multiply(self.0 ^ u64::from(byte), FOLD_MULTIPLIER);
        }
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = folded_multiply(self.0 ^ value, FOLD_MULTIPLIER);
    }
}
