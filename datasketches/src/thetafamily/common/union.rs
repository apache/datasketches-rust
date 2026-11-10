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
use crate::thetacommon::EntrySketch;
use crate::thetacommon::SketchEntry;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::SketchHashTable;
use crate::thetacommon::sketch_state::CompactSketchState;
use crate::thetacommon::sketch_state::ThetaFamilySketchMetadata;

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
    // None until the union receives a non-empty input sketch.
    result_theta: Option<u64>,
}

impl<E, P> UnionState<E, P>
where
    E: SketchEntry,
{
    pub fn new(
        lg_k: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
        policy: P,
    ) -> Result<Self, Error> {
        let table = SketchHashTable::new(lg_k, resize_factor, sampling_probability, seed)?;
        Ok(Self {
            result_theta: None,
            table,
            policy,
        })
    }

    /// Incorporate a sketch into the union.
    pub fn update<S>(&mut self, sketch: S) -> Result<(), Error>
    where
        S: EntrySketch<Entry = E>,
        P: UnionMergePolicy<E>,
    {
        let ThetaFamilySketchMetadata::NonEmpty {
            seed_hash,
            theta,
            ordered,
            ..
        } = sketch.metadata()
        else {
            return Ok(());
        };

        check_seed_hash(
            self.table.seed_hash(),
            seed_hash,
            "union update",
            ErrorKind::InvalidArgument,
        )?;

        let current_theta = self.result_theta.unwrap_or(self.table.retention_theta());
        let result_theta = current_theta.min(theta);
        self.result_theta = Some(result_theta);

        for entry in sketch.entries() {
            let hash = entry.hash();
            if hash < result_theta && hash < self.table.retention_theta() {
                self.table.upsert_entry(hash, |existing| match existing {
                    Some(existing) => {
                        self.policy.merge(existing, entry);
                        None
                    }
                    None => Some(entry),
                });
            } else if ordered {
                break;
            }
        }
        self.result_theta = Some(result_theta.min(self.table.retention_theta()));

        Ok(())
    }

    /// Returns the union as canonical compact-sketch state.
    pub fn to_compact_sketch_state(&self, ordered: bool) -> CompactSketchState<E>
    where
        E: Clone,
    {
        let Some(theta) = self.result_theta else {
            return CompactSketchState::empty(self.table.seed_hash());
        };
        let entries = self.retained_entries(theta).cloned().collect();
        self.finish_compaction(entries, theta, ordered)
    }

    /// Returns the union as canonical compact-sketch state that borrows the retained entries.
    #[cfg(feature = "tuple")]
    #[inline]
    pub fn to_borrowed_compact_sketch_state(&self, ordered: bool) -> CompactSketchState<&E> {
        let Some(theta) = self.result_theta else {
            return CompactSketchState::empty(self.table.seed_hash());
        };
        let entries = self.retained_entries(theta).collect::<Vec<_>>();
        if !ordered || entries.len() < 256 {
            return self.finish_compaction(entries, theta, ordered);
        }

        let mut state = self.finish_compaction(entries, theta, false);
        if let CompactSketchState::NonEmpty {
            retained_entries,
            theta,
            ordered,
            ..
        } = &mut state
        {
            sort_borrowed_entries(retained_entries, *theta);
            *ordered = true;
        }
        state
    }

    fn retained_entries(&self, theta: u64) -> impl Iterator<Item = &E> {
        let all_retained = theta >= self.table.retention_theta();
        self.table
            .iter_entries()
            .filter(move |entry| all_retained || entry.hash() < theta)
    }

    #[inline]
    fn finish_compaction<T: SketchEntry>(
        &self,
        mut retained_entries: Vec<T>,
        result_theta: u64,
        ordered: bool,
    ) -> CompactSketchState<T> {
        let mut theta = result_theta.min(self.table.retention_theta());
        let nominal_num = 1usize << self.table.lg_nom_size();
        if retained_entries.len() > nominal_num {
            let (_, kth, _) =
                retained_entries.select_nth_unstable_by_key(nominal_num, |entry| entry.hash());
            theta = kth.hash();
            retained_entries.truncate(nominal_num);
        }

        let ordered = ordered || (retained_entries.len() == 1 && theta == MAX_THETA);
        if ordered {
            retained_entries.sort_unstable_by_key(SketchEntry::hash);
        }

        CompactSketchState::non_empty(retained_entries, theta, self.table.seed_hash(), ordered)
    }

    /// Reset the union to its initial state.
    pub fn reset(&mut self) {
        self.table.reset();
        self.result_theta = None;
    }

    /// Returns an estimate of memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<P>() + self.table.estimated_size()
    }
}

#[cfg(feature = "tuple")]
fn sort_borrowed_entries<E: SketchEntry>(entries: &mut [&E], theta: u64) {
    // Partition by the top eight active hash bits before comparison sorting. Hashes are
    // distributed below theta, so scaling the buckets to theta also works in estimation mode.
    // Small buckets reduce comparisons that dereference entries in the original hash table.
    let shift = (u64::BITS - theta.leading_zeros()).saturating_sub(8);
    let mut offsets = [0usize; 256];
    for entry in entries.iter() {
        offsets[(entry.hash() >> shift) as usize] += 1;
    }
    let mut total = 0;
    for offset in &mut offsets {
        let count = *offset;
        *offset = total;
        total += count;
    }
    let mut scratch = entries.to_vec();
    for &entry in entries.iter() {
        let bucket = (entry.hash() >> shift) as usize;
        scratch[offsets[bucket]] = entry;
        offsets[bucket] += 1;
    }
    let mut start = 0;
    for end in offsets {
        scratch[start..end].sort_unstable_by_key(SketchEntry::hash);
        start = end;
    }
    entries.copy_from_slice(&scratch);
}

#[cfg(all(test, feature = "tuple"))]
mod tests {
    use super::sort_borrowed_entries;
    use crate::thetacommon::constants::MAX_THETA;

    #[test]
    fn borrowed_ordering_matches_hash_order_for_spread_and_clustered_entries() {
        for len in [255, 256, 257, 4096] {
            for theta in [8192, 1 << 32, MAX_THETA] {
                for spacing in [1, theta / (len as u64 + 1)] {
                    let hashes = (1..=len as u64)
                        .map(|value| value * spacing)
                        .collect::<Vec<_>>();
                    let mut entries = hashes.iter().rev().collect::<Vec<_>>();
                    entries.rotate_left(len / 3);

                    sort_borrowed_entries(&mut entries, theta);

                    assert!(entries.iter().copied().eq(hashes.iter()));
                }
            }
        }
    }
}
