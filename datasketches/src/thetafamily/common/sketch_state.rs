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

use crate::thetafamily::common::SketchEntry;
use crate::thetafamily::common::constants::MAX_THETA;

/// Observable metadata consumed by shared Theta-family set operations.
///
/// Empty sketches cannot carry a retained count, theta, or ordering claim in this representation.
/// A non-empty sketch may retain zero entries when theta screened every input.
#[derive(Clone, Copy, Debug)]
pub enum ThetaFamilySketchMetadata {
    Empty {
        seed_hash: u16,
    },
    NonEmpty {
        seed_hash: u16,
        theta: u64,
        ordered: bool,
        num_retained: usize,
    },
}

/// Canonical in-memory state for a compact Theta-family sketch.
///
/// The empty variant deliberately has neither retained entries nor theta. This makes the only
/// representable empty state use `MAX_THETA`, report exact mode, and serialize through the
/// canonical empty-image path. The non-empty variant may contain no retained entries after theta
/// screening.
#[derive(Clone, Debug)]
pub enum CompactSketchState<E> {
    Empty {
        seed_hash: u16,
    },
    NonEmpty {
        retained_entries: Vec<E>,
        theta: u64,
        seed_hash: u16,
        ordered: bool,
    },
}

impl<E> CompactSketchState<E> {
    pub fn empty(seed_hash: u16) -> Self {
        Self::Empty { seed_hash }
    }

    pub fn non_empty(retained_entries: Vec<E>, theta: u64, seed_hash: u16, ordered: bool) -> Self {
        Self::NonEmpty {
            retained_entries,
            theta,
            seed_hash,
            ordered,
        }
    }

    /// Canonicalizes and optionally sorts retained entries for a non-empty compact sketch.
    ///
    /// Exact singletons (`len == 1` at `MAX_THETA`) are always marked ordered to match their
    /// single-word canonical encoding. When ordering is requested and `retained_entries` are not
    /// already ordered, entries are sorted ascending by hash.
    pub fn compact(
        mut retained_entries: Vec<E>,
        theta: u64,
        seed_hash: u16,
        already_ordered: bool,
        ordered: bool,
    ) -> Self
    where
        E: SketchEntry,
    {
        let ordered =
            ordered || already_ordered || (retained_entries.len() == 1 && theta == MAX_THETA);
        if ordered && !already_ordered && retained_entries.len() > 1 {
            E::sort_by_hash(&mut retained_entries);
        }
        Self::NonEmpty {
            retained_entries,
            theta,
            seed_hash,
            ordered,
        }
    }

    pub fn theta(&self) -> u64 {
        match self {
            Self::Empty { .. } => MAX_THETA,
            Self::NonEmpty { theta, .. } => *theta,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty { .. })
    }

    pub fn is_estimation_mode(&self) -> bool {
        matches!(self, Self::NonEmpty { theta, .. } if *theta < MAX_THETA)
    }

    /// Returns the preamble size in 8-byte words for uncompressed compact Theta and Tuple images.
    pub fn uncompressed_preamble_longs(&self) -> u8 {
        if self.is_empty() {
            1
        } else if self.is_estimation_mode() {
            3
        } else if self.retained_entries().len() == 1 {
            1
        } else {
            2
        }
    }

    pub fn seed_hash(&self) -> u16 {
        match self {
            Self::Empty { seed_hash } | Self::NonEmpty { seed_hash, .. } => *seed_hash,
        }
    }

    pub fn retained_entries(&self) -> &[E] {
        match self {
            Self::Empty { .. } => &[],
            Self::NonEmpty {
                retained_entries, ..
            } => retained_entries,
        }
    }

    pub fn retained_entries_capacity(&self) -> usize {
        match self {
            Self::Empty { .. } => 0,
            Self::NonEmpty {
                retained_entries, ..
            } => retained_entries.capacity(),
        }
    }

    pub fn is_ordered(&self) -> bool {
        match self {
            Self::Empty { .. } => true,
            Self::NonEmpty { ordered, .. } => *ordered,
        }
    }

    #[cfg(feature = "theta")]
    pub fn map_retained_entries<T>(self, mut f: impl FnMut(E) -> T) -> CompactSketchState<T> {
        match self {
            Self::Empty { seed_hash } => CompactSketchState::empty(seed_hash),
            Self::NonEmpty {
                retained_entries,
                theta,
                seed_hash,
                ordered,
            } => CompactSketchState::non_empty(
                retained_entries.into_iter().map(&mut f).collect(),
                theta,
                seed_hash,
                ordered,
            ),
        }
    }
}

const MIN_BUCKET_SORT_LEN: usize = 256;
const MAX_BUCKET_LEN: u32 = 32;

/// Sorts uniform 64-bit MurmurHash3 entries in ascending hash order using a single top-bit bucket
/// pass followed by a linear insertion pass, falling back to `sort_unstable_by_key` for small or
/// skewed inputs.
fn with_bucket_starts<E: SketchEntry>(
    entries: &mut Vec<E>,
    scatter: impl FnOnce(&mut Vec<E>, &mut [u32], u32, usize),
) {
    let len = entries.len();
    if len < MIN_BUCKET_SORT_LEN {
        if len > 1 {
            entries.sort_unstable_by_key(SketchEntry::hash);
        }
        return;
    }

    // The highest set bit of the bitwise OR equals the highest set bit of the maximum hash,
    // avoiding a branch-dependent max reduction before computing the top-bit bucket shift.
    let bits = entries.iter().fold(0u64, |acc, entry| acc | entry.hash());
    let lg_buckets = len.ilog2();
    let num_buckets = 1usize << lg_buckets;
    let mask = num_buckets - 1;
    let shift = (u64::BITS - bits.leading_zeros()).saturating_sub(lg_buckets);

    let mut heap_starts;
    let mut stack_10;
    let mut stack_12;
    let starts: &mut [u32] = if lg_buckets <= 10 {
        stack_10 = [0u32; (1 << 10) + 1];
        &mut stack_10[..=num_buckets]
    } else if lg_buckets <= 12 {
        stack_12 = [0u32; (1 << 12) + 1];
        &mut stack_12[..=num_buckets]
    } else {
        heap_starts = vec![0u32; num_buckets + 1];
        &mut heap_starts
    };

    for entry in entries.iter() {
        starts[((entry.hash() >> shift) as usize & mask) + 1] += 1;
    }
    let mut largest = 0u32;
    for i in 1..starts.len() {
        largest = largest.max(starts[i]);
        starts[i] += starts[i - 1];
    }
    if largest > MAX_BUCKET_LEN {
        entries.sort_unstable_by_key(SketchEntry::hash);
        return;
    }

    scatter(entries, starts, shift, mask);
}

pub fn sort_copy_by_hash<E: SketchEntry + Copy>(entries: &mut Vec<E>) {
    with_bucket_starts(entries, |entries, starts, shift, mask| {
        let len = entries.len();
        let mut sorted = vec![entries[0]; len];
        for &entry in entries.iter() {
            let next = &mut starts[(entry.hash() >> shift) as usize & mask];
            sorted[*next as usize] = entry;
            *next += 1;
        }
        for i in 1..len {
            let entry = sorted[i];
            let hash = entry.hash();
            let mut j = i;
            while j > 0 && sorted[j - 1].hash() > hash {
                sorted[j] = sorted[j - 1];
                j -= 1;
            }
            sorted[j] = entry;
        }
        *entries = sorted;
    });
}

#[cfg(feature = "tuple")]
use crate::tuple::TupleEntry;

#[cfg(feature = "tuple")]
pub fn sort_niche_by_hash<S>(entries: &mut Vec<TupleEntry<S>>) {
    const {
        if size_of::<TupleEntry<S>>() != size_of::<Option<TupleEntry<S>>>() {
            panic!("niche sort requires Option<TupleEntry> to match TupleEntry in size");
        }
        if align_of::<TupleEntry<S>>() != align_of::<Option<TupleEntry<S>>>() {
            panic!("niche sort requires Option<TupleEntry> to match TupleEntry in alignment");
        }
    }

    with_bucket_starts(entries, |entries, starts, shift, mask| {
        let len = entries.len();
        let mut sorted = std::iter::repeat_with(|| None)
            .take(len)
            .collect::<Vec<_>>();
        for entry in std::mem::take(entries) {
            let next = &mut starts[(entry.hash() >> shift) as usize & mask];
            sorted[*next as usize] = Some(entry);
            *next += 1;
        }
        let mut sorted = sorted
            .into_iter()
            .map(|ent| ent.unwrap())
            .collect::<Vec<_>>();
        for i in 1..len {
            let mut j = i;
            while j > 0 && sorted[j - 1].hash() > sorted[j].hash() {
                sorted.swap(j - 1, j);
                j -= 1;
            }
        }
        *entries = sorted;
    });
}
