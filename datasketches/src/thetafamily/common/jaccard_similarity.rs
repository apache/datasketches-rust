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

use std::cmp::Ordering;

use crate::common::JaccardSimilarity;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::thetafamily::common::EntrySketch;
use crate::thetafamily::common::KeySketch;
use crate::thetafamily::common::SketchEntry;
use crate::thetafamily::common::binomial_bounds;
use crate::thetafamily::common::constants::MAX_LG_K;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::constants::MIN_LG_K;
use crate::thetafamily::common::hash_table::SketchHashTable;
use crate::thetafamily::common::sketch_state::ThetaFamilySketchMetadata;
use crate::thetafamily::common::sketch_state::sort_copy_by_hash;
use crate::thetafamily::common::union::UnionMergePolicy;
use crate::thetafamily::common::union::UnionState;

const NUM_STD_DEVS: f64 = 2.0;
const MAX_NOMINAL_ENTRIES: usize = 1usize << MAX_LG_K;
const SOURCE_A: u8 = 1 << 0;
const SOURCE_B: u8 = 1 << 1;
const SOURCE_BOTH: u8 = SOURCE_A | SOURCE_B;

#[derive(Clone, Copy, Debug)]
struct ProvenanceEntry {
    hash: u64,
    sources: u8,
}

impl SketchEntry for ProvenanceEntry {
    #[inline]
    fn hash(&self) -> u64 {
        self.hash
    }

    fn sort_by_hash(entries: &mut Vec<Self>) {
        sort_copy_by_hash(entries);
    }
}

#[derive(Clone, Copy, Debug)]
struct ProvenanceMergePolicy;

impl UnionMergePolicy<ProvenanceEntry> for ProvenanceMergePolicy {
    #[inline]
    fn merge(&self, existing: &mut ProvenanceEntry, incoming: ProvenanceEntry) {
        existing.sources |= incoming.sources;
    }
}

#[derive(Clone, Copy, Debug)]
struct TaggedEntries<S> {
    sketch: S,
    source: u8,
}

impl<S> KeySketch for TaggedEntries<S>
where
    S: KeySketch,
{
    fn metadata(self) -> ThetaFamilySketchMetadata {
        self.sketch.metadata()
    }

    fn hashes(self) -> impl Iterator<Item = u64> {
        self.sketch.hashes()
    }

    #[inline]
    fn hash_table(&self) -> Option<&SketchHashTable<impl SketchEntry>> {
        self.sketch.hash_table()
    }
}

impl<S> EntrySketch for TaggedEntries<S>
where
    S: KeySketch,
{
    type Entry = ProvenanceEntry;

    fn entries(self) -> impl Iterator<Item = Self::Entry> {
        let source = self.source;
        self.sketch.hashes().map(move |hash| ProvenanceEntry {
            hash,
            sources: source,
        })
    }
}

#[derive(Clone, Copy)]
struct NonEmptyMeta {
    seed_hash: u16,
    theta: u64,
    ordered: bool,
    num_retained: usize,
}

pub fn compute<A, B>(seed: u64, sketch_a: A, sketch_b: B) -> Result<JaccardSimilarity, Error>
where
    A: KeySketch,
    B: KeySketch,
{
    let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
    let (a, b) = match (
        non_empty_metadata(sketch_a.metadata()),
        non_empty_metadata(sketch_b.metadata()),
    ) {
        (None, None) => return Ok(JaccardSimilarity::exact(1.0)),
        (None, _) | (_, None) => return Ok(JaccardSimilarity::exact(0.0)),
        (Some(a), Some(b)) => (a, b),
    };
    check_seed_hash(seed_hash, a.seed_hash, "A", ErrorKind::InvalidArgument)?;
    check_seed_hash(seed_hash, b.seed_hash, "B", ErrorKind::InvalidArgument)?;

    let (union_theta, union_count, intersection_count) =
        compute_counts(seed, sketch_a, a, sketch_b, b)?;

    if identical_sets(a, b, union_theta, union_count, intersection_count) {
        return Ok(JaccardSimilarity::exact(1.0));
    }
    if union_count == 0 {
        return Ok(JaccardSimilarity::new(0.0, 0.5, 1.0));
    }

    let estimate = intersection_count as f64 / union_count as f64;
    if union_theta == MAX_THETA {
        return Ok(JaccardSimilarity::exact(estimate));
    }

    let sampling_probability = union_theta as f64 / MAX_THETA as f64;
    let adjustment = NUM_STD_DEVS * sampling_adjuster(sampling_probability);
    Ok(JaccardSimilarity::new(
        binomial_bounds::approximate_lower_bound_on_p(union_count, intersection_count, adjustment)?,
        estimate,
        binomial_bounds::approximate_upper_bound_on_p(union_count, intersection_count, adjustment)?,
    ))
}

pub fn exactly_equal<A, B>(seed: u64, sketch_a: A, sketch_b: B) -> Result<bool, Error>
where
    A: KeySketch,
    B: KeySketch,
{
    let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
    let (a, b) = match (
        non_empty_metadata(sketch_a.metadata()),
        non_empty_metadata(sketch_b.metadata()),
    ) {
        (None, None) => return Ok(true),
        (None, _) | (_, None) => return Ok(false),
        (Some(a), Some(b)) => (a, b),
    };
    check_seed_hash(seed_hash, a.seed_hash, "A", ErrorKind::InvalidArgument)?;
    check_seed_hash(seed_hash, b.seed_hash, "B", ErrorKind::InvalidArgument)?;

    if a.theta != b.theta || a.num_retained != b.num_retained {
        return Ok(false);
    }
    if a.num_retained == 0 {
        return Ok(true);
    }
    match (sketch_a.hash_table(), sketch_b.hash_table()) {
        (Some(a_table), Some(b_table)) => {
            return Ok(a_table
                .iter_entries()
                .all(|entry| b_table.entry(entry.hash()).is_some()));
        }
        (Some(a_table), None) if b.ordered => {
            if let Some(equal) = try_equal_table_and_ordered(a_table, sketch_b.hashes()) {
                return Ok(equal);
            }
        }
        (None, Some(b_table)) if a.ordered => {
            if let Some(equal) = try_equal_table_and_ordered(b_table, sketch_a.hashes()) {
                return Ok(equal);
            }
        }
        (None, None) if a.ordered && b.ordered => {
            return Ok(sketch_a.hashes().eq(sketch_b.hashes()));
        }
        _ => {}
    }

    let (union_theta, union_count, intersection_count) =
        counts_from_union(seed, sketch_a, a, sketch_b, b)?;
    Ok(identical_sets(
        a,
        b,
        union_theta,
        union_count,
        intersection_count,
    ))
}

fn compute_counts<A, B>(
    seed: u64,
    sketch_a: A,
    a: NonEmptyMeta,
    sketch_b: B,
    b: NonEmptyMeta,
) -> Result<(u64, u64, u64), Error>
where
    A: KeySketch,
    B: KeySketch,
{
    let union_theta = a.theta.min(b.theta);
    if a.num_retained == 0 && b.num_retained == 0 {
        return Ok((union_theta, 0, 0));
    }

    // When combined retained entries cannot exceed the maximum nominal capacity 2^26, the union
    // never rebuilds or truncates, so `union_theta` is `min(a.theta, b.theta)` and duplicate-free
    // inputs can be counted directly without allocating a temporary union or intersection table.
    if a.num_retained.saturating_add(b.num_retained) <= MAX_NOMINAL_ENTRIES {
        match (sketch_a.hash_table(), sketch_b.hash_table()) {
            (Some(a_table), Some(b_table)) => {
                let (union_count, intersection_count) =
                    counts_from_hash_tables(a_table, a.theta, b_table, b.theta, union_theta);
                return Ok((union_theta, union_count, intersection_count));
            }
            (Some(a_table), None) if b.ordered => {
                if let Some((union_count, intersection_count)) =
                    try_counts_table_and_ordered(a_table, a.theta, sketch_b.hashes(), union_theta)
                {
                    return Ok((union_theta, union_count, intersection_count));
                }
            }
            (None, Some(b_table)) if a.ordered => {
                if let Some((union_count, intersection_count)) =
                    try_counts_table_and_ordered(b_table, b.theta, sketch_a.hashes(), union_theta)
                {
                    return Ok((union_theta, union_count, intersection_count));
                }
            }
            (None, None) if a.ordered && b.ordered => {
                let (union_count, intersection_count) =
                    counts_from_ordered(sketch_a.hashes(), sketch_b.hashes(), union_theta);
                return Ok((union_theta, union_count, intersection_count));
            }
            _ => {}
        }
    }

    counts_from_union(seed, sketch_a, a, sketch_b, b)
}

fn counts_from_hash_tables<EA, EB>(
    a_table: &SketchHashTable<EA>,
    a_theta: u64,
    b_table: &SketchHashTable<EB>,
    b_theta: u64,
    union_theta: u64,
) -> (u64, u64)
where
    EA: SketchEntry,
    EB: SketchEntry,
{
    if a_theta < b_theta || (a_theta == b_theta && a_table.num_retained() >= b_table.num_retained())
    {
        scan_table_against_indexed(b_table, a_table, union_theta)
    } else {
        scan_table_against_indexed(a_table, b_table, union_theta)
    }
}

fn scan_table_against_indexed<ES, EI>(
    scanned: &SketchHashTable<ES>,
    indexed: &SketchHashTable<EI>,
    union_theta: u64,
) -> (u64, u64)
where
    ES: SketchEntry,
    EI: SketchEntry,
{
    debug_assert_eq!(indexed.retention_theta(), union_theta);
    let indexed_count = indexed.num_retained() as u64;
    let mut scanned_only_count = 0u64;
    let mut intersection_count = 0u64;
    for entry in scanned.iter_entries() {
        let hash = entry.hash();
        if hash < union_theta {
            if indexed.entry(hash).is_some() {
                intersection_count += 1;
            } else {
                scanned_only_count += 1;
            }
        }
    }
    (indexed_count + scanned_only_count, intersection_count)
}

fn try_counts_table_and_ordered<E, I>(
    table: &SketchHashTable<E>,
    table_theta: u64,
    ordered_hashes: I,
    union_theta: u64,
) -> Option<(u64, u64)>
where
    E: SketchEntry,
    I: Iterator<Item = u64>,
{
    let mut prev = None;
    let mut ordered_only_count = 0u64;
    let mut intersection_count = 0u64;
    for hash in ordered_hashes {
        if hash >= union_theta {
            break;
        }
        // Guard against duplicate hashes in malformed deserialized images marked ordered, which
        // would otherwise inflate `intersection_count` past `union_count`.
        if prev.replace(hash).is_some_and(|p| hash <= p) {
            return None;
        }
        if table.entry(hash).is_some() {
            intersection_count += 1;
        } else {
            ordered_only_count += 1;
        }
    }
    let table_count = if table_theta <= union_theta {
        table.num_retained() as u64
    } else {
        table
            .iter_entries()
            .filter(|entry| entry.hash() < union_theta)
            .count() as u64
    };
    Some((table_count + ordered_only_count, intersection_count))
}

fn try_equal_table_and_ordered<E, I>(table: &SketchHashTable<E>, ordered_hashes: I) -> Option<bool>
where
    E: SketchEntry,
    I: Iterator<Item = u64>,
{
    let mut prev = None;
    for hash in ordered_hashes {
        if prev.replace(hash).is_some_and(|p| hash <= p) {
            return None;
        }
        if table.entry(hash).is_none() {
            return Some(false);
        }
    }
    Some(true)
}

fn counts_from_ordered<A, B>(a_hashes: A, b_hashes: B, union_theta: u64) -> (u64, u64)
where
    A: Iterator<Item = u64>,
    B: Iterator<Item = u64>,
{
    let mut a = a_hashes.take_while(|&hash| hash < union_theta).peekable();
    let mut b = b_hashes.take_while(|&hash| hash < union_theta).peekable();
    let mut union_count = 0u64;
    let mut intersection_count = 0u64;

    loop {
        match (a.peek().copied(), b.peek().copied()) {
            (Some(ha), Some(hb)) => match ha.cmp(&hb) {
                Ordering::Less => {
                    a.next();
                    union_count += 1;
                }
                Ordering::Greater => {
                    b.next();
                    union_count += 1;
                }
                Ordering::Equal => {
                    a.next();
                    b.next();
                    union_count += 1;
                    intersection_count += 1;
                }
            },
            (Some(_), None) => {
                union_count += a.count() as u64;
                break;
            }
            (None, Some(_)) => {
                union_count += b.count() as u64;
                break;
            }
            (None, None) => break,
        }
    }

    (union_count, intersection_count)
}

fn counts_from_union<A, B>(
    seed: u64,
    sketch_a: A,
    a: NonEmptyMeta,
    sketch_b: B,
    b: NonEmptyMeta,
) -> Result<(u64, u64, u64), Error>
where
    A: KeySketch,
    B: KeySketch,
{
    let mut union = UnionState::new(
        union_lg_k(a.num_retained, b.num_retained),
        ResizeFactor::X8,
        1.0,
        seed,
        ProvenanceMergePolicy,
    )?;
    // Insert the lower-theta sketch first so `UnionState` immediately establishes
    // `min(a.theta, b.theta)` and screens higher-hash entries from the other sketch up front.
    if a.theta <= b.theta {
        union.update(TaggedEntries {
            sketch: sketch_a,
            source: SOURCE_A,
        })?;
        union.update(TaggedEntries {
            sketch: sketch_b,
            source: SOURCE_B,
        })?;
    } else {
        union.update(TaggedEntries {
            sketch: sketch_b,
            source: SOURCE_B,
        })?;
        union.update(TaggedEntries {
            sketch: sketch_a,
            source: SOURCE_A,
        })?;
    }
    let (union_theta, (union_count, intersection_count)) =
        union.fold_retained((0u64, 0u64), |(u_count, i_count), entry| {
            (
                u_count + 1,
                i_count + u64::from(entry.sources == SOURCE_BOTH),
            )
        });
    Ok((union_theta, union_count, intersection_count))
}

/// Returns whether both sketches have the same retained keys and theta.
fn identical_sets(
    a: NonEmptyMeta,
    b: NonEmptyMeta,
    union_theta: u64,
    union_count: u64,
    intersection_count: u64,
) -> bool {
    union_theta == a.theta
        && union_theta == b.theta
        && union_count == a.num_retained as u64
        && union_count == b.num_retained as u64
        && intersection_count == union_count
}

fn non_empty_metadata(metadata: ThetaFamilySketchMetadata) -> Option<NonEmptyMeta> {
    match metadata {
        ThetaFamilySketchMetadata::Empty { .. } => None,
        ThetaFamilySketchMetadata::NonEmpty {
            seed_hash,
            theta,
            ordered,
            num_retained,
        } => Some(NonEmptyMeta {
            seed_hash,
            theta,
            ordered,
            num_retained,
        }),
    }
}

fn sampling_adjuster(sampling_probability: f64) -> f64 {
    let adjustment = (1.0 - sampling_probability).sqrt();
    if sampling_probability <= 0.5 {
        adjustment
    } else {
        adjustment + (0.01 * (sampling_probability - 0.5))
    }
}

fn union_lg_k(left_count: usize, right_count: usize) -> u8 {
    let required_capacity = left_count.saturating_add(right_count).max(1);
    let lg_k = usize::BITS - (required_capacity - 1).leading_zeros();
    (lg_k as u8).clamp(MIN_LG_K, MAX_LG_K)
}
