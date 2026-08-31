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
use crate::hash::compute_seed_hash;
use crate::thetacommon::EntrySketch;
use crate::thetacommon::KeySketch;
use crate::thetacommon::SketchEntry;
use crate::thetacommon::ThetaSketchMetadata;
use crate::thetacommon::binomial_bounds;
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::constants::MIN_LG_K;
use crate::thetacommon::intersection::IntersectionMergePolicy;
use crate::thetacommon::intersection::IntersectionState;
use crate::thetacommon::sketch_state::CompactSketchState;
use crate::thetacommon::sketch_state::ThetaThreshold;
use crate::thetacommon::union::UnionMergePolicy;
use crate::thetacommon::union::UnionState;

const NUM_STD_DEVS: f64 = 2.0;

/// Jaccard similarity estimate and confidence bounds for two Theta-family sketches.
///
/// The bounds use a 95.4% confidence interval, equivalent to +/- 2 standard deviations.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JaccardSimilarity {
    lower_bound: f64,
    estimate: f64,
    upper_bound: f64,
}

impl JaccardSimilarity {
    /// Returns the approximate lower bound for the Jaccard index.
    pub fn lower_bound(&self) -> f64 {
        self.lower_bound
    }

    /// Returns the estimate of the Jaccard index.
    pub fn estimate(&self) -> f64 {
        self.estimate
    }

    /// Returns the approximate upper bound for the Jaccard index.
    pub fn upper_bound(&self) -> f64 {
        self.upper_bound
    }

    fn exact(value: f64) -> Self {
        Self {
            lower_bound: value,
            estimate: value,
            upper_bound: value,
        }
    }

    fn ratio_bounds(
        union_count: u64,
        intersection_count: u64,
        theta: ThetaThreshold,
    ) -> Result<Self, Error> {
        if intersection_count > union_count {
            return Err(Error::invalid_argument(format!(
                "intersection count cannot exceed union count: {intersection_count} > {union_count}"
            )));
        }
        if union_count == 0 {
            return Ok(Self {
                lower_bound: 0.0,
                estimate: 0.5,
                upper_bound: 1.0,
            });
        }

        let sampling_probability = theta.get() as f64 / MAX_THETA as f64;
        if theta == ThetaThreshold::MAX {
            return Ok(Self::exact(intersection_count as f64 / union_count as f64));
        }

        let adjustment = NUM_STD_DEVS * sampling_adjuster(sampling_probability);
        Ok(Self {
            lower_bound: binomial_bounds::approximate_lower_bound_on_p(
                union_count,
                intersection_count,
                adjustment,
            )?,
            estimate: intersection_count as f64 / union_count as f64,
            upper_bound: binomial_bounds::approximate_upper_bound_on_p(
                union_count,
                intersection_count,
                adjustment,
            )?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct KeyEntry {
    hash: u64,
}

impl SketchEntry for KeyEntry {
    fn hash(&self) -> u64 {
        self.hash
    }
}

#[derive(Clone, Copy, Debug)]
struct NoopMergePolicy;

impl<E: SketchEntry> UnionMergePolicy<E> for NoopMergePolicy {
    fn merge(&self, _existing: &mut E, _incoming: E) {}
}

impl<E: SketchEntry> IntersectionMergePolicy<E> for NoopMergePolicy {
    fn merge(&self, _existing: &mut E, _incoming: E) {}
}

#[derive(Clone, Copy, Debug)]
struct KeyEntries<S>(S);

impl<S> KeySketch for KeyEntries<S>
where
    S: KeySketch,
{
    fn metadata(self) -> ThetaSketchMetadata {
        self.0.metadata()
    }

    fn hashes(self) -> impl Iterator<Item = u64> {
        self.0.hashes()
    }
}

impl<S> EntrySketch for KeyEntries<S>
where
    S: KeySketch,
{
    type Entry = KeyEntry;

    fn entries(self) -> impl Iterator<Item = Self::Entry> {
        self.0.hashes().map(|hash| KeyEntry { hash })
    }
}

pub fn compute<A, B>(seed: u64, sketch_a: A, sketch_b: B) -> Result<JaccardSimilarity, Error>
where
    A: KeySketch,
    B: KeySketch,
{
    let a_metadata = sketch_a.metadata();
    let b_metadata = sketch_b.metadata();
    if a_metadata.is_empty() && b_metadata.is_empty() {
        return Ok(JaccardSimilarity::exact(1.0));
    }
    if a_metadata.is_empty() || b_metadata.is_empty() {
        return Ok(JaccardSimilarity::exact(0.0));
    }

    let sketch_a_state = (a_metadata.num_retained(), a_metadata.theta());
    let sketch_b_state = (b_metadata.num_retained(), b_metadata.theta());
    let union = compute_union(seed, sketch_a, sketch_b)?;
    if identical_sets(sketch_a_state, sketch_b_state, &union) {
        return Ok(JaccardSimilarity::exact(1.0));
    }

    let mut intersection = IntersectionState::new(seed, NoopMergePolicy)?;
    intersection.update(KeyEntries(sketch_a))?;
    intersection.update(KeyEntries(sketch_b))?;
    let intersection = intersection
        .to_compact_sketch_state(false)
        .expect("two intersection updates must produce a result");
    let union_theta = union.theta_sketch_state().theta();
    let intersection_count = intersection
        .retained_entries()
        .iter()
        .filter(|entry| entry.hash < union_theta.get())
        .count();

    JaccardSimilarity::ratio_bounds(
        union.retained_entries().len() as u64,
        intersection_count as u64,
        union_theta,
    )
}

pub fn exactly_equal<A, B>(seed: u64, sketch_a: A, sketch_b: B) -> Result<bool, Error>
where
    A: KeySketch,
    B: KeySketch,
{
    let a_metadata = sketch_a.metadata();
    let b_metadata = sketch_b.metadata();
    if a_metadata.is_empty() && b_metadata.is_empty() {
        return Ok(true);
    }
    if a_metadata.is_empty() || b_metadata.is_empty() {
        return Ok(false);
    }

    let sketch_a_state = (a_metadata.num_retained(), a_metadata.theta());
    let sketch_b_state = (b_metadata.num_retained(), b_metadata.theta());
    let union = compute_union(seed, sketch_a, sketch_b)?;
    Ok(identical_sets(sketch_a_state, sketch_b_state, &union))
}

fn compute_union<A, B>(
    seed: u64,
    sketch_a: A,
    sketch_b: B,
) -> Result<CompactSketchState<KeyEntry>, Error>
where
    A: KeySketch,
    B: KeySketch,
{
    let a_metadata = sketch_a.metadata();
    let b_metadata = sketch_b.metadata();
    let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
    check_seed_hash(
        seed_hash,
        a_metadata.seed_hash(),
        "A",
        ErrorKind::InvalidData,
    )?;
    check_seed_hash(
        seed_hash,
        b_metadata.seed_hash(),
        "B",
        ErrorKind::InvalidData,
    )?;

    let mut union = UnionState::new(
        union_lg_k(a_metadata.num_retained(), b_metadata.num_retained()),
        ResizeFactor::X8,
        1.0,
        seed,
        NoopMergePolicy,
    )?;
    union.update(KeyEntries(sketch_a))?;
    union.update(KeyEntries(sketch_b))?;
    Ok(union.to_compact_sketch_state(false))
}

/// Returns whether both sketches have the same retained keys and theta.
///
/// When the union retains no additional keys and preserves both input theta values, each input
/// contains exactly the same retained key set represented by the union.
fn identical_sets(
    sketch_a: (usize, ThetaThreshold),
    sketch_b: (usize, ThetaThreshold),
    union: &CompactSketchState<KeyEntry>,
) -> bool {
    let union_state = union.theta_sketch_state();
    union.retained_entries().len() == sketch_a.0
        && union.retained_entries().len() == sketch_b.0
        && union_state.theta() == sketch_a.1
        && union_state.theta() == sketch_b.1
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
