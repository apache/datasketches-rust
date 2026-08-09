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
use crate::thetacommon::OwnedEntrySketchView;
use crate::thetacommon::RetainedEntry;
use crate::thetacommon::SetOpProps;
use crate::thetacommon::SetOperationSketchView;
use crate::thetacommon::binomial_bounds;
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::constants::MIN_LG_K;
use crate::thetacommon::hash_table::CompactSketchParts;
use crate::thetacommon::intersection::IntersectionMergePolicy;
use crate::thetacommon::intersection::IntersectionState;
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

    fn ratio_bounds(union_count: u64, intersection_count: u64, theta: u64) -> Result<Self, Error> {
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

        let sampling_probability = theta as f64 / MAX_THETA as f64;
        if sampling_probability <= 0.0 || sampling_probability > 1.0 {
            return Err(Error::invalid_argument(format!(
                "theta must produce a probability in (0.0, 1.0], got {sampling_probability}"
            )));
        }
        if sampling_probability == 1.0 {
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

impl RetainedEntry for KeyEntry {
    fn hash(&self) -> u64 {
        self.hash
    }
}

#[derive(Clone, Copy, Debug)]
struct NoopMergePolicy;

impl<E: RetainedEntry> UnionMergePolicy<E> for NoopMergePolicy {
    fn merge(&self, _existing: &mut E, _incoming: E) {}
}

impl<E: RetainedEntry> IntersectionMergePolicy<E> for NoopMergePolicy {
    fn merge(&self, _existing: &mut E, _incoming: E) {}
}

#[derive(Clone, Copy, Debug)]
struct KeySketch<S>(S);

impl<S> SetOperationSketchView for KeySketch<S>
where
    S: SetOperationSketchView,
{
    fn props(self) -> SetOpProps {
        self.0.props()
    }

    fn hashes(self) -> impl Iterator<Item = u64> {
        self.0.hashes()
    }
}

impl<S> OwnedEntrySketchView for KeySketch<S>
where
    S: SetOperationSketchView,
{
    type Entry = KeyEntry;

    fn entries(self) -> impl Iterator<Item = Self::Entry> {
        self.0.hashes().map(|hash| KeyEntry { hash })
    }
}

pub fn compute<A, B>(seed: u64, sketch_a: A, sketch_b: B) -> Result<JaccardSimilarity, Error>
where
    A: SetOperationSketchView,
    B: SetOperationSketchView,
{
    let SetOpProps {
        theta: a_theta,
        empty: a_empty,
        num_retained: a_num_retained,
        ..
    } = sketch_a.props();
    let SetOpProps {
        theta: b_theta,
        empty: b_empty,
        num_retained: b_num_retained,
        ..
    } = sketch_b.props();
    if a_empty && b_empty {
        return Ok(JaccardSimilarity::exact(1.0));
    }
    if a_empty || b_empty {
        return Ok(JaccardSimilarity::exact(0.0));
    }

    let sketch_a_state = (a_num_retained, a_theta);
    let sketch_b_state = (b_num_retained, b_theta);
    let union = compute_union(seed, sketch_a, sketch_b)?;
    if !union.entries.is_empty() && identical_sets(sketch_a_state, sketch_b_state, &union) {
        return Ok(JaccardSimilarity::exact(1.0));
    }

    let mut intersection = IntersectionState::new(seed, NoopMergePolicy);
    intersection.update(KeySketch(sketch_a))?;
    intersection.update(KeySketch(sketch_b))?;
    let intersection = intersection.result(false);
    let intersection_count = intersection
        .entries
        .iter()
        .filter(|entry| entry.hash < union.theta)
        .count();

    JaccardSimilarity::ratio_bounds(
        union.entries.len() as u64,
        intersection_count as u64,
        union.theta,
    )
}

pub fn exactly_equal<A, B>(seed: u64, sketch_a: A, sketch_b: B) -> Result<bool, Error>
where
    A: SetOperationSketchView,
    B: SetOperationSketchView,
{
    let SetOpProps {
        theta: a_theta,
        empty: a_empty,
        num_retained: a_num_retained,
        ..
    } = sketch_a.props();
    let SetOpProps {
        theta: b_theta,
        empty: b_empty,
        num_retained: b_num_retained,
        ..
    } = sketch_b.props();
    if a_empty && b_empty {
        return Ok(true);
    }
    if a_empty || b_empty {
        return Ok(false);
    }

    let sketch_a_state = (a_num_retained, a_theta);
    let sketch_b_state = (b_num_retained, b_theta);
    let union = compute_union(seed, sketch_a, sketch_b)?;
    Ok(identical_sets(sketch_a_state, sketch_b_state, &union))
}

fn compute_union<A, B>(
    seed: u64,
    sketch_a: A,
    sketch_b: B,
) -> Result<CompactSketchParts<KeyEntry>, Error>
where
    A: SetOperationSketchView,
    B: SetOperationSketchView,
{
    let SetOpProps {
        seed_hash: a_seed_hash,
        num_retained: a_num_retained,
        ..
    } = sketch_a.props();
    let SetOpProps {
        seed_hash: b_seed_hash,
        num_retained: b_num_retained,
        ..
    } = sketch_b.props();
    let seed_hash = compute_seed_hash(seed);
    check_seed_hash(seed_hash, a_seed_hash, "A", ErrorKind::InvalidData)?;
    check_seed_hash(seed_hash, b_seed_hash, "B", ErrorKind::InvalidData)?;

    let mut union = UnionState::new(
        union_lg_k(a_num_retained, b_num_retained),
        ResizeFactor::X8,
        1.0,
        seed,
        NoopMergePolicy,
    );
    union.update(KeySketch(sketch_a))?;
    union.update(KeySketch(sketch_b))?;
    Ok(union.to_compact_parts(false))
}

/// Returns whether both sketches have the same retained keys and theta.
///
/// When the union retains no additional keys and preserves both input theta values, each input
/// contains exactly the same retained key set represented by the union.
fn identical_sets(
    sketch_a: (usize, u64),
    sketch_b: (usize, u64),
    union: &CompactSketchParts<KeyEntry>,
) -> bool {
    union.entries.len() == sketch_a.0
        && union.entries.len() == sketch_b.0
        && union.theta == sketch_a.1
        && union.theta == sketch_b.1
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
