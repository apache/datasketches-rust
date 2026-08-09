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
use crate::thetacommon::RetainedEntry;
use crate::thetacommon::SketchMetadata;
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

pub(crate) trait JaccardSketch: Copy {
    fn metadata(self) -> SketchMetadata;

    fn hashes(self) -> impl Iterator<Item = u64>;
}

/// Configured Jaccard operator shared by Theta and Tuple public wrappers.
#[derive(Clone, Copy, Debug)]
pub(crate) struct JaccardSimilarityOperator {
    seed: u64,
}

impl JaccardSimilarityOperator {
    pub(crate) fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub(crate) fn compute<A, B>(&self, sketch_a: A, sketch_b: B) -> Result<JaccardSimilarity, Error>
    where
        A: JaccardSketch,
        B: JaccardSketch,
    {
        let metadata_a = sketch_a.metadata();
        let metadata_b = sketch_b.metadata();
        if metadata_a.empty && metadata_b.empty {
            return Ok(JaccardSimilarity::exact(1.0));
        }
        if metadata_a.empty || metadata_b.empty {
            return Ok(JaccardSimilarity::exact(0.0));
        }

        let sketch_a_state = (metadata_a.num_retained, metadata_a.theta);
        let sketch_b_state = (metadata_b.num_retained, metadata_b.theta);
        let union = self.compute_union(sketch_a, sketch_b)?;
        if !union.entries.is_empty() && identical_sets(sketch_a_state, sketch_b_state, &union) {
            return Ok(JaccardSimilarity::exact(1.0));
        }

        let mut intersection = IntersectionState::new(self.seed, NoopMergePolicy);
        intersection.update(metadata_a, sketch_a.hashes().map(|hash| KeyEntry { hash }))?;
        intersection.update(metadata_b, sketch_b.hashes().map(|hash| KeyEntry { hash }))?;
        let union_metadata = SketchMetadata {
            seed_hash: union.seed_hash,
            theta: union.theta,
            empty: union.empty,
            ordered: union.ordered,
            num_retained: union.entries.len(),
        };
        intersection.update(union_metadata, union.entries.iter().copied())?;
        let intersection = intersection.result(false);

        JaccardSimilarity::ratio_bounds(
            union.entries.len() as u64,
            intersection.entries.len() as u64,
            union.theta,
        )
    }

    pub(crate) fn exactly_equal<A, B>(&self, sketch_a: A, sketch_b: B) -> Result<bool, Error>
    where
        A: JaccardSketch,
        B: JaccardSketch,
    {
        let metadata_a = sketch_a.metadata();
        let metadata_b = sketch_b.metadata();
        if metadata_a.empty && metadata_b.empty {
            return Ok(true);
        }
        if metadata_a.empty || metadata_b.empty {
            return Ok(false);
        }

        let sketch_a_state = (metadata_a.num_retained, metadata_a.theta);
        let sketch_b_state = (metadata_b.num_retained, metadata_b.theta);
        let union = self.compute_union(sketch_a, sketch_b)?;
        Ok(identical_sets(sketch_a_state, sketch_b_state, &union))
    }

    fn compute_union<A, B>(
        &self,
        sketch_a: A,
        sketch_b: B,
    ) -> Result<CompactSketchParts<KeyEntry>, Error>
    where
        A: JaccardSketch,
        B: JaccardSketch,
    {
        let metadata_a = sketch_a.metadata();
        let metadata_b = sketch_b.metadata();
        let seed_hash = compute_seed_hash(self.seed);
        check_seed_hash(seed_hash, metadata_a.seed_hash, "A", ErrorKind::InvalidData)?;
        check_seed_hash(seed_hash, metadata_b.seed_hash, "B", ErrorKind::InvalidData)?;

        let mut union = UnionState::new(
            union_lg_k(metadata_a.num_retained, metadata_b.num_retained),
            ResizeFactor::X8,
            1.0,
            self.seed,
            NoopMergePolicy,
        );
        union.update(metadata_a, sketch_a.hashes().map(|hash| KeyEntry { hash }))?;
        union.update(metadata_b, sketch_b.hashes().map(|hash| KeyEntry { hash }))?;
        Ok(union.to_compact_parts(false))
    }
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
