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
use crate::thetacommon::RetainedEntry;
use crate::thetacommon::ThetaFamilySketchView;
use crate::thetacommon::bounds_binomial_proportions;
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::constants::MIN_LG_K;
use crate::thetacommon::intersection::IntersectionMergePolicy;
use crate::thetacommon::intersection::IntersectionState;
use crate::thetacommon::union::UnionMergePolicy;
use crate::thetacommon::union::UnionState;

const NUM_STD_DEVS: f64 = 2.0;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RawThetaJaccardSimilarity {
    pub(crate) lower_bound: f64,
    pub(crate) estimate: f64,
    pub(crate) upper_bound: f64,
}

#[derive(Debug)]
struct NoopMergePolicy;

impl<E: RetainedEntry> UnionMergePolicy<E> for NoopMergePolicy {
    fn merge(&self, _existing: &mut E, _incoming: E) {}
}

impl<E: RetainedEntry> IntersectionMergePolicy<E> for NoopMergePolicy {
    fn merge(&self, _existing: &mut E, _incoming: E) {}
}

struct CompactSketchView<E> {
    entries: Vec<E>,
    theta: u64,
    seed_hash: u16,
    ordered: bool,
    empty: bool,
}

impl<E: RetainedEntry + Clone> ThetaFamilySketchView for CompactSketchView<E> {
    type Entry = E;

    fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    fn theta64(&self) -> u64 {
        self.theta
    }

    fn is_empty(&self) -> bool {
        self.empty
    }

    fn is_ordered(&self) -> bool {
        self.ordered
    }

    fn iter(&self) -> impl Iterator<Item = E> + '_ {
        self.entries.iter().cloned()
    }

    fn num_retained(&self) -> usize {
        self.entries.len()
    }
}

impl RawThetaJaccardSimilarity {
    pub(crate) fn compute<A, B>(sketch_a: &A, sketch_b: &B, seed: u64) -> Result<Self, Error>
    where
        A: ThetaFamilySketchView,
        B: ThetaFamilySketchView<Entry = A::Entry>,
        A::Entry: Clone,
    {
        if sketch_a.is_empty() && sketch_b.is_empty() {
            return Ok(Self::exact(1.0));
        }
        if sketch_a.is_empty() || sketch_b.is_empty() {
            return Ok(Self::exact(0.0));
        }

        let mut union = UnionState::new(
            union_lg_k(sketch_a.num_retained(), sketch_b.num_retained()),
            ResizeFactor::X8,
            1.0,
            seed,
            NoopMergePolicy,
        );
        union.update(sketch_a)?;
        union.update(sketch_b)?;
        let union = union.to_compact_parts(false);

        if union.entries.len() == sketch_a.num_retained()
            && union.entries.len() == sketch_b.num_retained()
            && union.theta == sketch_a.theta64()
            && union.theta == sketch_b.theta64()
        {
            return Ok(Self::exact(1.0));
        }

        let union = CompactSketchView {
            entries: union.entries,
            theta: union.theta,
            seed_hash: union.seed_hash,
            ordered: union.ordered,
            empty: union.empty,
        };
        let mut intersection = IntersectionState::new(seed, NoopMergePolicy);
        intersection.update(sketch_a)?;
        intersection.update(sketch_b)?;
        intersection.update(&union)?;
        let intersection = intersection.result(false);

        Self::ratio_bounds(
            union.num_retained() as u64,
            intersection.entries.len() as u64,
            union.theta64(),
        )
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
            lower_bound: bounds_binomial_proportions::approximate_lower_bound_on_p(
                union_count,
                intersection_count,
                adjustment,
            )?,
            estimate: intersection_count as f64 / union_count as f64,
            upper_bound: bounds_binomial_proportions::approximate_upper_bound_on_p(
                union_count,
                intersection_count,
                adjustment,
            )?,
        })
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
