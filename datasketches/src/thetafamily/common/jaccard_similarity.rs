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
use crate::thetacommon::ThetaFamilySketchView;
use crate::thetacommon::ThetaKeySketchView;
use crate::thetacommon::binomial_bounds;
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::constants::MIN_LG_K;
use crate::thetacommon::hash_table::CompactSketchParts;
use crate::thetacommon::intersection::IntersectionMergePolicy;
use crate::thetacommon::intersection::IntersectionState;
use crate::thetacommon::sealed;
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
    fn __private(&self, _: sealed::Token) {}

    fn hash(&self) -> u64 {
        self.hash
    }
}

struct KeySketchView<'a, S> {
    sketch: &'a S,
}

impl<'a, S> KeySketchView<'a, S> {
    fn new(sketch: &'a S) -> Self {
        Self { sketch }
    }
}

impl<S: ThetaKeySketchView> ThetaKeySketchView for KeySketchView<'_, S> {
    fn __private(&self, _: sealed::Token) {}

    fn seed_hash(&self) -> u16 {
        self.sketch.seed_hash()
    }

    fn theta64(&self) -> u64 {
        self.sketch.theta64()
    }

    fn is_empty(&self) -> bool {
        self.sketch.is_empty()
    }

    fn is_ordered(&self) -> bool {
        self.sketch.is_ordered()
    }

    fn iter_hashes(&self) -> impl Iterator<Item = u64> + '_ {
        self.sketch.iter_hashes()
    }

    fn num_retained(&self) -> usize {
        self.sketch.num_retained()
    }
}

impl<S: ThetaKeySketchView> ThetaFamilySketchView for KeySketchView<'_, S> {
    type Entry = KeyEntry;

    fn iter(&self) -> impl Iterator<Item = KeyEntry> + '_ {
        self.sketch.iter_hashes().map(|hash| KeyEntry { hash })
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

struct CompactKeySketchView {
    entries: Vec<KeyEntry>,
    theta: u64,
    seed_hash: u16,
    ordered: bool,
    empty: bool,
}

impl ThetaKeySketchView for CompactKeySketchView {
    fn __private(&self, _: sealed::Token) {}

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

    fn iter_hashes(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.iter().map(RetainedEntry::hash)
    }

    fn num_retained(&self) -> usize {
        self.entries.len()
    }
}

impl ThetaFamilySketchView for CompactKeySketchView {
    type Entry = KeyEntry;

    fn iter(&self) -> impl Iterator<Item = KeyEntry> + '_ {
        self.entries.iter().copied()
    }
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

    pub(crate) fn compute<A, B>(
        &self,
        sketch_a: &A,
        sketch_b: &B,
    ) -> Result<JaccardSimilarity, Error>
    where
        A: ThetaKeySketchView,
        B: ThetaKeySketchView,
    {
        if sketch_a.is_empty() && sketch_b.is_empty() {
            return Ok(JaccardSimilarity::exact(1.0));
        }
        if sketch_a.is_empty() || sketch_b.is_empty() {
            return Ok(JaccardSimilarity::exact(0.0));
        }

        let union = self.compute_union(sketch_a, sketch_b)?;
        if !union.entries.is_empty() && identical_sets(sketch_a, sketch_b, &union) {
            return Ok(JaccardSimilarity::exact(1.0));
        }

        let sketch_a = KeySketchView::new(sketch_a);
        let sketch_b = KeySketchView::new(sketch_b);
        let union = CompactKeySketchView {
            entries: union.entries,
            theta: union.theta,
            seed_hash: union.seed_hash,
            ordered: union.ordered,
            empty: union.empty,
        };
        let mut intersection = IntersectionState::new(self.seed, NoopMergePolicy);
        intersection.update(&sketch_a)?;
        intersection.update(&sketch_b)?;
        intersection.update(&union)?;
        let intersection = intersection.result(false);

        JaccardSimilarity::ratio_bounds(
            union.num_retained() as u64,
            intersection.entries.len() as u64,
            union.theta64(),
        )
    }

    pub(crate) fn exactly_equal<A, B>(&self, sketch_a: &A, sketch_b: &B) -> Result<bool, Error>
    where
        A: ThetaKeySketchView,
        B: ThetaKeySketchView,
    {
        if sketch_a.is_empty() && sketch_b.is_empty() {
            return Ok(true);
        }
        if sketch_a.is_empty() || sketch_b.is_empty() {
            return Ok(false);
        }

        let union = self.compute_union(sketch_a, sketch_b)?;
        Ok(identical_sets(sketch_a, sketch_b, &union))
    }

    fn compute_union<A, B>(
        &self,
        sketch_a: &A,
        sketch_b: &B,
    ) -> Result<CompactSketchParts<KeyEntry>, Error>
    where
        A: ThetaKeySketchView,
        B: ThetaKeySketchView,
    {
        let seed_hash = compute_seed_hash(self.seed);
        check_seed_hash(seed_hash, sketch_a.seed_hash(), "A", ErrorKind::InvalidData)?;
        check_seed_hash(seed_hash, sketch_b.seed_hash(), "B", ErrorKind::InvalidData)?;

        let sketch_a = KeySketchView::new(sketch_a);
        let sketch_b = KeySketchView::new(sketch_b);
        let mut union = UnionState::new(
            union_lg_k(sketch_a.num_retained(), sketch_b.num_retained()),
            ResizeFactor::X8,
            1.0,
            self.seed,
            NoopMergePolicy,
        );
        union.update(&sketch_a)?;
        union.update(&sketch_b)?;
        Ok(union.to_compact_parts(false))
    }
}

/// Returns whether both sketches have the same retained keys and theta.
///
/// When the union retains no additional keys and preserves both input theta values, each input
/// contains exactly the same retained key set represented by the union.
fn identical_sets<A, B>(sketch_a: &A, sketch_b: &B, union: &CompactSketchParts<KeyEntry>) -> bool
where
    A: ThetaKeySketchView,
    B: ThetaKeySketchView,
{
    union.entries.len() == sketch_a.num_retained()
        && union.entries.len() == sketch_b.num_retained()
        && union.theta == sketch_a.theta64()
        && union.theta == sketch_b.theta64()
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
