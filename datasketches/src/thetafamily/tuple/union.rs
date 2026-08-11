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

//! Tuple sketch union.
//!
//! [`TupleUnion`] computes the union (set OR) of any number of Tuple sketches. It shares its state
//! machine with the Theta union; the only Tuple-specific behavior is that when an incoming key
//! already exists in the union, the two summaries are combined with a [`SummaryCombinePolicy`]
//! instead of one being dropped.

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::thetacommon::constants::DEFAULT_LG_K;
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MIN_LG_K;
use crate::thetacommon::union::UnionState;
use crate::tuple::hash_table::TupleEntry;
use crate::tuple::policy::SummaryCombinePolicy;
use crate::tuple::sketch::CompactTupleSketch;
use crate::tuple::sketch::TupleSketchView;

/// Union (set OR) of Tuple sketches.
///
/// `P` is the [`SummaryCombinePolicy`] applied when a key is present in more than one input. For
/// additive summaries the built-in
/// [`DefaultUnionPolicy`](crate::tuple::DefaultUnionPolicy) can be used.
///
/// # Examples
///
/// ```
/// use datasketches::tuple::DefaultUnionPolicy;
/// use datasketches::tuple::DefaultUpdatePolicy;
/// use datasketches::tuple::TupleSketchBuilder;
/// use datasketches::tuple::TupleUnionBuilder;
///
/// let update_policy = DefaultUpdatePolicy::<u64>::default();
/// let mut a = TupleSketchBuilder::new(update_policy).build();
/// a.update("apple", 1);
/// a.update("banana", 1);
///
/// let mut b = TupleSketchBuilder::new(update_policy).build();
/// b.update("banana", 1);
/// b.update("cherry", 1);
///
/// let mut union = TupleUnionBuilder::new(DefaultUnionPolicy::<u64>::default()).build();
/// union.update(&a).unwrap();
/// union.update(&b).unwrap();
///
/// let result = union.to_sketch(true);
/// assert_eq!(result.num_retained(), 3); // apple, banana, cherry
/// ```
#[derive(Debug)]
pub struct TupleUnion<P>
where
    P: SummaryCombinePolicy,
{
    state: UnionState<TupleEntry<P::Summary>, P>,
}

impl<P> TupleUnion<P>
where
    P: SummaryCombinePolicy,
{
    /// Merges a sketch into the union.
    ///
    /// Accepts either an [`TupleSketch`](crate::tuple::TupleSketch) or a
    /// [`CompactTupleSketch`] through [`TupleSketchView`]. Keys present in both the running union
    /// and `sketch` have their summaries combined via the union policy.
    ///
    /// # Errors
    ///
    /// Returns an error if `sketch` was built with a different seed than this union (its seed hash
    /// does not match).
    pub fn update<'a>(
        &mut self,
        sketch: impl Into<TupleSketchView<'a, P::Summary>>,
    ) -> Result<(), Error>
    where
        P::Summary: Clone + 'a,
    {
        let sketch = sketch.into();
        self.state.update(sketch)
    }

    /// Returns the union as a [`CompactTupleSketch`].
    ///
    /// If `ordered` is `true`, retained entries are sorted ascending by hash.
    pub fn to_sketch(&self, ordered: bool) -> CompactTupleSketch<P::Summary>
    where
        P::Summary: Clone,
    {
        let result = self.state.to_compact_parts(ordered);
        CompactTupleSketch::from_parts(
            result.entries,
            result.theta,
            result.seed_hash,
            result.ordered,
            result.empty,
        )
    }

    /// Resets the union to its initial empty state.
    pub fn reset(&mut self) {
        self.state.reset();
    }

    /// Returns the estimated size of the union in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.state.estimated_size()
    }
}

/// Builder for [`TupleUnion`].
///
/// Every builder carries a concrete [`SummaryCombinePolicy`]. Use
/// [`DefaultUnionPolicy`](crate::tuple::DefaultUnionPolicy) for additive summaries, or supply a
/// custom combine policy.
#[derive(Debug)]
pub struct TupleUnionBuilder<P>
where
    P: SummaryCombinePolicy,
{
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    policy: P,
}

impl<P> TupleUnionBuilder<P>
where
    P: SummaryCombinePolicy,
{
    /// Creates a builder with the given combine policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tuple::DefaultUnionPolicy;
    /// use datasketches::tuple::TupleUnionBuilder;
    ///
    /// let union = TupleUnionBuilder::new(DefaultUnionPolicy::<u64>::default())
    ///     .lg_k(12)
    ///     .build();
    /// ```
    pub fn new(policy: P) -> Self {
        Self {
            lg_k: DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: DEFAULT_UPDATE_SEED,
            policy,
        }
    }

    /// Sets `lg_k`, the base-2 logarithm of the nominal capacity.
    ///
    /// # Panics
    ///
    /// Panics if `lg_k` is outside `[5, 26]`.
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&lg_k),
            "lg_k must be in [{MIN_LG_K}, {MAX_LG_K}], got {lg_k}"
        );
        self.lg_k = lg_k;
        self
    }

    /// Sets the resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Sets the sampling probability.
    ///
    /// # Panics
    ///
    /// Panics if `probability` is outside `(0.0, 1.0]`.
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        assert!(
            (0.0..=1.0).contains(&probability) && probability > 0.0,
            "sampling_probability must be in (0.0, 1.0], got {probability}"
        );
        self.sampling_probability = probability;
        self
    }

    /// Sets the hash seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Builds the [`TupleUnion`].
    pub fn build(self) -> TupleUnion<P> {
        TupleUnion {
            state: UnionState::new(
                self.lg_k,
                self.resize_factor,
                self.sampling_probability,
                self.seed,
                self.policy,
            ),
        }
    }
}
