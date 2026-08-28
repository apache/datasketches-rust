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

//! Jaccard similarity for Tuple sketches.

use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::thetacommon::jaccard_similarity;
use crate::thetacommon::jaccard_similarity::JaccardSimilarity;
use crate::tuple::TupleSketchView;

/// Jaccard similarity operator for Tuple sketches.
///
/// Only retained hash keys participate in the similarity calculation. Summary values are ignored,
/// need not implement [`Clone`], and may have different types in the two inputs. The returned
/// [`JaccardSimilarity`] contains the estimate and its 95.4% confidence interval.
///
/// # Examples
///
/// ```
/// use datasketches::tuple::DefaultUpdatePolicy;
/// use datasketches::tuple::TupleJaccardSimilarity;
/// use datasketches::tuple::TupleSketchBuilder;
///
/// let policy = DefaultUpdatePolicy::<u64>::default();
/// let mut a = TupleSketchBuilder::new(policy).build().unwrap();
/// let mut b = TupleSketchBuilder::new(policy).build().unwrap();
/// a.update("apple", 1);
/// b.update("apple", 2);
///
/// let result = TupleJaccardSimilarity::default().compute(&a, &b).unwrap();
/// assert_eq!(result.estimate(), 1.0);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct TupleJaccardSimilarity {
    seed: u64,
}

impl Default for TupleJaccardSimilarity {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED).expect("the default Tuple Jaccard seed must be valid")
    }
}

impl TupleJaccardSimilarity {
    /// Creates a Jaccard similarity operator for the given `seed`.
    ///
    /// # Errors
    ///
    /// Returns an error if the computed seed hash is zero.
    pub fn with_seed(seed: u64) -> Result<Self, Error> {
        compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
        Ok(Self { seed })
    }

    /// Computes the Jaccard similarity index for `sketch_a` and `sketch_b`.
    ///
    /// Summary values do not participate in the comparison.
    ///
    /// # Errors
    ///
    /// Returns an error if both sketches are logically non-empty and either was built with a seed
    /// different from this operator's configured seed. Empty-input fast paths return an exact
    /// result without checking seed compatibility.
    pub fn compute<'a, 'b, S, T>(
        &self,
        sketch_a: impl Into<TupleSketchView<'a, S>>,
        sketch_b: impl Into<TupleSketchView<'b, T>>,
    ) -> Result<JaccardSimilarity, Error>
    where
        S: 'a,
        T: 'b,
    {
        jaccard_similarity::compute(self.seed, sketch_a.into(), sketch_b.into())
    }

    /// Returns whether the two sketches are exactly equal.
    ///
    /// Two logically empty sketches compare equal, while exactly one logically empty sketch
    /// compares unequal. Otherwise, the retained hash keys and theta must match. Summary values do
    /// not participate in the comparison. This compares sketch state, not the original input
    /// populations.
    ///
    /// # Errors
    ///
    /// Returns an error if both sketches are non-empty and either was built with a seed different
    /// from this operator's configured seed.
    pub fn exactly_equal<'a, 'b, S, T>(
        &self,
        sketch_a: impl Into<TupleSketchView<'a, S>>,
        sketch_b: impl Into<TupleSketchView<'b, T>>,
    ) -> Result<bool, Error>
    where
        S: 'a,
        T: 'b,
    {
        jaccard_similarity::exactly_equal(self.seed, sketch_a.into(), sketch_b.into())
    }
}
