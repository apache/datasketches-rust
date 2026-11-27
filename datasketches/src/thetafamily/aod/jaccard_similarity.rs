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

use crate::aod::ArrayOfDoublesSketchView;
use crate::common::JaccardSimilarity;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::thetafamily::common::jaccard_similarity;

/// Jaccard similarity operator for ArrayOfDoubles sketches.
///
/// Only retained hash keys participate in the similarity calculation; summary values and
/// `num_values` are ignored and may differ between the two inputs. The returned
/// [`JaccardSimilarity`] contains the estimate and its 95.4% confidence interval.
///
/// # Examples
///
/// ```
/// use datasketches::aod::ArrayOfDoublesJaccardSimilarity;
/// use datasketches::aod::ArrayOfDoublesSketchBuilder;
///
/// let mut a = ArrayOfDoublesSketchBuilder::default()
///     .num_values(1)
///     .build()?;
/// let mut b = ArrayOfDoublesSketchBuilder::default()
///     .num_values(2)
///     .build()?;
/// a.update("apple", [1.0])?;
/// b.update("apple", [2.0, 3.0])?;
///
/// let result = ArrayOfDoublesJaccardSimilarity::default().compute(&a, &b)?;
/// assert_eq!(result.estimate(), 1.0);
/// # Ok::<(), datasketches::error::Error>(())
/// ```
#[derive(Clone, Copy, Debug)]
pub struct ArrayOfDoublesJaccardSimilarity {
    seed: u64,
}

impl Default for ArrayOfDoublesJaccardSimilarity {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED).unwrap()
    }
}

impl ArrayOfDoublesJaccardSimilarity {
    /// Creates a Jaccard similarity operator for `seed`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the computed seed hash is zero.
    pub fn with_seed(seed: u64) -> Result<Self, Error> {
        compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
        Ok(Self { seed })
    }

    /// Computes the Jaccard similarity index for `sketch_a` and `sketch_b`.
    ///
    /// Summary values and `num_values` do not participate in the comparison.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if both sketches are logically non-empty and either was built
    /// with a seed different from this operator's configured seed. Empty-input fast paths return
    /// an exact result without checking seed compatibility.
    pub fn compute<'a, 'b>(
        &self,
        sketch_a: impl Into<ArrayOfDoublesSketchView<'a>>,
        sketch_b: impl Into<ArrayOfDoublesSketchView<'b>>,
    ) -> Result<JaccardSimilarity, Error> {
        jaccard_similarity::compute(self.seed, sketch_a.into(), sketch_b.into())
    }

    /// Returns whether the two sketches represent the exact same retained key set and theta.
    ///
    /// Two logically empty sketches compare equal, while exactly one logically empty sketch
    /// compares unequal. Otherwise, the retained hash keys and theta must match. Summary values
    /// and `num_values` do not participate in the comparison.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if both sketches are non-empty and either was built with a seed
    /// different from this operator's configured seed.
    pub fn exactly_equal<'a, 'b>(
        &self,
        sketch_a: impl Into<ArrayOfDoublesSketchView<'a>>,
        sketch_b: impl Into<ArrayOfDoublesSketchView<'b>>,
    ) -> Result<bool, Error> {
        jaccard_similarity::exactly_equal(self.seed, sketch_a.into(), sketch_b.into())
    }
}
