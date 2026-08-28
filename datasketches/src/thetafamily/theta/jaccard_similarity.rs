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

//! Jaccard similarity for Theta sketches.

use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::theta::ThetaSketchView;
use crate::thetacommon::jaccard_similarity;
use crate::thetacommon::jaccard_similarity::JaccardSimilarity;

/// Jaccard similarity operator for Theta sketches.
///
/// This is a stateless operator other than its configured hash seed. The returned
/// [`JaccardSimilarity`] contains the estimate and its 95.4% confidence interval.
///
/// # Examples
///
/// ```
/// use datasketches::theta::ThetaJaccardSimilarity;
/// use datasketches::theta::ThetaSketchBuilder;
///
/// let mut a = ThetaSketchBuilder::default().build().unwrap();
/// let mut b = ThetaSketchBuilder::default().build().unwrap();
/// a.update("apple");
/// b.update("apple");
///
/// let result = ThetaJaccardSimilarity::default().compute(&a, &b).unwrap();
/// assert_eq!(result.estimate(), 1.0);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct ThetaJaccardSimilarity {
    seed: u64,
}

impl Default for ThetaJaccardSimilarity {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED).unwrap()
    }
}

impl ThetaJaccardSimilarity {
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
    /// # Errors
    ///
    /// Returns an error if both sketches are logically non-empty and either was built with a seed
    /// different from this operator's configured seed. Empty-input fast paths return an exact
    /// result without checking seed compatibility.
    pub fn compute<'a, 'b>(
        &self,
        sketch_a: impl Into<ThetaSketchView<'a>>,
        sketch_b: impl Into<ThetaSketchView<'b>>,
    ) -> Result<JaccardSimilarity, Error> {
        jaccard_similarity::compute(self.seed, sketch_a.into(), sketch_b.into())
    }

    /// Returns whether the two sketches are exactly equal.
    ///
    /// Two logically empty sketches compare equal, while exactly one logically empty sketch
    /// compares unequal. Otherwise, the retained hashes and theta must match. This compares sketch
    /// state, not the original input populations.
    ///
    /// # Errors
    ///
    /// Returns an error if both sketches are non-empty and either was built with a seed different
    /// from this operator's configured seed.
    pub fn exactly_equal<'a, 'b>(
        &self,
        sketch_a: impl Into<ThetaSketchView<'a>>,
        sketch_b: impl Into<ThetaSketchView<'b>>,
    ) -> Result<bool, Error> {
        jaccard_similarity::exactly_equal(self.seed, sketch_a.into(), sketch_b.into())
    }
}
