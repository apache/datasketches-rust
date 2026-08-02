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
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::ThetaSketchView;
use crate::thetacommon::jaccard_similarity::JaccardSimilarity;
use crate::thetacommon::jaccard_similarity::JaccardSimilarityOperator;

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
/// let mut a = ThetaSketchBuilder::default().build();
/// let mut b = ThetaSketchBuilder::default().build();
/// a.update("apple");
/// b.update("apple");
///
/// let result = ThetaJaccardSimilarity::default().compute(&a, &b).unwrap();
/// assert_eq!(result.estimate(), 1.0);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct ThetaJaccardSimilarity {
    op: JaccardSimilarityOperator,
}

impl Default for ThetaJaccardSimilarity {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED)
    }
}

impl ThetaJaccardSimilarity {
    /// Creates a Jaccard similarity operator for the given `seed`.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            op: JaccardSimilarityOperator::new(seed),
        }
    }

    /// Computes the Jaccard similarity index for `sketch_a` and `sketch_b`.
    ///
    /// # Errors
    ///
    /// Returns an error if either non-empty sketch was built with a seed different from this
    /// operator's configured seed.
    pub fn compute<A: ThetaSketchView, B: ThetaSketchView>(
        &self,
        sketch_a: &A,
        sketch_b: &B,
    ) -> Result<JaccardSimilarity, Error> {
        self.op.compute(sketch_a, sketch_b)
    }
}
