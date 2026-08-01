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
use crate::thetacommon::jaccard_similarity::RawThetaJaccardSimilarity;

/// Jaccard similarity result for two Theta sketches.
///
/// The bounds use a 95.4% confidence interval, equivalent to +/- 2 standard deviations.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JaccardSimilarity {
    lower_bound: f64,
    estimate: f64,
    upper_bound: f64,
}

impl JaccardSimilarity {
    /// Computes the Jaccard similarity index with the default update seed.
    pub fn between<A: ThetaSketchView, B: ThetaSketchView>(
        sketch_a: &A,
        sketch_b: &B,
    ) -> Result<Self, Error> {
        Self::between_with_seed(sketch_a, sketch_b, DEFAULT_UPDATE_SEED)
    }

    /// Computes the Jaccard similarity index with an explicit update seed.
    ///
    /// Returns an error if a non-empty sketch was built with a different seed.
    pub fn between_with_seed<A: ThetaSketchView, B: ThetaSketchView>(
        sketch_a: &A,
        sketch_b: &B,
        seed: u64,
    ) -> Result<Self, Error> {
        let raw = RawThetaJaccardSimilarity::compute(sketch_a, sketch_b, seed)?;
        Ok(Self {
            lower_bound: raw.lower_bound,
            estimate: raw.estimate,
            upper_bound: raw.upper_bound,
        })
    }

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
}
