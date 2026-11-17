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

use crate::density::value::DensityValue;

/// Similarity kernel evaluated between two `dim`-dimensional points.
///
/// A kernel drives both coreset compaction (selecting which points are retained) and density
/// evaluation in [`DensitySketch`](crate::density::DensitySketch). [`PartialEq`] is used by
/// [`DensitySketch::merge`](crate::density::DensitySketch::merge) to reject merging sketches
/// configured with different kernel parameters.
pub trait Kernel<T>: PartialEq {
    /// Evaluates the kernel between two points `a` and `b`.
    fn evaluate(&self, a: &[T], b: &[T]) -> f64;
}

/// Standard unscaled Gaussian (RBF) kernel: `exp(-sum_i (a_i - b_i)^2)`.
///
/// # Panics
///
/// [`Kernel::evaluate`] panics if `a.len() != b.len()`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GaussianKernel;

fn gaussian_evaluate<T: DensityValue>(a: &[T], b: &[T]) -> f64 {
    assert_eq!(a.len(), b.len(), "point dimension mismatch");
    let sq_dist: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(&x, &y)| {
            let diff = x.to_f64() - y.to_f64();
            diff * diff
        })
        .sum();
    (-sq_dist).exp()
}

impl Kernel<f32> for GaussianKernel {
    /// Evaluates `exp(-sum_i (a_i as f64 - b_i as f64)^2)`.
    ///
    /// # Panics
    ///
    /// Panics if `a.len() != b.len()`.
    #[inline]
    fn evaluate(&self, a: &[f32], b: &[f32]) -> f64 {
        gaussian_evaluate(a, b)
    }
}

impl Kernel<f64> for GaussianKernel {
    /// Evaluates `exp(-sum_i (a_i - b_i)^2)`.
    ///
    /// # Panics
    ///
    /// Panics if `a.len() != b.len()`.
    #[inline]
    fn evaluate(&self, a: &[f64], b: &[f64]) -> f64 {
        gaussian_evaluate(a, b)
    }
}
