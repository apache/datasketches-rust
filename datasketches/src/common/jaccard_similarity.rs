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

use crate::error::Error;
#[cfg(any(feature = "theta", feature = "tuple"))]
use crate::hash::DEFAULT_UPDATE_SEED;

/// Sketch input accepted by [`JaccardSimilarity`] comparisons.
///
/// Implemented for borrowed Theta and Tuple sketches and their read-only views. Only retained hash
/// keys and `theta` participate in comparisons; Tuple summary values are ignored, need not
/// implement [`Clone`], and may differ in type between the two inputs. Both inputs may mix Theta
/// and Tuple sketches directly.
///
/// This trait is sealed and cannot be implemented outside `datasketches`.
#[cfg(any(feature = "theta", feature = "tuple"))]
pub trait JaccardSketch: Copy + crate::thetafamily::common::KeySketch {}

/// Jaccard similarity estimate and confidence bounds for two sketches.
///
/// The bounds use a two-sided 95.4% confidence interval, equivalent to +/- 2 standard deviations.
/// When the `theta` or `tuple` feature is enabled, associated functions compute Jaccard similarity
/// and exact sketch-state equality across any pair of [`JaccardSketch`] inputs.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "theta")]
/// # {
/// use datasketches::common::JaccardSimilarity;
/// use datasketches::theta::ThetaSketchBuilder;
///
/// let mut a = ThetaSketchBuilder::default().build().unwrap();
/// let mut b = ThetaSketchBuilder::default().build().unwrap();
/// a.update("apple");
/// b.update("apple");
///
/// let result = JaccardSimilarity::compute(&a, &b).unwrap();
/// assert_eq!(result.estimate(), 1.0);
/// assert!(result.is_similar(0.95).unwrap());
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JaccardSimilarity {
    lower_bound: f64,
    estimate: f64,
    upper_bound: f64,
}

impl JaccardSimilarity {
    #[cfg(any(feature = "theta", feature = "tuple"))]
    pub(crate) const fn new(lower_bound: f64, estimate: f64, upper_bound: f64) -> Self {
        Self {
            lower_bound,
            estimate,
            upper_bound,
        }
    }

    #[cfg(any(feature = "theta", feature = "tuple"))]
    pub(crate) const fn exact(value: f64) -> Self {
        Self::new(value, value, value)
    }

    /// Computes the Jaccard similarity index for `sketch_a` and `sketch_b` using the default
    /// update seed.
    ///
    /// Tuple summary values do not participate in the comparison, so Theta and Tuple sketches can
    /// be compared in any combination.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if both
    /// sketches are logically non-empty and either was built with a seed different from the
    /// default update seed. Empty-input fast paths return an exact result without checking sketch
    /// seed hashes.
    #[cfg(any(feature = "theta", feature = "tuple"))]
    pub fn compute(
        sketch_a: impl JaccardSketch,
        sketch_b: impl JaccardSketch,
    ) -> Result<Self, Error> {
        Self::compute_with_seed(sketch_a, sketch_b, DEFAULT_UPDATE_SEED)
    }

    /// Computes the Jaccard similarity index for `sketch_a` and `sketch_b` using `seed`.
    ///
    /// Tuple summary values do not participate in the comparison, so Theta and Tuple sketches can
    /// be compared in any combination.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `seed`
    /// hashes to the reserved zero seed hash, or if both sketches are logically non-empty and
    /// either was built with a seed incompatible with `seed`. Empty-input fast paths validate
    /// `seed` itself and then return an exact result without checking sketch seed hashes.
    #[cfg(any(feature = "theta", feature = "tuple"))]
    pub fn compute_with_seed(
        sketch_a: impl JaccardSketch,
        sketch_b: impl JaccardSketch,
        seed: u64,
    ) -> Result<Self, Error> {
        crate::thetafamily::common::jaccard_similarity::compute(seed, sketch_a, sketch_b)
    }

    /// Returns whether `sketch_a` and `sketch_b` have identical sketch state under the default
    /// update seed.
    ///
    /// Two logically empty sketches compare equal, while a logically empty sketch and a logically
    /// non-empty sketch compare unequal. Otherwise, the retained hash keys and `theta` must match.
    /// Tuple summary values are ignored. This compares sketch state rather than the original input
    /// populations.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if both
    /// sketches are logically non-empty and either was built with a seed different from the
    /// default update seed.
    #[cfg(any(feature = "theta", feature = "tuple"))]
    pub fn exactly_equal(
        sketch_a: impl JaccardSketch,
        sketch_b: impl JaccardSketch,
    ) -> Result<bool, Error> {
        Self::exactly_equal_with_seed(sketch_a, sketch_b, DEFAULT_UPDATE_SEED)
    }

    /// Returns whether `sketch_a` and `sketch_b` have identical sketch state under `seed`.
    ///
    /// Two logically empty sketches compare equal, while a logically empty sketch and a logically
    /// non-empty sketch compare unequal. Otherwise, the retained hash keys and `theta` must match.
    /// Tuple summary values are ignored. This compares sketch state rather than the original input
    /// populations.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `seed`
    /// hashes to the reserved zero seed hash, or if both sketches are logically non-empty and
    /// either was built with a seed incompatible with `seed`.
    #[cfg(any(feature = "theta", feature = "tuple"))]
    pub fn exactly_equal_with_seed(
        sketch_a: impl JaccardSketch,
        sketch_b: impl JaccardSketch,
        seed: u64,
    ) -> Result<bool, Error> {
        crate::thetafamily::common::jaccard_similarity::exactly_equal(seed, sketch_a, sketch_b)
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

    /// Tests whether the Jaccard similarity is at least `threshold` with at least 97.7%
    /// one-sided confidence (`self.lower_bound() >= threshold`).
    ///
    /// Because [`lower_bound`](Self::lower_bound) and [`upper_bound`](Self::upper_bound) form a
    /// two-sided 95.4% confidence interval (+/- 2 standard deviations), comparing `threshold`
    /// against `lower_bound` yields a one-sided test at 97.7% confidence. When `threshold` lies
    /// strictly inside `(lower_bound, upper_bound)`, both `is_similar(threshold)` and
    /// [`is_dissimilar`](Self::is_dissimilar)`(threshold)` return `Ok(false)`, indicating that
    /// the sketch sample cannot decide either way at 97.7% confidence.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// `threshold` is outside `[0.0, 1.0]` or is `NaN`.
    pub fn is_similar(&self, threshold: f64) -> Result<bool, Error> {
        validate_threshold(threshold)?;
        Ok(self.lower_bound >= threshold)
    }

    /// Tests whether the Jaccard similarity is at most `threshold` with at least 97.7%
    /// one-sided confidence (`self.upper_bound() <= threshold`).
    ///
    /// When `threshold` lies strictly inside `(lower_bound, upper_bound)`, both
    /// [`is_similar`](Self::is_similar)`(threshold)` and `is_dissimilar(threshold)` return
    /// `Ok(false)`, indicating that the sketch sample cannot decide either way at 97.7%
    /// confidence.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// `threshold` is outside `[0.0, 1.0]` or is `NaN`.
    pub fn is_dissimilar(&self, threshold: f64) -> Result<bool, Error> {
        validate_threshold(threshold)?;
        Ok(self.upper_bound <= threshold)
    }
}

fn validate_threshold(threshold: f64) -> Result<(), Error> {
    if !(0.0..=1.0).contains(&threshold) {
        return Err(Error::invalid_argument(format!(
            "threshold must be in [0.0, 1.0], got {threshold}"
        )));
    }
    Ok(())
}
