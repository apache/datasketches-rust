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

/// Jaccard similarity estimate and confidence bounds for two sketches.
///
/// The bounds use a two-sided 95.4% confidence interval, equivalent to +/- 2 standard deviations.
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
