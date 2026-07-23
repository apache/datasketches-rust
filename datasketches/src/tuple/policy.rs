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

//! Policies describing how summaries are created and updated.
//!
//! A Tuple sketch keeps a user-defined summary `S` next to every retained key. The behavior of a
//! summary is supplied externally through policy objects rather than baked into the summary type
//! itself, so the same summary type (for example a plain `u64` or a `Vec<f64>`) can be driven by
//! different behaviors and can carry per-instance configuration (such as the number of values in an
//! array-of-doubles summary).

use std::marker::PhantomData;
use std::ops::AddAssign;

/// Defines how summaries are created.
pub trait SummaryPolicy {
    /// Summary type retained alongside each key.
    type Summary;

    /// Creates a new summary for a key seen for the first time.
    ///
    /// The summary should be in its identity state; the first update value is folded in separately
    /// via [`SummaryUpdatePolicy::update`].
    fn create(&self) -> Self::Summary;
}

/// Defines how update values are folded into summaries.
///
/// A policy may implement this trait for multiple update types. For example, an array policy can
/// accept slices, vectors, or other containers while retaining a single summary type defined by
/// [`SummaryPolicy`].
pub trait SummaryUpdatePolicy<U>: SummaryPolicy {
    /// Folds an update value into an existing summary.
    fn update(&self, summary: &mut Self::Summary, value: U);
}

/// Built-in update policy for additive summaries.
///
/// The factory determines the summary type and creates its identity state. The policy accepts every
/// update type `U` for which the summary implements [`AddAssign<U>`].
#[derive(Default, Debug, Clone, Copy)]
pub struct DefaultUpdatePolicy<S> {
    marker: PhantomData<fn() -> S>,
}

impl<S> SummaryPolicy for DefaultUpdatePolicy<S>
where
    S: Default,
{
    type Summary = S;

    fn create(&self) -> Self::Summary {
        S::default()
    }
}

impl<S, U> SummaryUpdatePolicy<U> for DefaultUpdatePolicy<S>
where
    S: Default + AddAssign<U>,
{
    fn update(&self, summary: &mut Self::Summary, value: U) {
        *summary += value;
    }
}
