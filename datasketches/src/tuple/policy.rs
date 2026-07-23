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

/// Defines how a summary is created and how update values are folded into it.
///
/// A policy determines both the stored summary type and the family of values accepted by
/// [`update`](Self::update). The generic associated update type allows a policy to accept borrowed
/// values, such as `&[f64]`, without putting a lifetime on the policy itself.
pub trait SummaryUpdatePolicy {
    /// Summary type retained alongside each key.
    type Summary;

    /// Update value accepted by this policy.
    type Update<'a>;

    /// Creates a new summary for a key seen for the first time.
    ///
    /// The summary should be in its identity state; the first update value is folded in separately
    /// via [`update`](Self::update).
    fn create(&self) -> Self::Summary;

    /// Folds an update value into an existing summary.
    fn update(&self, summary: &mut Self::Summary, value: Self::Update<'_>);
}

/// Default update policy for summaries that are default-constructible and additive.
///
/// The summary type is selected when
/// [`TupleSketchBuilder::build`](crate::tuple::TupleSketchBuilder::build) is called, and updates fold
/// values with `summary += value`. The zero-sized type witness records that this concrete policy's
/// associated summary is `S`.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultUpdatePolicy<S>(PhantomData<fn() -> S>);

impl<S> SummaryUpdatePolicy for DefaultUpdatePolicy<S>
where
    S: Default + AddAssign<S>,
{
    type Summary = S;
    type Update<'a> = S;

    fn create(&self) -> Self::Summary {
        S::default()
    }

    fn update(&self, summary: &mut Self::Summary, value: Self::Update<'_>) {
        *summary += value;
    }
}
