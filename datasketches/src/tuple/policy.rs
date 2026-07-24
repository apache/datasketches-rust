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

//! Policies describing how summaries are created, updated, and combined.
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

/// Defines how two summaries that share the same key are combined.
///
/// This is used by both union and intersection. Each operator is given its own policy instance,
/// because the two operations may combine summaries differently for the same summary type.
pub trait SummaryCombinePolicy: SummaryPolicy {
    /// Combines `other` into `summary` in place.
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary);
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

/// Built-in combine policy for additive summaries, used by the union when no custom policy is
/// given.
///
/// It combines two summaries with `summary += other` and is available for any summary type `S`
/// where `S: AddAssign<&S>`.
///
/// There is intentionally no default combine policy for the intersection: how to combine summaries
/// of the keys present in both inputs is application-specific, so the intersection always requires
/// an explicit policy.
#[derive(Default, Debug, Clone, Copy)]
pub struct DefaultUnionPolicy<S> {
    marker: PhantomData<fn() -> S>,
}

impl<S> SummaryPolicy for DefaultUnionPolicy<S>
where
    S: Default,
{
    type Summary = S;

    fn create(&self) -> Self::Summary {
        S::default()
    }
}

impl<S> SummaryCombinePolicy for DefaultUnionPolicy<S>
where
    S: Default + for<'a> AddAssign<&'a S>,
{
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
        *summary += other;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_update_policy_update_accumulates() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut summary = 0u64;
        policy.update(&mut summary, 3);
        policy.update(&mut summary, 4);
        assert_eq!(summary, 7);
    }

    /// A non-trivial custom policy (keeps the maximum) to exercise the traits beyond the additive
    /// default.
    #[derive(Debug, Default, Clone, Copy)]
    struct MaxPolicy;

    impl SummaryPolicy for MaxPolicy {
        type Summary = u64;

        fn create(&self) -> Self::Summary {
            0
        }
    }

    impl SummaryUpdatePolicy<u64> for MaxPolicy {
        fn update(&self, summary: &mut Self::Summary, value: u64) {
            *summary = (*summary).max(value);
        }
    }

    impl SummaryCombinePolicy for MaxPolicy {
        fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
            self.update(summary, *other);
        }
    }

    #[test]
    fn custom_update_policy_keeps_max() {
        let policy = MaxPolicy;
        let mut summary = policy.create();
        policy.update(&mut summary, 3);
        policy.update(&mut summary, 7);
        policy.update(&mut summary, 2);
        assert_eq!(summary, 7);
    }

    #[test]
    fn custom_combine_policy_keeps_max() {
        let policy = MaxPolicy;
        let mut summary = 5u64;
        policy.combine(&mut summary, &10);
        policy.combine(&mut summary, &7);
        assert_eq!(summary, 10);
    }

    #[test]
    fn default_union_policy_combines_additively() {
        let policy = DefaultUnionPolicy::<u64>::default();
        let mut summary = 5u64;
        policy.combine(&mut summary, &10);
        policy.combine(&mut summary, &7);
        assert_eq!(summary, 22);
    }
}
