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
//!
//! This mirrors the policy approach used by the C++ implementation
//! (`default_tuple_update_policy`, `default_tuple_union_policy`) and the Java
//! `SummaryFactory` / `SummarySetOperations` interfaces.

use std::ops::AddAssign;

use crate::thetacommon::intersection::RawThetaIntersectionPolicy;
use crate::thetacommon::union::RawThetaUnionPolicy;
use crate::tuple::hash_table::TupleEntry;

/// Adapts a [`SummaryCombinePolicy`] to the raw set operations' entry-merge policies.
#[derive(Debug)]
pub(crate) struct CombinePolicyAdapter<P>(pub(crate) P);

impl<S, P> RawThetaUnionPolicy<TupleEntry<S>> for CombinePolicyAdapter<P>
where
    P: SummaryCombinePolicy<S>,
{
    fn merge(&self, existing: &mut TupleEntry<S>, incoming: TupleEntry<S>) {
        self.0.combine(existing.summary_mut(), incoming.summary());
    }
}

impl<S, P> RawThetaIntersectionPolicy<TupleEntry<S>> for CombinePolicyAdapter<P>
where
    P: SummaryCombinePolicy<S>,
{
    fn merge(&self, existing: &mut TupleEntry<S>, incoming: TupleEntry<S>) {
        self.0.combine(existing.summary_mut(), incoming.summary());
    }
}

/// Defines how a summary is created and how update values are folded into it.
///
/// This is used by the update tuple sketch. `S` is the stored summary type and `U` is the type of
/// the update value, which may be a borrowed type such as `&[f64]`.
///
/// Corresponds to C++ `default_tuple_update_policy<Summary, Update>` and Java
/// `UpdatableSummary<U>` together with `SummaryFactory`.
pub trait SummaryUpdatePolicy<S, U> {
    /// Creates a new summary for a key seen for the first time.
    ///
    /// The summary should be in its identity state; the first update value is folded in separately
    /// via [`update`](Self::update).
    fn create(&self) -> S;

    /// Folds an update value into an existing summary.
    fn update(&self, summary: &mut S, value: U);
}

/// Defines how two summaries that share the same key are combined.
///
/// This is used by both union and intersection. Each operator is given its own policy instance,
/// because the two operations may combine summaries differently for the same summary type.
///
/// Corresponds to C++ `default_tuple_union_policy` / the tuple intersection policy and Java
/// `SummarySetOperations`.
pub trait SummaryCombinePolicy<S> {
    /// Combines `other` into `summary` in place.
    fn combine(&self, summary: &mut S, other: &S);
}

/// Default update policy for summaries that are default-constructible and additive.
///
/// This is the convenience policy used when no custom policy is supplied, equivalent to C++
/// `default_tuple_update_policy` (which folds updates with `summary += update`). It is available
/// for any summary type `S` and update type `U` where `S: Default + AddAssign<U>`.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultUpdatePolicy;

impl<S, U> SummaryUpdatePolicy<S, U> for DefaultUpdatePolicy
where
    S: Default + AddAssign<U>,
{
    fn create(&self) -> S {
        S::default()
    }

    fn update(&self, summary: &mut S, value: U) {
        *summary += value;
    }
}

/// Default combine policy for additive summaries, used by the union when no custom policy is given.
///
/// This is equivalent to C++ `default_tuple_union_policy`, which combines two summaries with
/// `summary += other`. It is available for any summary type `S` where `S: AddAssign<&S>`.
///
/// There is intentionally no default combine policy for the intersection: how to combine summaries
/// of the keys present in both inputs is application-specific, so the intersection always requires
/// an explicit policy.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultUnionPolicy;

impl<S> SummaryCombinePolicy<S> for DefaultUnionPolicy
where
    for<'a> S: AddAssign<&'a S>,
{
    fn combine(&self, summary: &mut S, other: &S) {
        *summary += other;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_update_policy_update_accumulates() {
        let policy = DefaultUpdatePolicy;
        let mut summary = 0u64;
        policy.update(&mut summary, 3);
        policy.update(&mut summary, 4);
        assert_eq!(summary, 7);
    }

    /// A non-trivial custom policy (keeps the maximum) to exercise the traits beyond the additive
    /// default.
    #[derive(Debug, Default, Clone, Copy)]
    struct MaxPolicy;

    impl SummaryUpdatePolicy<u64, u64> for MaxPolicy {
        fn create(&self) -> u64 {
            0
        }

        fn update(&self, summary: &mut u64, value: u64) {
            *summary = (*summary).max(value);
        }
    }

    impl SummaryCombinePolicy<u64> for MaxPolicy {
        fn combine(&self, summary: &mut u64, other: &u64) {
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
        let policy = DefaultUnionPolicy;
        let mut summary = 5u64;
        policy.combine(&mut summary, &10);
        policy.combine(&mut summary, &7);
        assert_eq!(summary, 22);
    }
}
