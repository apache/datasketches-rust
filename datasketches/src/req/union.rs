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

//! REQ union — combines REQ sketches into a single result.

use super::RankAccuracy;
use super::sketch::ReqSketch;
use super::value::ReqValue;
use crate::error::Error;

/// Combines multiple [`ReqSketch`]es into a single result.
///
/// Internally wraps a `ReqSketch` configured for union semantics. 
#[derive(Debug, Clone)]
pub struct ReqUnion<T: ReqValue> {
    inner: ReqSketch<T>,
}

impl<T: ReqValue> ReqUnion<T> {
    /// Creates a new union with default `k = 12` and `RankAccuracy::HighRank`.
    pub fn new() -> Self {
        Self {
            inner: ReqSketch::new(),
        }
    }

    /// Creates a new union with the given `k` and rank accuracy.
    ///
    /// # Errors
    ///
    /// Returns an error if `k` is invalid (see [`ReqSketch::try_new`]).
    pub fn try_new(k: u16, rank_accuracy: RankAccuracy) -> Result<Self, Error> {
        Ok(Self {
            inner: ReqSketch::try_new(k, rank_accuracy)?,
        })
    }

    /// Returns the configured `k` parameter.
    pub fn k(&self) -> u16 {
        self.inner.k()
    }

    /// Returns the configured rank accuracy.
    pub fn rank_accuracy(&self) -> RankAccuracy {
        self.inner.rank_accuracy()
    }

    /// Returns true if the union has not yet absorbed any sketch.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Merges a sketch into the union.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch's `rank_accuracy` differs from the union's.
    pub fn merge(&mut self, sketch: &ReqSketch<T>) -> Result<(), Error> {
        self.inner.merge(sketch)
    }

    /// Extracts the merged result as a [`ReqSketch`].
    ///
    /// Equivalent to C++ `req_union::get_result`. Renamed per the workspace's
    /// CPC PR #81 precedent.
    pub fn to_sketch(&self) -> ReqSketch<T> {
        self.inner.clone()
    }

    /// Resets the union to empty.
    pub fn reset(&mut self) {
        self.inner.reset();
    }
}

impl<T: ReqValue> Default for ReqUnion<T> {
    fn default() -> Self {
        Self::new()
    }
}
