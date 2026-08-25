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
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::CompactThetaSketch;
use crate::theta::ThetaSketchView;
use crate::theta::hash_table::ThetaEntry;
use crate::thetacommon::intersection::IntersectionMergePolicy;
use crate::thetacommon::intersection::IntersectionState;

/// Stateful intersection operator for Theta sketches.
///
/// A newly created operator has no result. [`has_result`](Self::has_result) returns `false` and
/// [`to_sketch`](Self::to_sketch) returns `None` until the first successful
/// [`update`](Self::update).
#[derive(Debug)]
pub struct ThetaIntersection {
    state: IntersectionState<ThetaEntry, NoopIntersectionPolicy>,
}

impl Default for ThetaIntersection {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED)
    }
}

#[derive(Debug)]
struct NoopIntersectionPolicy;

impl IntersectionMergePolicy<ThetaEntry> for NoopIntersectionPolicy {
    fn merge(&self, _existing: &mut ThetaEntry, _incoming: ThetaEntry) {}
}

impl ThetaIntersection {
    /// Creates a new intersection operator for the given `seed`.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            state: IntersectionState::new(seed, NoopIntersectionPolicy),
        }
    }

    /// Updates the intersection with a given sketch.
    ///
    /// The intersection can be viewed as starting from the "universe" set,
    /// and every update can reduce the current set to leave the overlapping
    /// subset only.
    pub fn update<'a>(&mut self, sketch: impl Into<ThetaSketchView<'a>>) -> Result<(), Error> {
        let sketch = sketch.into();
        self.state.update(sketch)
    }

    /// Returns `true` after the first successful [`update`](Self::update).
    pub fn has_result(&self) -> bool {
        self.state.has_result()
    }

    /// Returns the estimated size of the intersection in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.state.estimated_size()
    }

    /// Returns the current intersection as a compact theta sketch.
    ///
    /// Returns `None` until the first successful [`update`](Self::update). After that, returns
    /// `Some` even when the intersection is empty.
    ///
    /// If `ordered` is `true`, retained hashes are sorted in ascending order.
    pub fn to_sketch(&self, ordered: bool) -> Option<CompactThetaSketch> {
        if !self.state.has_result() {
            return None;
        }
        let parts = self.state.to_compact_parts(ordered);
        Some(CompactThetaSketch::from_parts(
            parts
                .entries
                .into_iter()
                .map(|entry| entry.hash())
                .collect(),
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        ))
    }
}
