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
use crate::thetacommon::intersection::RawThetaIntersection;
use crate::thetacommon::intersection::RawThetaIntersectionPolicy;

/// Stateful intersection operator for Theta sketches.
///
/// Before the first [`update`](Self::update), the result is undefined; use
/// [`has_result`](Self::has_result) to check.
#[derive(Debug)]
pub struct ThetaIntersection {
    raw: RawThetaIntersection<ThetaEntry, NoopIntersectionPolicy>,
}

#[derive(Debug)]
struct NoopIntersectionPolicy;

impl RawThetaIntersectionPolicy<ThetaEntry> for NoopIntersectionPolicy {
    fn merge(&self, _existing: &mut ThetaEntry, _incoming: ThetaEntry) {}
}

impl ThetaIntersection {
    /// Creates a new intersection operator for the given `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            raw: RawThetaIntersection::new(seed, NoopIntersectionPolicy),
        }
    }

    /// Creates a new intersection operator with the default seed.
    pub fn new_with_default_seed() -> Self {
        Self::new(DEFAULT_UPDATE_SEED)
    }

    /// Updates the intersection with a given sketch.
    ///
    /// The intersection can be viewed as starting from the "universe" set,
    /// and every update can reduce the current set to leave the overlapping
    /// subset only.
    pub fn update<S: ThetaSketchView>(&mut self, sketch: &S) -> Result<(), Error> {
        self.raw.update(sketch)
    }

    /// Returns whether this operator has received at least one update.
    pub fn has_result(&self) -> bool {
        self.raw.has_result()
    }

    /// Returns the intersection result as a compact theta sketch.
    ///
    /// # Panics
    ///
    /// Panics if called before the first [`update`](Self::update).
    pub fn to_sketch(&self, ordered: bool) -> CompactThetaSketch {
        assert!(
            self.raw.has_result(),
            "ThetaIntersection::to_sketch() called before first update()"
        );
        let parts = self.raw.result(ordered);
        CompactThetaSketch::from_parts(
            parts
                .entries
                .into_iter()
                .map(|entry| entry.hash())
                .collect(),
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        )
    }
}
