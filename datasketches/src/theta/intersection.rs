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

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::CompactThetaSketch;
use crate::theta::ThetaSketchView;
use crate::theta::hash_table::MAX_THETA;
use crate::theta::hash_table::REBUILD_THRESHOLD;
use crate::theta::hash_table::ThetaHashTable;

/// Stateful intersection operator for Theta sketches.
///
/// Before the first [`update`](Self::update), the result is undefined; use
/// [`has_result`](Self::has_result) to check.
#[derive(Debug)]
pub struct ThetaIntersection {
    is_valid: bool,
    table: ThetaHashTable,
}

impl ThetaIntersection {
    /// Creates a new intersection operator for the given `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            is_valid: false,
            table: ThetaHashTable::from_raw_parts(
                0,
                0,
                ResizeFactor::X1,
                1.0,
                MAX_THETA,
                seed,
                false,
            ),
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
        let new_default_table = |table: &ThetaHashTable| {
            ThetaHashTable::from_raw_parts(
                0,
                0,
                ResizeFactor::X1,
                1.0,
                table.theta(),
                table.hash_seed(),
                table.is_empty(),
            )
        };

        if self.table.is_empty() {
            return Ok(());
        }

        if !sketch.is_empty() && sketch.seed_hash() != self.table.seed_hash() {
            return Err(Error::invalid_argument(format!(
                "incompatible seed hash: expected {}, got {}",
                self.table.seed_hash(),
                sketch.seed_hash()
            )));
        }

        self.table
            .set_empty(self.table.is_empty() || sketch.is_empty());
        let theta = if self.table.is_empty() {
            MAX_THETA
        } else {
            self.table.theta().min(sketch.theta64())
        };
        self.table.set_theta(theta);

        if self.is_valid && self.table.num_retained() == 0 {
            return Ok(());
        }

        if sketch.num_retained() == 0 {
            self.is_valid = true;
            self.table = new_default_table(&self.table);
            return Ok(());
        }

        // first update, copy or move incoming sketch
        if !self.is_valid {
            self.is_valid = true;
            let lg_size = ThetaHashTable::lg_size_from_count_for_rebuild(
                sketch.num_retained(),
                REBUILD_THRESHOLD,
            );
            self.table = ThetaHashTable::from_raw_parts(
                lg_size,
                lg_size - 1,
                ResizeFactor::X1,
                1.0,
                self.table.theta(),
                self.table.hash_seed(),
                self.table.is_empty(),
            );
            for hash in sketch.iter() {
                if !self.table.try_insert_hash(hash) {
                    return Err(Error::invalid_argument(
                        "Insert entries from sketch fail, possibly corrupted input sketch",
                    ));
                }
            }
            // Safety check.
            if self.table.num_retained() != sketch.num_retained() {
                return Err(Error::invalid_argument(
                    "num entries mismatch, possibly corrupted input sketch",
                ));
            }
        } else {
            let max_matches = self.table.num_retained().min(sketch.num_retained());
            let mut matched_entries = Vec::with_capacity(max_matches);
            let mut count = 0;
            for hash in sketch.iter() {
                if hash < self.table.theta() {
                    if self.table.contains_hash(hash) {
                        if matched_entries.len() == max_matches {
                            return Err(Error::invalid_argument(
                                "max matches exceeded, possibly corrupted input sketch",
                            ));
                        }
                        matched_entries.push(hash);
                    }
                } else if sketch.is_ordered() {
                    break; // early stop for ordered sketches
                }
                count += 1;
            }
            // Safety check.
            if count > sketch.num_retained() {
                return Err(Error::invalid_argument(
                    "more keys than expected, possibly corrupted input sketch",
                ));
            } else if !sketch.is_ordered() && count < sketch.num_retained() {
                return Err(Error::invalid_argument(
                    "fewer keys than expected, possibly corrupted input sketch",
                ));
            }
            if matched_entries.is_empty() {
                self.table = new_default_table(&self.table);
                if self.table.theta() == MAX_THETA {
                    self.table.set_empty(true);
                }
            } else {
                let lg_size = ThetaHashTable::lg_size_from_count_for_rebuild(
                    matched_entries.len(),
                    REBUILD_THRESHOLD,
                );
                self.table = ThetaHashTable::from_raw_parts(
                    lg_size,
                    lg_size - 1,
                    ResizeFactor::X1,
                    1.0,
                    self.table.theta(),
                    self.table.hash_seed(),
                    self.table.is_empty(),
                );
                for hash in matched_entries {
                    if !self.table.try_insert_hash(hash) {
                        return Err(Error::invalid_argument(
                            "duplicate key, possibly corrupted input sketch",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Returns whether this operator has received at least one update.
    pub fn has_result(&self) -> bool {
        self.is_valid
    }

    /// Returns the intersection result as a compact theta sketch (ordered).
    ///
    /// # Panics
    ///
    /// Panics if called before the first [`update`](Self::update).
    pub fn result(&self) -> CompactThetaSketch {
        self.result_with_ordered(true)
    }

    /// Returns the intersection result as a compact theta sketch.
    ///
    /// # Panics
    ///
    /// Panics if called before the first [`update`](Self::update).
    pub fn result_with_ordered(&self, ordered: bool) -> CompactThetaSketch {
        assert!(
            self.is_valid,
            "ThetaIntersection::result() called before first update()"
        );
        let mut hashes: Vec<u64> = self.table.iter().collect();
        if ordered {
            hashes.sort_unstable();
        }
        CompactThetaSketch::from_parts(
            hashes,
            self.table.theta(),
            self.table.seed_hash(),
            ordered,
            self.table.is_empty(),
        )
    }
}
