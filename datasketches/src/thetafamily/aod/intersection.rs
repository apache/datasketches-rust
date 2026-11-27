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

use std::fmt;

use crate::aod::ArrayOfDoublesCombiner;
use crate::aod::ArrayOfDoublesSketchView;
use crate::aod::CompactArrayOfDoublesSketch;
use crate::aod::hash_table::AodCompactData;
use crate::aod::hash_table::AodHashTable;
use crate::aod::policy::check_num_values;
use crate::aod::policy::validate_num_values;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::thetafamily::common::constants::HASH_TABLE_REBUILD_THRESHOLD;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::hash_table::SketchHashTable;

/// Stateful intersection operator for ArrayOfDoubles sketches.
///
/// The caller supplies an [`ArrayOfDoublesCombiner`] for values associated with shared keys. A
/// newly created operator has no result until its first successful [`update`](Self::update).
///
/// # Examples
///
/// ```
/// use datasketches::aod::ArrayOfDoublesCombiner;
/// use datasketches::aod::ArrayOfDoublesIntersection;
/// use datasketches::aod::ArrayOfDoublesSketchBuilder;
///
/// struct SumCombiner;
/// impl ArrayOfDoublesCombiner for SumCombiner {
///     fn combine(&self, existing: &mut [f64], incoming: &[f64]) {
///         for (dst, src) in existing.iter_mut().zip(incoming) {
///             *dst += *src;
///         }
///     }
/// }
///
/// let mut a = ArrayOfDoublesSketchBuilder::default().build()?;
/// a.update("shared", [3.0])?;
/// let mut b = ArrayOfDoublesSketchBuilder::default().build()?;
/// b.update("shared", [4.0])?;
///
/// let mut intersection = ArrayOfDoublesIntersection::new(1, SumCombiner)?;
/// intersection.update(&a)?;
/// intersection.update(&b)?;
/// assert_eq!(
///     intersection
///         .to_sketch(true)
///         .unwrap()
///         .iter()
///         .next()
///         .unwrap()
///         .values(),
///     &[7.0]
/// );
/// # Ok::<(), datasketches::error::Error>(())
/// ```
pub struct ArrayOfDoublesIntersection<C>
where
    C: ArrayOfDoublesCombiner,
{
    table: AodHashTable,
    combiner: C,
    result_state: AodIntersectionState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AodIntersectionState {
    Uninitialized,
    Empty,
    NonEmpty,
}

impl<C> fmt::Debug for ArrayOfDoublesIntersection<C>
where
    C: ArrayOfDoublesCombiner,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ArrayOfDoublesIntersection");
        debug
            .field("seed_hash", &self.table.seed_hash())
            .field("num_values", &self.table.num_values())
            .field("has_result", &self.has_result());
        match self.result_state {
            AodIntersectionState::Uninitialized => {}
            AodIntersectionState::Empty => {
                debug
                    .field("is_empty", &true)
                    .field("theta64", &MAX_THETA)
                    .field("num_retained", &0usize);
            }
            AodIntersectionState::NonEmpty => {
                debug
                    .field("is_empty", &false)
                    .field("theta64", &self.table.retention_theta())
                    .field("num_retained", &self.table.num_retained());
            }
        }
        debug.finish()
    }
}

impl<C> ArrayOfDoublesIntersection<C>
where
    C: ArrayOfDoublesCombiner,
{
    /// Creates an intersection operator with the default seed and the given configuration.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `num_values` is zero.
    pub fn new(num_values: u8, combiner: C) -> Result<Self, Error> {
        Self::with_seed(num_values, combiner, DEFAULT_UPDATE_SEED)
    }

    /// Creates an intersection operator with the given configuration and `seed`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `num_values` is zero or the computed seed hash is zero.
    pub fn with_seed(num_values: u8, combiner: C, seed: u64) -> Result<Self, Error> {
        validate_num_values(num_values)?;
        let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
        Ok(Self {
            table: AodHashTable::for_intersection(0, 0, MAX_THETA, seed, seed_hash, num_values),
            combiner,
            result_state: AodIntersectionState::Uninitialized,
        })
    }

    /// Updates the intersection with a mutable or compact sketch.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` without modifying the intersection if `sketch` has a different
    /// `num_values`, a non-empty `sketch` has a different seed hash, or the input appears
    /// corrupted.
    pub fn update<'a>(
        &mut self,
        sketch: impl Into<ArrayOfDoublesSketchView<'a>>,
    ) -> Result<(), Error> {
        let sketch = sketch.into();
        let num_values = self.table.num_values();
        let seed_hash = self.table.seed_hash();
        check_num_values(
            num_values,
            usize::from(sketch.num_values()),
            "intersection update",
        )?;
        if !sketch.is_empty() {
            check_seed_hash(
                seed_hash,
                sketch.seed_hash(),
                "ArrayOfDoubles intersection input",
                ErrorKind::InvalidArgument,
            )?;
        }
        if self.result_state == AodIntersectionState::Empty {
            return Ok(());
        }

        let seed = self.table.seed();
        let new_table = |count, theta| {
            let lg_size = if count == 0 {
                0
            } else {
                SketchHashTable::<u64>::lg_size_from_count_for_rebuild(
                    count,
                    HASH_TABLE_REBUILD_THRESHOLD,
                )
            };
            AodHashTable::for_intersection(
                lg_size,
                lg_size.saturating_sub(1),
                theta,
                seed,
                seed_hash,
                num_values,
            )
        };
        if sketch.is_empty() {
            self.table = new_table(0, MAX_THETA);
            self.result_state = AodIntersectionState::Empty;
            return Ok(());
        }

        let result_theta = self.table.retention_theta().min(sketch.theta64());
        let input_count = sketch.num_retained();
        if input_count == 0 {
            self.table = new_table(0, result_theta);
            self.result_state = AodIntersectionState::NonEmpty;
            return Ok(());
        }
        if self.result_state == AodIntersectionState::NonEmpty && self.table.num_retained() == 0 {
            if result_theta < self.table.retention_theta() {
                self.table.set_retention_theta(result_theta);
            }
            return Ok(());
        }

        if self.result_state == AodIntersectionState::Uninitialized {
            let mut next_table = new_table(input_count, result_theta);
            for entry in sketch.iter() {
                next_table
                    .insert_unique(entry.hash(), entry.values())
                    .ok_or_else(|| {
                        Error::invalid_argument("invalid or duplicate hash in intersection input")
                    })?;
            }
            if next_table.num_retained() != input_count {
                return Err(Error::invalid_argument(
                    "retained count mismatch in intersection input",
                ));
            }
            self.table = next_table;
            self.result_state = AodIntersectionState::NonEmpty;
            return Ok(());
        }

        let max_matches = input_count.min(self.table.num_retained());
        let mut next_table = new_table(max_matches, result_theta);
        let ordered = sketch.is_ordered();
        let mut count = 0;
        for entry in sketch.iter() {
            let hash = entry.hash();
            if hash < result_theta {
                if let Some(existing) = self.table.find_values(hash) {
                    if next_table.num_retained() == max_matches {
                        return Err(Error::invalid_argument(
                            "intersection match count exceeds input size",
                        ));
                    }
                    let slot = next_table.insert_unique(hash, existing).ok_or_else(|| {
                        Error::invalid_argument("duplicate hash in intersection input")
                    })?;
                    self.combiner.combine(slot, entry.values());
                }
            } else if ordered {
                break;
            }
            count += 1;
        }
        if count > input_count || (!ordered && count < input_count) {
            return Err(Error::invalid_argument(
                "retained count mismatch in intersection input",
            ));
        }

        let mut state = AodIntersectionState::NonEmpty;
        if next_table.num_retained() == 0 {
            next_table = new_table(0, result_theta);
            if result_theta == MAX_THETA {
                state = AodIntersectionState::Empty;
            }
        }
        self.table = next_table;
        self.result_state = state;
        Ok(())
    }

    /// Returns `true` after the first successful [`update`](Self::update).
    pub fn has_result(&self) -> bool {
        self.result_state != AodIntersectionState::Uninitialized
    }

    /// Returns an estimate of heap memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.table.estimated_size()
    }

    /// Returns the current intersection as a compact sketch.
    ///
    /// Returns `None` until the first successful [`update`](Self::update). After that, returns
    /// `Some` even when the intersection is empty. If `ordered` is `true`, retained entries are
    /// sorted ascending by hash.
    pub fn to_sketch(&self, ordered: bool) -> Option<CompactArrayOfDoublesSketch> {
        let data = match self.result_state {
            AodIntersectionState::Uninitialized => return None,
            AodIntersectionState::Empty => {
                AodCompactData::empty(self.table.seed_hash(), self.table.num_values())
            }
            AodIntersectionState::NonEmpty => self.table.to_non_empty_compact_data(ordered),
        };
        Some(CompactArrayOfDoublesSketch::from_compact_data(data))
    }
}
