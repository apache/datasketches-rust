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

use crate::aod::ArrayOfDoublesSketchView;
use crate::aod::CompactArrayOfDoublesSketch;
use crate::aod::hash_table::AodHashTable;
use crate::aod::policy::check_num_values;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::check_seed_hash;
use crate::thetafamily::common::constants::DEFAULT_LG_K;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::sort::sort_by_hash_key;

/// Stateful union operator for ArrayOfDoubles sketches.
///
/// Values associated with a key present in multiple inputs are added element by element.
///
/// # Examples
///
/// ```
/// use datasketches::aod::ArrayOfDoublesSketchBuilder;
/// use datasketches::aod::ArrayOfDoublesUnionBuilder;
///
/// let mut a = ArrayOfDoublesSketchBuilder::default()
///     .num_values(2)
///     .build()?;
/// a.update("shared", [1.0, 2.0])?;
/// let mut b = ArrayOfDoublesSketchBuilder::default()
///     .num_values(2)
///     .build()?;
/// b.update("shared", [10.0, 20.0])?;
///
/// let mut union = ArrayOfDoublesUnionBuilder::default()
///     .num_values(2)
///     .build()?;
/// union.update(&a)?;
/// union.update(&b)?;
///
/// let result = union.to_sketch(true);
/// assert_eq!(result.iter().next().unwrap().values(), &[11.0, 22.0]);
/// # Ok::<(), datasketches::error::Error>(())
/// ```
pub struct ArrayOfDoublesUnion {
    pub(super) table: AodHashTable,
    // None until the union receives a non-empty input sketch.
    result_theta: Option<u64>,
}

impl fmt::Debug for ArrayOfDoublesUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let is_empty = self.result_theta.is_none();
        let union_theta = self
            .result_theta
            .map_or(MAX_THETA, |theta| theta.min(self.table.retention_theta));
        let num_retained = match self.result_theta {
            None => 0,
            Some(theta) if theta >= self.table.retention_theta => self.table.num_retained,
            Some(theta) => self
                .table
                .iter()
                .filter(|entry| entry.hash() < theta)
                .count(),
        };
        f.debug_struct("ArrayOfDoublesUnion")
            .field("lg_k", &self.table.lg_nom_size)
            .field("seed_hash", &self.table.seed_hash)
            .field("num_values", &self.table.num_values)
            .field("is_empty", &is_empty)
            .field("union_theta", &union_theta)
            .field("num_retained", &num_retained)
            .finish()
    }
}

pub(super) struct CompactUnionSlots {
    pub theta: u64,
    pub is_ordered: bool,
    pub slots: Vec<(u64, u32)>,
}

impl ArrayOfDoublesUnion {
    /// Merges a mutable or compact sketch into the union.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` without modifying the union if `sketch` has a different
    /// `num_values` or a non-empty `sketch` has a different seed hash.
    pub fn update<'a>(
        &mut self,
        sketch: impl Into<ArrayOfDoublesSketchView<'a>>,
    ) -> Result<(), Error> {
        let sketch = sketch.into();
        check_num_values(
            self.table.num_values,
            usize::from(sketch.num_values()),
            "union update",
        )?;
        if sketch.is_empty() {
            return Ok(());
        }
        check_seed_hash(
            self.table.seed_hash,
            sketch.seed_hash(),
            "union update",
            ErrorKind::InvalidArgument,
        )?;

        let current_theta = self.result_theta.unwrap_or(self.table.retention_theta);
        let result_theta = current_theta.min(sketch.theta64());
        self.result_theta = Some(result_theta);
        let ordered = sketch.is_ordered();
        for entry in sketch.iter() {
            let hash = entry.hash();
            if hash < result_theta && hash < self.table.retention_theta {
                self.table.insert_or_add(hash, entry.values());
            } else if ordered {
                break;
            }
        }
        self.result_theta = Some(result_theta.min(self.table.retention_theta));
        Ok(())
    }

    pub(super) fn compact_slots(&self, ordered: bool) -> Option<CompactUnionSlots> {
        let result_theta = self.result_theta?;
        let mut theta = result_theta.min(self.table.retention_theta);
        let mut slots = Vec::with_capacity(self.table.num_retained);
        for (slot, &hash) in self.table.keys.iter().enumerate() {
            if hash != 0 && hash < theta {
                slots.push((hash, slot as u32));
            }
        }
        let nominal_num = 1usize << self.table.lg_nom_size;
        if slots.len() > nominal_num {
            let (_, kth, _) = slots.select_nth_unstable_by_key(nominal_num, |&(hash, _)| hash);
            theta = kth.0;
            slots.truncate(nominal_num);
        }
        if ordered && slots.len() > 1 {
            sort_by_hash_key(&mut slots, |&(hash, _)| hash);
        }
        let is_ordered = ordered || (slots.len() == 1 && theta == MAX_THETA);
        Some(CompactUnionSlots {
            theta,
            is_ordered,
            slots,
        })
    }

    /// Returns the union result as a compact sketch.
    ///
    /// If `ordered` is `true`, retained entries are sorted ascending by hash.
    pub fn to_sketch(&self, ordered: bool) -> CompactArrayOfDoublesSketch {
        match self.compact_slots(ordered) {
            Some(CompactUnionSlots {
                theta,
                is_ordered,
                slots,
            }) => self.table.materialize_compact(theta, is_ordered, &slots),
            None => CompactArrayOfDoublesSketch::empty(self.table.seed_hash, self.table.num_values),
        }
    }

    /// Serializes the union result directly into the compact ArrayOfDoubles binary format.
    ///
    /// Produces the same retained entries, theta, and seed hash as
    /// `self.to_sketch(ordered).serialize()`, and identical bytes when `ordered` is `true`,
    /// without allocating an intermediate compact sketch's value buffer.
    ///
    /// The serialized data can be read by other DataSketches implementations when `num_values` is
    /// in `1..=127`. Values in `128..=255` can be read by DataSketches C++ and Go, but not by
    /// DataSketches Java.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::aod::ArrayOfDoublesSketchBuilder;
    /// use datasketches::aod::ArrayOfDoublesUnionBuilder;
    /// use datasketches::aod::CompactArrayOfDoublesSketch;
    ///
    /// let mut sketch = ArrayOfDoublesSketchBuilder::default().build()?;
    /// sketch.update("apple", [1.0])?;
    ///
    /// let mut union = ArrayOfDoublesUnionBuilder::default().build()?;
    /// union.update(&sketch)?;
    ///
    /// let bytes = union.serialize(true);
    /// let decoded = CompactArrayOfDoublesSketch::deserialize(&bytes)?;
    /// assert_eq!(decoded.num_retained(), 1);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn serialize(&self, ordered: bool) -> Vec<u8> {
        crate::aod::serialization::serialize_union(self, ordered)
    }

    /// Resets the union to its initial empty state.
    pub fn reset(&mut self) {
        self.table.reset();
        self.result_theta = None;
    }

    /// Returns an estimate of heap memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.table.estimated_size()
    }
}

/// Builder for [`ArrayOfDoublesUnion`].
///
/// Configuration is stored without validation and checked when [`build`](Self::build) is called.
#[derive(Debug, Clone)]
pub struct ArrayOfDoublesUnionBuilder {
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    num_values: u8,
}

impl Default for ArrayOfDoublesUnionBuilder {
    fn default() -> Self {
        Self {
            lg_k: DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: DEFAULT_UPDATE_SEED,
            num_values: 1,
        }
    }
}

impl ArrayOfDoublesUnionBuilder {
    /// Sets `lg_k`, the base-2 logarithm of the nominal capacity (`5..=26`).
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        self.lg_k = lg_k;
        self
    }

    /// Sets the hash-table resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Sets the initial uniform sampling probability in `(0.0, 1.0]`.
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        self.sampling_probability = probability;
        self
    }

    /// Sets the hash seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Sets the number of `f64` values associated with each key (`1..=255`).
    pub fn num_values(mut self, num_values: u8) -> Self {
        self.num_values = num_values;
        self
    }

    /// Builds an [`ArrayOfDoublesUnion`].
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `num_values` is zero, `lg_k` is outside `5..=26`,
    /// `sampling_probability` is outside `(0.0, 1.0]`, or the computed seed hash is zero.
    pub fn build(self) -> Result<ArrayOfDoublesUnion, Error> {
        let table = AodHashTable::new(
            self.lg_k,
            self.resize_factor,
            self.sampling_probability,
            self.seed,
            self.num_values,
        )?;
        Ok(ArrayOfDoublesUnion {
            table,
            result_theta: None,
        })
    }
}
