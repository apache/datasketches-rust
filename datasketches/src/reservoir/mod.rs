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

//! Reservoir sampling sketch and union for uniform unweighted random samples over streams.
//!
//! # Overview
//!
//! [`ReservoirSketch`] maintains a uniform random sample of up to `k` unweighted items observed
//! from a stream of unknown length in a single pass using Vitter's Algorithm R. While the total
//! stream weight is at most `k`, the sketch operates in exact mode and retains every observed item.
//! Once the stream exceeds `k` items, each retained sample represents an implicit stream weight of
//! `total_weight / k`, and every item seen so far has equal marginal inclusion probability `k / n`.
//!
//! [`ReservoirSketch::estimate_subset_sum`] evaluates an arbitrary predicate over the retained
//! sample to estimate the total count of matching items across the full stream, returning a
//! [`SampleSubsetSummary`] with heuristic confidence bounds at roughly two standard deviations.
//!
//! # Union semantics
//!
//! [`ReservoirUnion`] combines multiple [`ReservoirSketch`] instances (and individual items) up to
//! a configured `max_k` capacity:
//!
//! * Input sketches with `k > max_k` are downsampled to `max_k` before merging.
//! * Input sketches in exact mode (`total_weight <= k`) with `k < max_k` can be promoted into a
//!   `max_k` gadget without losing exactness.
//! * Combining reservoirs of different sampling capacities bounds the output capacity by
//!   `min(max_k, k_1, k_2, ...)`. Every item in the combined stream retains a uniform marginal
//!   inclusion probability, though higher-order permutation statistics are not preserved across
//!   weighted merges.
//!
//! # Cross-language compatibility
//!
//! [`ReservoirSketch`] (`Family::RESERVOIR`, ID `11`) and [`ReservoirUnion`]
//! (`Family::RESERVOIR_UNION`, ID `12`) serialize using version `2` of the DataSketches reservoir
//! binary format and accept both version `2` and legacy version `1` (`ReservoirSize`-encoded `k`)
//! images during deserialization.
//!
//! Cross-language interchange with DataSketches Java (`ReservoirLongsSketch`,
//! `ReservoirItemsSketch`, `ReservoirLongsUnion`, `ReservoirItemsUnion`) and DataSketches Go
//! (`ReservoirItemsSketch`, `ReservoirItemsUnion`) is supported out of the box when using `i64`,
//! `f64`, or `String` items.
//!
//! # Examples
//!
//! ```
//! use datasketches::reservoir::ReservoirSketch;
//! use datasketches::reservoir::ReservoirUnion;
//!
//! let mut left = ReservoirSketch::<i64>::new(64)?;
//! for value in 0..500 {
//!     left.update(value);
//! }
//!
//! let mut right = ReservoirSketch::<i64>::new(64)?;
//! for value in 500..1_000 {
//!     right.update(value);
//! }
//!
//! let mut union = ReservoirUnion::<i64>::new(64)?;
//! union.update(&left);
//! union.update(&right);
//!
//! let merged = union.to_sketch();
//! assert_eq!(merged.total_weight(), 1_000);
//! assert_eq!(merged.num_retained(), 64);
//!
//! let summary = merged.estimate_subset_sum(|&x| x < 500);
//! assert!(summary.lower_bound() <= summary.estimate());
//! assert!(summary.estimate() <= summary.upper_bound());
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod serialization;
mod size;
mod sketch;
mod union;
mod value;

pub use self::sketch::ReservoirSketch;
pub use self::sketch::SampleSubsetSummary;
pub use self::union::ReservoirUnion;
pub use self::value::ReservoirValue;

/// Minimum reservoir capacity `k` (required by the lightness invariant in two-way weighted merges).
const MIN_K: usize = 2;

/// Maximum reservoir capacity `k`, matching the signed 32-bit wire field in the DataSketches
/// reservoir preamble.
const MAX_K: usize = i32::MAX as usize;

/// Smallest initial internal allocation exponent (`2^4 = 16` items).
const MIN_LG_ARR_ITEMS: u8 = 4;

/// Maximum total items seen (`2^48 - 1`), bounded below 53-bit `f64` precision so weighted merge
/// acceptance probabilities remain accurate near capacity.
const MAX_ITEMS_SEEN: u64 = 0xFFFF_FFFF_FFFF;

/// Default number of standard deviations (`2.0`) used for subset-sum confidence bounds.
const DEFAULT_KAPPA: f64 = 2.0;
