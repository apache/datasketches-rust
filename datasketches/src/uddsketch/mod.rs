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

//! Uniformly collapsing logarithmic histograms for reproducible quantiles.
//!
//! [`UddSketch`] implements the insertion-only uniform-collapse strategy from
//! [UDDSketch](https://arxiv.org/abs/2004.08604), with a fixed integer bucket hierarchy.
//! Compatible sketches produce identical serialized states and estimates for the same multiset,
//! regardless of input order, partition boundaries, or merge tree. This guarantee assumes the
//! same mapping version and states built through updates, merges, and unmodified serialization
//! roundtrips. The initial mapping is deterministic and monotone.
//!
//! # Accuracy and quantile semantics
//!
//! Queries approximate the lower discrete quantile: sorted item `floor(q * (n - 1))`, using
//! zero-based indexing. There is no interpolation between ranks. Endpoints return exact extrema.
//! [`UddSketch::relative_error`] bounds relative **value** error against that item, not rank error
//! and not error against an interpolated percentile. A true zero quantile returns exactly zero.
//!
//! The initial accuracy is a target, not an unconditional bound. Exceeding the bucket budget
//! coarsens every bucket, including those near the median and tails. The final error bound is
//! computed from retained bucket extrema, including floating-point rounding and subnormal values.
//! Use [`UddSketch::relative_error`] for a guarantee even when no collapse has occurred.
//!
//! # Mapping and compatibility
//!
//! The initial index is `ceil(L(abs(x)) / (L(1 + alpha) - L(1 - alpha)))`, where `L` is
//! a fixed, monotone Q52 approximation to `log2` and `1 +/- alpha` uses `f64` arithmetic.
//! `L` normalizes the exponent and significand exactly, then evaluates a polynomial
//! with positive coefficients within sixteen significand intervals using correctly rounded basic
//! `f64` arithmetic. The resulting Q52 coordinates are combined and divided as integers. Interval
//! endpoints are capped to join monotonically. It does not depend on platform logarithms. Indices
//! are translated so that the smallest positive `f64` has index zero. Each collapse divides these
//! nonnegative indices by two using integer arithmetic. New values always traverse this same
//! hierarchy; they never use a new logarithm base after compaction. Negative values have a mirrored
//! hierarchy and zero has its own bucket. Translation lets all finite magnitudes eventually share
//! one bucket per sign, so three buckets suffice for any finite input. It changes the pairing
//! alignment from the paper's grid at one.
//!
//! Each bucket retains its count and observed extrema. Its representative is their harmonic mean
//! in magnitude, rounded and clamped to those extrema. Unlike representatives derived from powers
//! of gamma, this remains finite across the entire `f64` domain and permits a data-dependent error
//! certificate. Counts and extrema merge exactly; no order-dependent floating-point sum is stored.
//!
//! For insertion-only streams, the occupied bucket count at any fixed level never decreases.
//! A subset cannot require a coarser level than the complete multiset. Consequently every build
//! reaches the same smallest level that fits the budget. Integer parent mapping composes exactly,
//! and combining counts and extrema is associative and commutative, so merging partitions reaches
//! the same state. These arguments require identical initial parameters and exclude deletion.
//!
//! Serialization uses a versioned, Rust-specific format, not an Apache DataSketches TCK format
//! or the Timescale format. No cross-language compatibility is claimed. The mapping constants
//! and rounding rules are part of this version's bucket semantics. Deserialization validates
//! histogram structure, not provenance or minimal compaction level. Externally constructed
//! images can represent coarser histograms and need not reproduce a direct build from original
//! inputs; see [`UddSketch::deserialize`].
//!
//! # Examples
//!
//! ```
//! use datasketches::uddsketch::UddSketch;
//!
//! let mut sketch = UddSketch::new(512, 0.01)?;
//! for value in [1.0, 2.0, 3.0] {
//!     sketch.update(value)?;
//! }
//! assert_eq!(sketch.quantile(0.5)?, Some(2.0));
//! let restored = UddSketch::deserialize(&sketch.serialize())?;
//! sketch.merge(&restored)?;
//! assert_eq!(sketch.total_weight(), 6);
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod mapping;
mod sketch;
pub use self::sketch::UddSketch;
