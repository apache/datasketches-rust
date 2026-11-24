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

//! Uniform DDSketch (`UDDSketch`) for reproducible relative-error quantile estimation.
//!
//! [`UddSketch`] summarizes streams of finite `f64` values using logarithmically spaced counter
//! buckets and bounded memory. When the number of occupied nonzero buckets exceeds the configured
//! budget, all buckets are collapsed uniformly two-by-two, doubling the logarithmic bucket width
//! across the entire domain rather than dropping accuracy only at one tail.
//!
//! The algorithm is based on [UDDSketch: Accurate Tracking of Quantiles in Data Streams][uddsketch]
//! by Italo Epicoco, Catiuscia Melle, Massimo Cafaro, Marco Pulimeno, and Giuseppe Morleo, and its
//! [mergeability analysis][mergeability] by Massimo Cafaro, Catiuscia Melle, Italo Epicoco, and
//! Marco Pulimeno.
//!
//! # Reproducibility and accuracy
//!
//! [`UddSketch`] uses a deterministic fixed-point binary logarithm mapping and integer parent
//! bucket collapsing. For sketches configured with identical `max_buckets` and `initial_alpha`,
//! insertion-only streams (via [`UddSketch::update`], [`UddSketch::update_with_weight`], and
//! [`UddSketch::merge`]) produce identical states and quantile estimates regardless of input order,
//! partitioning, or merge tree topology.
//!
//! Quantile queries target the lower discrete `q`-quantile at zero-based sorted index
//! `floor(q * (total_weight - 1))`. Ranks `0.0` and `1.0` return the exact stream minimum and
//! maximum, and ranks falling in the zero bucket return `0.0`. For nonzero estimates over normal
//! `f64` values, [`UddSketch::alpha`] reports the effective relative value error bound after any
//! uniform collapses.
//!
//! [uddsketch]: https://arxiv.org/abs/2004.08604
//! [mergeability]: https://arxiv.org/abs/2101.06758
//!
//! # Examples
//!
//! ```
//! use datasketches::uddsketch::UddSketch;
//!
//! let mut left = UddSketch::new(512, 0.01)?;
//! let mut right = UddSketch::new(512, 0.01)?;
//!
//! for i in 1..=500 {
//!     left.update(i as f64);
//!     right.update((500 + i) as f64);
//! }
//!
//! left.merge(&right)?;
//! let median = left.quantile(0.5)?.unwrap();
//! assert!((median - 500.0).abs() <= left.alpha() * 500.0);
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod mapping;
mod sketch;
mod store;

pub use self::sketch::UddSketch;

const DEFAULT_MAX_BUCKETS: u32 = 512;
const DEFAULT_ALPHA: f64 = 0.01;
const MIN_MAX_BUCKETS: u32 = 4;
const MIN_ALPHA: f64 = 1e-12;
