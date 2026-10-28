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

//! Logarithmic histograms for quantiles with relative value accuracy.
//!
//! [`DdSketch`] implements [DDSketch][paper] for distributed aggregation of numeric data,
//! such as request latencies with long tails. For relative accuracy `alpha`, it maps each positive
//! value to `i = ceil(ln(x) / ln(gamma))`, where `gamma = (1 + alpha) / (1 - alpha)`, and
//! represents that bucket by `2 * gamma^i / (gamma + 1)`. Negative values use a mirrored store,
//! and zero has its own counter. Compatible sketches merge by adding bucket counts.
//!
//! # Accuracy and memory
//!
//! Queries approximate the lower discrete quantile, sorted item `floor(q * (n - 1))`, with
//! zero-based indexing and no interpolation between ranks. Endpoints return exact extrema.
//! An uncollapsed bucket's estimate `y` of a nonzero item `x` satisfies
//! `abs(y - x) / abs(x) <= alpha` in exact arithmetic. This bounds value error, unlike the
//! rank error controlled by KLL and REQ. Floating-point logarithms and exponentials introduce
//! rounding error; `alpha` is not an outward-rounded numerical certificate. Inputs with magnitude
//! below [`DdSketch::zero_threshold`] share the zero bucket, for which only absolute error below
//! that threshold is guaranteed. NaN and infinities are rejected.
//!
//! Each sign has a dense store with at most `max_buckets` counters, including empty gaps.
//! Storage grows lazily and is independent of stream length. Increasing the value range beyond
//! this budget collapses one end of each store. The default [`CollapseDirection::Lowest`]
//! sacrifices smaller values to retain high-quantile accuracy; [`CollapseDirection::Highest`]
//! sacrifices larger values instead. Directions refer to numerical value, including for negative
//! inputs. Zero is never collapsed. [`DdSketch::quantile_is_collapsed`] identifies queries whose
//! selected bucket has lost the relative accuracy guarantee. Uniformly collapsing sketches
//! instead coarsen the entire mapping, trading accuracy at every quantile for their memory bound.
//!
//! Updates inside the retained range take constant time. Extending a store, merging, and scalar
//! quantile queries take at most linear time in the bucket limit. The same mapping and limit are
//! required for merging, so a collapsed input cannot hide lost information in a finer store.
//! Insertion order and merge trees preserve bucket counts under the same mapping; platform math
//! functions can disagree at bucket boundaries, so cross-platform bitwise reproducibility is not
//! promised. [`DdSketch::reset`] and [`DdSketch::serialize_into`] allow allocation reuse.
//!
//! Serialization uses a versioned Rust-specific format. It is not a Datadog wire format or an
//! Apache DataSketches TCK format. Datadog's [sketches-go][reference] informs the operational
//! design, but its mapping conventions and treatment of negative collapsing stores differ.
//!
//! [paper]: https://www.vldb.org/pvldb/vol12/p2195-masson.pdf
//! [reference]: https://github.com/DataDog/sketches-go/tree/228b76a8e69df016726ce47579da54cffb0d0779/ddsketch
//!
//! # Examples
//!
//! ```
//! use datasketches::ddsketch::DdSketch;
//!
//! let mut sketch = DdSketch::new(2048, 0.01)?;
//! for latency in [1.0, 2.0, 100.0] {
//!     sketch.update(latency)?;
//! }
//! let median = sketch.quantile(0.5)?.unwrap();
//! assert!((median - 2.0).abs() <= 0.01 * 2.0);
//! assert_eq!(sketch.quantile_is_collapsed(0.5)?, Some(false));
//! let restored = DdSketch::deserialize(&sketch.serialize())?;
//! sketch.merge(&restored)?;
//! assert_eq!(sketch.total_weight(), 6);
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod mapping;
mod sketch;
mod store;

pub use self::sketch::CollapseDirection;
pub use self::sketch::DdSketch;
