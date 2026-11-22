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

//! DDSketch implementation for estimating quantiles with relative-value error guarantees.
//!
//! [`DdSketch`] is a mergeable quantile sketch over finite `f64` streams based on
//! [DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees][paper]
//! by Charles Masson, Jee E. Rim, and Homin K. Lee. Unlike rank-accurate sketches such as KLL and
//! REQ, DDSketch bounds relative error in value space: for a configured `relative_accuracy`
//! `alpha`, any quantile outside the zero bucket (`|value| > zero_threshold`) whose bucket has not
//! been collapsed is approximated within `alpha * |value|`.
//!
//! Positive and negative magnitudes are mapped to integer bucket indices by a configurable
//! [`MappingKind`] (cubic significand interpolation by default, or linear interpolation / exact
//! logarithms), while values within `[ -zero_threshold, zero_threshold ]` share a dedicated zero
//! bucket. Bucket counts are maintained in a configurable [`StoreKind`], defaulting to a bounded
//! dense store that collapses lowest-magnitude buckets once the observed index span exceeds `2048`
//! bins per sign so high-magnitude tail quantiles retain their relative accuracy under bounded
//! memory.
//!
//! # Serialization
//!
//! [`DdSketch::serialize`] and [`DdSketch::deserialize`] use this crate's native `DDSKETCH` binary
//! format, preserving [`MappingKind`], [`StoreKind`], collapse state, and exact `u64` bucket
//! weights.
//!
//! [paper]: https://www.vldb.org/pvldb/vol12/p2195-masson.pdf
//!
//! # Examples
//!
//! ```
//! use datasketches::ddsketch::DdSketch;
//!
//! let mut sketch = DdSketch::new(0.01)?;
//! for latency_ms in [1.2, 2.5, 4.8, 12.0, 250.0] {
//!     sketch.update(latency_ms);
//! }
//!
//! let p50 = sketch.quantile(0.5)?.unwrap();
//! assert!((p50 - 4.8).abs() <= 0.01 * 4.8);
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod iter;
mod mapping;
mod serialization;
mod sketch;
mod store;

pub use self::iter::DdSketchIter;
pub use self::mapping::MappingKind;
pub use self::sketch::DdSketch;
pub use self::sketch::DdSketchBuilder;
pub use self::store::StoreKind;

const DEFAULT_RELATIVE_ACCURACY: f64 = 0.01;
const MIN_RELATIVE_ACCURACY: f64 = 1e-6;
const DEFAULT_MAX_NUM_BINS: usize = 2048;
const MAX_DENSE_BINS: usize = 1 << 26;
