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

//! Relative Error Quantiles (REQ) sketch.
//!
//! [`ReqSketch`] provides bounded-memory rank, quantile, PMF, and CDF estimates with
//! configurable relative accuracy at either end of the rank domain. It is based on
//! [Relative Error Streaming Quantiles](https://arxiv.org/abs/2004.01668) and the Apache
//! DataSketches C++ implementation.
//!
//! # Item ordering
//!
//! The REQ paper defines input items as coming from a totally ordered universe. Accordingly,
//! sketch items must implement [`Ord`]. Rust's `f32` and `f64` do not implement `Ord` because NaN
//! is unordered; wrap floating-point items in [`ReqFloat`], whose constructor rejects NaN. Signed
//! zeros compare equal and infinities retain their usual numerical order.
//!
//! Custom item types need only [`Clone`] and [`Ord`] for in-memory use. Serialization additionally
//! requires [`ReqValue`].
//!
//! # Example
//!
//! ```
//! use datasketches::req::ReqFloat;
//! use datasketches::req::ReqSketch;
//! use datasketches::req::SearchCriteria;
//!
//! let mut sketch = ReqSketch::default();
//! for value in [1.0, 2.0, 3.0] {
//!     sketch.update(ReqFloat::<f64>::new(value)?);
//! }
//!
//! let median = sketch.quantile(0.5, SearchCriteria::Inclusive)?;
//! assert_eq!(median.into_inner(), 2.0);
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod compactor;
mod iter;
mod serialization;
mod sketch;
mod sorted_view;
mod value;

pub use self::iter::ReqSketchIterator;
pub use self::sketch::ReqSketch;
pub use self::sorted_view::SortedView;
pub use self::value::ReqFloat;
pub use self::value::ReqValue;
pub use crate::common::SearchCriteria;

/// Default value of `k` if not specified. Roughly 1% relative error at 95% confidence.
const DEFAULT_K: u16 = 12;
/// Minimum allowed value of `k`.
const MIN_K: u16 = 4;
/// Maximum allowed value of `k`.
const MAX_K: u16 = 1024;

/// Selects which tail of the rank domain the sketch optimizes for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RankAccuracy {
    /// Optimize for accuracy at high ranks (near 1.0).
    #[default]
    HighRank,
    /// Optimize for accuracy at low ranks (near 0.0).
    LowRank,
}

/// Number of sections in a newly created compactor. The section count and size
/// determine its capacity and compaction range; the count doubles as its state grows.
const INITIAL_SECTIONS_PER_COMPACTOR: u8 = 3;

fn nearest_even_section_size(value: f32) -> u32 {
    ((value / 2.0).round() as u32) << 1
}
