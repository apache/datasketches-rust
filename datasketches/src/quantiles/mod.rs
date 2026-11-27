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

//! Classic Quantiles sketch for estimating ranks and quantiles over ordered items.
//!
//! [`QuantilesSketch`] estimates ranks and quantiles in one pass and can merge independent streams.
//! Items must implement [`Clone`] and [`Ord`].
//! Wrap `f32` or `f64` values in [`QuantilesFloat`] to use them as items.
//!
//! # Cross-language compatibility
//!
//! The Java Classic doubles sketch and the C++ floating-point sketch skip NaN inputs.
//! To match them, skip NaNs before constructing a [`QuantilesFloat`].
//!
//! Rust uses the `f64` product `rank * total_weight` directly in quantile queries.
//! For up to `10_000_000` items, DataSketches Java rounds it to the nearest multiple of `1e-7`.
//! This can change which item the query selects.
//!
//! Rust and DataSketches Java give each discarded extremum one unit of weight.
//! That weight comes from its nearest retained neighbor.
//! DataSketches C++ Classic Quantiles uses the retained items with their original weights.
//! Quantiles, ranks, CDFs, and PMFs can therefore differ near either endpoint.
//!
//! For example, suppose the stream `[1, 2, 3, 4]` retains `1` and `3` with weight `2` each.
//! At rank `1.0`, this sketch returns `4` and DataSketches C++ returns `3`.
//! The exclusive rank of `4` is `0.75` here and `1.0` in DataSketches C++.
//!
//! # Examples
//!
//! ```
//! use datasketches::common::SearchCriteria;
//! use datasketches::quantiles::QuantilesSketch;
//!
//! let mut sketch = QuantilesSketch::default();
//! for latency_ms in [40, 10, 30, 20] {
//!     sketch.update(latency_ms);
//! }
//! assert_eq!(sketch.quantile(0.5, SearchCriteria::Inclusive)?, Some(20));
//! assert_eq!(sketch.quantile(0.5, SearchCriteria::Exclusive)?, Some(30));
//! assert_eq!(sketch.rank(&20, SearchCriteria::Exclusive), Some(0.25));
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod query;
mod sketch;

pub use self::sketch::QuantilesSketch;
pub use crate::common::quantiles_float::QuantilesFloat;
pub use crate::common::sorted_view::SortedView;
