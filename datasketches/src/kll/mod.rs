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

//! KLL sketch implementation for estimating quantiles and ranks.
//!
//! KLL is a compact, streaming quantiles sketch with lazy compaction and
//! near-optimal accuracy per retained item. It supports one-pass updates,
//! approximate quantiles, ranks, PMF, and CDF queries.
//!
//! This implementation follows Apache DataSketches semantics and uses the compact binary
//! serialization format shared by the Java, C++, and Go implementations.
//!
//! Items must implement [`Ord`]. Wrap `f32` or `f64` values in [`KllFloat`], which rejects NaN and
//! provides their ordinary numerical order. Custom ordering should be expressed with a newtype
//! that implements [`Ord`], keeping the ordering semantics part of the item type.
//!
//! # Usage
//!
//! ```rust
//! # use datasketches::common::SearchCriteria;
//! # use datasketches::kll::KllSketch;
//! let mut sketch = KllSketch::<i64>::new(200).unwrap();
//! sketch.update(1);
//! sketch.update(2);
//! let q = sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap();
//! assert!((1..=2).contains(&q));
//! ```

mod capacity;
mod serialization;
mod sketch;
mod sorted_view;
mod value;

pub use self::sketch::KllSketch;
pub use self::sorted_view::SortedView;
pub use self::value::KllFloat;
pub use self::value::KllValue;
/// Default value of parameter k.
const DEFAULT_K: u16 = 200;
/// Default value of parameter m.
const DEFAULT_M: u8 = 8;
/// Minimum value of parameter k.
const MIN_K: u16 = DEFAULT_M as u16;
/// Maximum value of parameter k.
const MAX_K: u16 = u16::MAX;
