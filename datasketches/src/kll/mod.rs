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
//! This implementation follows Apache DataSketches semantics (Java KllSketch
//! / KllPreambleUtil, C++ kll_sketch) and uses the same binary serialization
//! format as those implementations.
//!
//! # Usage
//!
//! ```rust
//! # use datasketches::kll::KllSketch;
//! let mut sketch = KllSketch::<f64>::new(200).unwrap();
//! sketch.update(1.0);
//! sketch.update(2.0);
//! let q = sketch.quantile(0.5, true).unwrap();
//! assert!(q >= 1.0 && q <= 2.0);
//! ```

mod capacity;
mod serialization;
mod sketch;
mod sorted_view;

pub use self::sketch::KllComparator;
pub use self::sketch::KllItem;
pub use self::sketch::KllSketch;
pub use self::sketch::NaturalOrder;

/// KLL sketch specialized for `f64`.
pub type KllSketchF64<C = NaturalOrder> = KllSketch<f64, C>;
/// KLL sketch specialized for `f32`.
pub type KllSketchF32<C = NaturalOrder> = KllSketch<f32, C>;
/// KLL sketch specialized for `i64`.
pub type KllSketchI64<C = NaturalOrder> = KllSketch<i64, C>;
/// KLL sketch specialized for `String`.
pub type KllSketchString<C = NaturalOrder> = KllSketch<String, C>;

/// Default value of parameter k.
pub const DEFAULT_K: u16 = 200;
/// Default value of parameter m.
pub const DEFAULT_M: u8 = 8;
/// Minimum value of parameter k.
pub const MIN_K: u16 = DEFAULT_M as u16;
/// Maximum value of parameter k.
pub const MAX_K: u16 = u16::MAX;
