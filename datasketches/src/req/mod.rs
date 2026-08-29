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
//! Provides approximate quantile estimation with relative error guarantees, especially
//! useful for streaming scenarios needing bounded memory. Based on the paper
//! [Relative Error Streaming Quantiles](https://arxiv.org/abs/2004.01668) by Cormode,
//! Karnin, Liberty, Thaler and Veselý, and on the Apache DataSketches C++ reference
//! implementation.

mod compactor;
mod iter;
mod serialization;
mod sketch;
mod sorted_view;
mod value;

pub use self::iter::ReqSketchIterator;
pub use self::sketch::ReqSketch;
pub use self::sorted_view::SortedView;
pub use self::value::DefaultReqItemCodec;
pub use self::value::DefaultReqOrder;
pub use self::value::ReqItemCodec;
pub use self::value::ReqOrder;

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

/// Whether queries include the weight of the search item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchCriteria {
    /// Include the weight of the search item in the result.
    #[default]
    Inclusive,
    /// Exclude the weight of the search item from the result.
    Exclusive,
}

/// Number of sections in a newly created compactor. The section count and size
/// determine its capacity and compaction range; the count doubles as its state grows.
const INITIAL_SECTIONS_PER_COMPACTOR: u8 = 3;

fn nearest_even_section_size(value: f32) -> u32 {
    ((value / 2.0).round() as u32) << 1
}
