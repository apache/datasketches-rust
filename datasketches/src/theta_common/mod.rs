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

pub(crate) mod hash_table;
pub(crate) mod sketch_view;
pub(crate) mod union;

// Public because public view APIs expose this trait and its entry bound, e.g. `ThetaSketchView: RawThetaSketchView<ThetaEntry>`.
pub use self::hash_table::RawHashTableEntry;
pub use self::sketch_view::RawThetaSketchView;

/// Maximum theta value (signed max for compatibility with Java).
pub(crate) const MAX_THETA: u64 = i64::MAX as u64;
/// Minimum log2 of K.
pub(crate) const MIN_LG_K: u8 = 5;
/// Maximum log2 of K.
pub(crate) const MAX_LG_K: u8 = 26;
/// Default log2 of K.
pub(crate) const DEFAULT_LG_K: u8 = 12;
/// Resize threshold (0.5 = 50% load factor).
pub(crate) const HASH_TABLE_RESIZE_THRESHOLD: f64 = 0.5;
/// Rebuild threshold (15/16 = 93.75% load factor).
pub(crate) const HASH_TABLE_REBUILD_THRESHOLD: f64 = 15.0 / 16.0;
/// Stride hash bits (7 bits for stride calculation).
pub(crate) const STRIDE_HASH_BITS: u8 = 7;
/// Stride mask.
pub(crate) const STRIDE_MASK: u64 = (1 << STRIDE_HASH_BITS) - 1;
