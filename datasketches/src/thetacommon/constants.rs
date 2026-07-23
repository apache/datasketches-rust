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

/// Maximum theta value.
///
/// The value is `i64::MAX` to be compatible with datasketches-java.
pub const MAX_THETA: u64 = i64::MAX as u64;

/// Minimum log2 of K.
pub const MIN_LG_K: u8 = 5;
/// Maximum log2 of K.
pub const MAX_LG_K: u8 = 26;
/// Default log2 of K.
pub const DEFAULT_LG_K: u8 = 12;

/// Resize threshold (0.5 = 50% load factor).
pub const HASH_TABLE_RESIZE_THRESHOLD: f64 = 0.5;
/// Rebuild threshold (15/16 = 93.75% load factor).
pub const HASH_TABLE_REBUILD_THRESHOLD: f64 = 15.0 / 16.0;

pub const STRIDE_HASH_BITS: u8 = 7;
pub const STRIDE_MASK: u64 = (1 << STRIDE_HASH_BITS) - 1;

// Flag bits of the flags byte in the Theta-family wire format, shared by Theta and Tuple sketches.
/// Flags byte bit: the sketch is read-only.
pub const FLAGS_IS_READ_ONLY: u8 = 1 << 1;
/// Flags byte bit: the sketch is logically empty.
pub const FLAGS_IS_EMPTY: u8 = 1 << 2;
/// Flags byte bit: the sketch is in compact form.
pub const FLAGS_IS_COMPACT: u8 = 1 << 3;
/// Flags byte bit: retained entries are ordered by ascending hash.
pub const FLAGS_IS_ORDERED: u8 = 1 << 4;
