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

//! Binary serialization format constants for KLL sketches.
//!
//! Naming and layout follow the Apache DataSketches Java implementation
//! (`KllPreambleUtil`) and the C++ `kll_sketch` serialization format.

/// Family ID for KLL sketches in DataSketches format (KllPreambleUtil.KLL_FAMILY).
pub const KLL_FAMILY_ID: u8 = 15;

/// Serialization version for empty or full sketches (KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL).
pub const SERIAL_VERSION_1: u8 = 1;
/// Serialization version for single-item sketches (KllPreambleUtil.SERIAL_VERSION_SINGLE).
pub const SERIAL_VERSION_2: u8 = 2;

/// Preamble ints for empty and single-item sketches (KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE).
pub const PREAMBLE_INTS_SHORT: u8 = 2;
/// Preamble ints for sketches with more than one item (KllPreambleUtil.PREAMBLE_INTS_FULL).
pub const PREAMBLE_INTS_FULL: u8 = 5;

/// Flag indicating the sketch is empty (KllPreambleUtil.EMPTY_BIT_MASK).
pub const FLAG_EMPTY: u8 = 1 << 0;
/// Flag indicating level zero is sorted (KllPreambleUtil.LEVEL_ZERO_SORTED_BIT_MASK).
pub const FLAG_LEVEL_ZERO_SORTED: u8 = 1 << 1;
/// Flag indicating the sketch has a single item (KllPreambleUtil.SINGLE_ITEM_BIT_MASK).
pub const FLAG_SINGLE_ITEM: u8 = 1 << 2;

/// Serialized size for an empty sketch in bytes (KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM).
pub const EMPTY_SIZE_BYTES: usize = 8;
/// Data offset for single-item sketches (KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM).
pub const DATA_START_SINGLE_ITEM: usize = 8;
/// Data offset for sketches with more than one item (KllPreambleUtil.DATA_START_ADR).
pub const DATA_START: usize = 20;
