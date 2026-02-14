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

//! Binary serialization format constants for Theta sketches
//!
//! This module contains all constants related to the Apache DataSketches
//! binary serialization format for Theta sketches. The format is compatible
//! with the Java and C++ implementations.
//!
//! # Compact Sketch Binary Format
//!
//! The compact theta sketch format stores sorted hash values with a minimal preamble.
//!
//! ## Preamble Layout (Little Endian)
//!
//! | Byte | Field | Description |
//! |------|-------|-------------|
//! | 0 | preamble_longs | Number of 8-byte longs in preamble (1, 2, or 3) |
//! | 1 | serial_version | Serialization version (currently 3) |
//! | 2 | family_id | Family ID (3 for Theta) |
//! | 3 | lg_k | Log2 of nominal entries |
//! | 4 | lg_resize | Unused in compact format (0) |
//! | 5 | flags | Bit flags (see below) |
//! | 6-7 | seed_hash | 16-bit hash of the seed |
//!
//! If preamble_longs >= 2:
//! | Byte 8-11 | retained_entries | Number of hash values stored |
//! | Byte 12-15 | p | Sampling probability as float (unused in compact, set to 1.0) |
//!
//! If preamble_longs >= 3:
//! | Byte 16-23 | theta | Theta value as 64-bit integer |
//!
//! ## Flags (Byte 5)
//!
//! | Bit | Name | Description |
//! |-----|------|-------------|
//! | 0 | BIG_ENDIAN | Not used (always 0 for little endian) |
//! | 1 | READ_ONLY | Sketch is read-only (always 1 for compact) |
//! | 2 | EMPTY | Sketch is empty |
//! | 3 | COMPACT | Sketch is in compact form (always 1) |
//! | 4 | ORDERED | Hash values are sorted (always 1 for compact) |

pub const THETA_FAMILY_ID: u8 = 3;
pub const SERIAL_VERSION: u8 = 3;

pub const FLAG_READ_ONLY: u8 = 1 << 1;
pub const FLAG_EMPTY: u8 = 1 << 2;
pub const FLAG_COMPACT: u8 = 1 << 3;
pub const FLAG_ORDERED: u8 = 1 << 4;
pub const FLAG_SINGLE_ITEM: u8 = 1 << 5;

pub const PREAMBLE_LONGS_EMPTY: u8 = 1;
pub const PREAMBLE_LONGS_EXACT: u8 = 2;
pub const PREAMBLE_LONGS_ESTIMATION: u8 = 3;

pub const HASH_SIZE_BYTES: usize = 8;
pub const DEFAULT_P_FLOAT_BITS: u32 = 0x3F80_0000;
