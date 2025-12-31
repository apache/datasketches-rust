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

//! Serialization constants and helpers for frequency sketches.

/// Family ID for frequency sketches.
pub const FAMILY_ID: u8 = 10;
/// Serialization version.
pub const SER_VER: u8 = 1;

/// Preamble longs for empty sketch.
pub const PREAMBLE_LONGS_EMPTY: u8 = 1;
/// Preamble longs for non-empty sketch.
pub const PREAMBLE_LONGS_NONEMPTY: u8 = 4;

/// Empty flag mask (both bits for compatibility).
pub const EMPTY_FLAG_MASK: u8 = 5;

/// Offset of preamble longs byte.
pub const PREAMBLE_LONGS_BYTE: usize = 0;
/// Offset of serialization version byte.
pub const SER_VER_BYTE: usize = 1;
/// Offset of family ID byte.
pub const FAMILY_BYTE: usize = 2;
/// Offset of lg_max_map_size byte.
pub const LG_MAX_MAP_SIZE_BYTE: usize = 3;
/// Offset of lg_cur_map_size byte.
pub const LG_CUR_MAP_SIZE_BYTE: usize = 4;
/// Offset of flags byte.
pub const FLAGS_BYTE: usize = 5;

/// Offset of active items int (low 32 bits of second pre-long).
pub const ACTIVE_ITEMS_INT: usize = 8;
/// Offset of stream weight (third pre-long).
pub const STREAM_WEIGHT_LONG: usize = 16;
/// Offset of offset (fourth pre-long).
pub const OFFSET_LONG: usize = 24;

/// Read an u32 value from bytes at the given offset (little-endian).
#[inline]
pub fn read_u32_le(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

/// Read an i64 value from bytes at the given offset (little-endian).
#[inline]
pub fn read_i64_le(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

/// Read an u64 value from bytes at the given offset (little-endian).
#[inline]
pub fn read_u64_le(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

/// Write a u32 value to bytes at the given offset (little-endian).
#[inline]
pub fn write_u32_le(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

/// Write an u64 value to bytes at the given offset (little-endian).
#[inline]
pub fn write_u64_le(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
