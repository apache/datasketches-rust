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

//! Shared hashable input fixtures spanning the orthogonal `Hasher` write patterns:
//!
//! * Single fixed-width write (`0..ITEMS as u64`): enters `write_u64` with an empty buffer.
//! * Multi-write fixed-width composite (`u64_pair`): enters `write_u64` repeatedly across non-empty
//!   buffer states and block boundaries.
//! * Single block-aligned slice (`bytes_32_values` with `raw_bytes`): enters `write` once with a
//!   compile-time block-multiple length (`rem == 0`).
//! * Single variable-length slice (`variable_byte_slices` with `raw_bytes`): enters `write` once
//!   with runtime lengths `1..=64`, spanning sub-block tails, exact blocks, and multi-block inputs
//!   across every 16-byte and 32-byte remainder.
//! * Multi-write length-prefixed slice (`variable_byte_slices` via `[u8]::hash`): calls
//!   `write_usize` followed by `write`, entering the buffered slice streaming path.

pub const ITEMS: usize = 10_000;

#[inline(always)]
pub fn u64_pair(value: u64) -> (u64, u64) {
    (value, value.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

pub fn bytes_32_values() -> Vec<[u8; 32]> {
    (0..ITEMS)
        .map(|value| {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&(value as u64).to_le_bytes());
            bytes
        })
        .collect()
}

pub fn bytes_64_values() -> Vec<[u8; 64]> {
    (0..ITEMS)
        .map(|value| {
            let mut bytes = [0; 64];
            bytes[..8].copy_from_slice(&(value as u64).to_le_bytes());
            bytes
        })
        .collect()
}

/// Cycles slice lengths through `1..=64` so callers exercise every 16-byte and 32-byte block
/// remainder in both single-block and multi-block regimes.
pub fn variable_byte_slices(values: &[[u8; 64]]) -> Vec<&[u8]> {
    values
        .iter()
        .enumerate()
        .map(|(i, bytes)| &bytes[..1 + i % bytes.len()])
        .collect()
}
