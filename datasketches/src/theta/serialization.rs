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
//! binary serialization format for Theta sketches.

/// Family ID for Theta sketches in DataSketches format
pub const THETA_FAMILY_ID: u8 = 3;

/// Current serialization version
pub const SERIAL_VERSION: u8 = 3;

/// Preamble size for empty sketch (8 bytes = 1 long)
pub const PREAMBLE_LONGS_EMPTY: u8 = 1;

/// Preamble size for exact/single-item sketch (16 bytes = 2 longs)
pub const PREAMBLE_LONGS_EXACT: u8 = 2;

/// Preamble size for estimation mode sketch (24 bytes = 3 longs)
pub const PREAMBLE_LONGS_ESTIMATION: u8 = 3;

// Flags (byte 5) - bit masks
/// Flag: data is in big-endian format (we always use little-endian)
#[allow(dead_code)]
pub const FLAG_IS_BIG_ENDIAN: u8 = 1 << 0;
/// Flag: sketch is read-only (compact sketches are read-only)
pub const FLAG_IS_READ_ONLY: u8 = 1 << 1;
/// Flag: sketch is empty
pub const FLAG_IS_EMPTY: u8 = 1 << 2;
/// Flag: sketch is in compact format
pub const FLAG_IS_COMPACT: u8 = 1 << 3;
/// Flag: hash values are ordered (sorted)
pub const FLAG_IS_ORDERED: u8 = 1 << 4;
/// Flag: sketch contains a single item (special case)
#[allow(dead_code)]
pub const FLAG_HAS_SINGLE_ITEM: u8 = 1 << 5;

/// Size of a single hash entry in bytes (u64)
pub const HASH_SIZE_BYTES: usize = 8;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flag_masks() {
        // Verify flag masks are distinct powers of 2
        assert_eq!(FLAG_IS_BIG_ENDIAN, 1);
        assert_eq!(FLAG_IS_READ_ONLY, 2);
        assert_eq!(FLAG_IS_EMPTY, 4);
        assert_eq!(FLAG_IS_COMPACT, 8);
        assert_eq!(FLAG_IS_ORDERED, 16);
        assert_eq!(FLAG_HAS_SINGLE_ITEM, 32);
    }
}
