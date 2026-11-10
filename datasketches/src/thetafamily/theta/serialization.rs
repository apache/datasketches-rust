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

//! Binary serialization for compact Theta sketches.

use crate::codec::SketchBytes;
use crate::codec::family::Family;
use crate::thetacommon::SketchEntry;
use crate::thetacommon::constants::FLAGS_IS_COMPACT;
use crate::thetacommon::constants::FLAGS_IS_ORDERED;
use crate::thetacommon::constants::FLAGS_IS_READ_ONLY;
use crate::thetacommon::sketch_state::CompactSketchState;

pub const UNCOMPRESSED_SERIAL_VERSION: u8 = 3;
pub const COMPRESSED_SERIAL_VERSION: u8 = 4;

/// Canonical empty compact image, independent of the configured seed.
pub const EMPTY_SKETCH_BYTES: [u8; 8] = [1, 3, 3, 0, 0, 0x1e, 0, 0];

pub const V2_PREAMBLE_EMPTY: u8 = 1;
pub const V2_PREAMBLE_PRECISE: u8 = 2;
pub const V2_PREAMBLE_ESTIMATE: u8 = 3;

pub(super) fn serialize_uncompressed<E: SketchEntry>(state: &CompactSketchState<E>) -> Vec<u8> {
    if state.is_empty() {
        return EMPTY_SKETCH_BYTES.to_vec();
    }

    let entries = state.retained_entries();
    let pre_longs = if state.is_estimation_mode() {
        3
    } else if entries.len() == 1 {
        1
    } else {
        2
    };
    let mut bytes = SketchBytes::with_capacity((usize::from(pre_longs) + entries.len()) * 8);
    bytes.write_u8(pre_longs);
    bytes.write_u8(UNCOMPRESSED_SERIAL_VERSION);
    bytes.write_u8(Family::THETA.id);
    bytes.write_u16_be(0); // unused for compact

    let mut flags = 0u8;
    flags |= FLAGS_IS_READ_ONLY;
    flags |= FLAGS_IS_COMPACT;
    if state.is_ordered() {
        flags |= FLAGS_IS_ORDERED;
    }
    bytes.write_u8(flags);

    bytes.write_u16_le(state.seed_hash());

    if pre_longs > 1 {
        bytes.write_u32_le(entries.len() as u32);
        // Unused by compact sketches; matches datasketches-java and datasketches-cpp.
        bytes.write_u32_be(0);
    }
    if state.is_estimation_mode() {
        bytes.write_u64_le(state.theta());
    }
    for entry in entries {
        bytes.write_u64_le(entry.hash());
    }
    bytes.into_bytes()
}
