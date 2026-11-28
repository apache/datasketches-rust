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
use crate::thetafamily::common::constants::FLAGS_IS_COMPACT;
use crate::thetafamily::common::constants::FLAGS_IS_ORDERED;
use crate::thetafamily::common::constants::FLAGS_IS_READ_ONLY;
use crate::thetafamily::entry::SketchEntry;
use crate::thetafamily::entry::sketch_state::CompactSketchState;

pub const UNCOMPRESSED_SERIAL_VERSION: u8 = 3;
pub const COMPRESSED_SERIAL_VERSION: u8 = 4;

/// Canonical empty compact image, independent of the configured seed.
pub const EMPTY_SKETCH_BYTES: [u8; 8] = [1, 3, 3, 0, 0, 0x1e, 0, 0];

pub const V2_PREAMBLE_EMPTY: u8 = 1;
pub const V2_PREAMBLE_PRECISE: u8 = 2;
pub const V2_PREAMBLE_ESTIMATE: u8 = 3;

pub(super) fn serialize_compact_uncompressed<E: SketchEntry>(
    state: &CompactSketchState<E>,
) -> Vec<u8> {
    if state.is_empty() {
        return EMPTY_SKETCH_BYTES.to_vec();
    }

    let entries = state.retained_entries();
    let pre_longs = state.uncompressed_preamble_longs();
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

pub(super) fn serialize_compact_compressed<E: SketchEntry>(
    state: &CompactSketchState<E>,
) -> Vec<u8> {
    if is_suitable_for_compression(state) {
        serialize_v4(state)
    } else {
        serialize_compact_uncompressed(state)
    }
}

fn is_suitable_for_compression<E: SketchEntry>(state: &CompactSketchState<E>) -> bool {
    let num_retained = state.retained_entries().len();
    state.is_ordered() && num_retained != 0 && (num_retained != 1 || state.is_estimation_mode())
}

fn serialize_v4<E: SketchEntry>(state: &CompactSketchState<E>) -> Vec<u8> {
    let entries = state.retained_entries();
    let pre_longs = if state.is_estimation_mode() { 2u8 } else { 1u8 };
    let entry_bits = compute_entry_bits(entries);
    let num_entries_bytes = num_entries_bytes(entries.len());

    // Pre-size exactly: preamble longs (8 bytes each) + num_entries_bytes + packed bits.
    let compressed_bits = entry_bits as usize * entries.len();
    let compressed_bytes = compressed_bits.div_ceil(8);
    let out_bytes = (pre_longs as usize * 8) + (num_entries_bytes as usize) + compressed_bytes;
    let mut bytes = SketchBytes::with_capacity(out_bytes);

    bytes.write_u8(pre_longs);
    bytes.write_u8(COMPRESSED_SERIAL_VERSION);
    bytes.write_u8(Family::THETA.id);
    bytes.write_u8(entry_bits);
    bytes.write_u8(num_entries_bytes);

    let mut flags = 0u8;
    flags |= FLAGS_IS_READ_ONLY;
    flags |= FLAGS_IS_COMPACT;
    flags |= FLAGS_IS_ORDERED;
    bytes.write_u8(flags);

    bytes.write_u16_le(state.seed_hash());
    if state.is_estimation_mode() {
        bytes.write_u64_le(state.theta());
    }

    let mut n = entries.len() as u32;
    for _ in 0..num_entries_bytes {
        bytes.write_u8((n & 0xff) as u8);
        n >>= 8;
    }

    let bits = u32::from(entry_bits);
    let mut acc = 0u128;
    let mut acc_bits = 0u32;
    let mut previous = 0u64;
    for entry in entries {
        let hash = entry.hash();
        acc = (acc << bits) | u128::from(hash - previous);
        previous = hash;
        acc_bits += bits;
        if acc_bits >= 64 {
            acc_bits -= 64;
            bytes.write(&((acc >> acc_bits) as u64).to_be_bytes());
        }
    }
    if acc_bits > 0 {
        let tail = ((acc << (64 - acc_bits)) as u64).to_be_bytes();
        bytes.write(&tail[..acc_bits.div_ceil(8) as usize]);
    }

    bytes.into_bytes()
}

fn compute_entry_bits<E: SketchEntry>(entries: &[E]) -> u8 {
    let mut previous = 0u64;
    let mut ored = 0u64;
    for entry in entries {
        let hash = entry.hash();
        let delta = hash - previous;
        ored |= delta;
        previous = hash;
    }
    (64 - ored.leading_zeros()) as u8
}

fn num_entries_bytes(num_entries: usize) -> u8 {
    let n = num_entries as u32;
    let bits = u32::BITS - n.leading_zeros();
    bits.div_ceil(8) as u8
}
