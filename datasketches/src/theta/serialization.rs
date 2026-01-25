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

//! Binary serialization format constants and helpers for Theta sketches.

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::common::compute_seed_hash;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::hash_table::MAX_THETA;

use super::sketch::CompactThetaSketch;

pub(crate) const FAMILY_COMPACT: u8 = 3;
pub(crate) const SERIAL_VERSION_V3: u8 = 3;
pub(crate) const SERIAL_VERSION_V4: u8 = 4;

pub(crate) const FLAGS_IS_BIG_ENDIAN: u8 = 1 << 0;
pub(crate) const FLAGS_IS_READ_ONLY: u8 = 1 << 1;
pub(crate) const FLAGS_IS_EMPTY: u8 = 1 << 2;
pub(crate) const FLAGS_IS_COMPACT: u8 = 1 << 3;
pub(crate) const FLAGS_IS_ORDERED: u8 = 1 << 4;
pub(crate) const FLAGS_IS_SINGLE_ITEM: u8 = 1 << 5;
pub(crate) const FLAGS_RESERVED_MASK: u8 = 0b1100_0000;

pub(crate) fn serialize_v3(sketch: &CompactThetaSketch) -> Vec<u8> {
    let mut entries = sketch.entries.clone();
    let mut theta = sketch.theta;
    let empty = sketch.empty || (entries.is_empty() && theta == MAX_THETA);
    if empty && entries.is_empty() {
        // Java/C++ correctThetaOnCompact(empty && curCount==0)
        theta = MAX_THETA;
    }

    let single_item = !empty && entries.len() == 1 && theta == MAX_THETA;
    let ordered = sketch.ordered || single_item;
    if ordered && entries.len() > 1 {
        entries.sort_unstable();
    }

    let pre_longs = if theta < MAX_THETA {
        3
    } else if empty {
        1
    } else if entries.len() == 1 {
        1
    } else {
        2
    };

    let mut flags = 0u8;
    flags |= FLAGS_IS_READ_ONLY;
    flags |= FLAGS_IS_COMPACT;
    if empty {
        flags |= FLAGS_IS_EMPTY;
    }
    if ordered {
        flags |= FLAGS_IS_ORDERED;
    }
    if single_item {
        flags |= FLAGS_IS_SINGLE_ITEM;
    }

    let out_longs = pre_longs as usize + entries.len();
    let mut bytes = SketchBytes::with_capacity(out_longs * 8);
    bytes.write_u8(pre_longs & 0x3f); // upper 2 bits unused for compact sketches
    bytes.write_u8(SERIAL_VERSION_V3);
    bytes.write_u8(FAMILY_COMPACT);
    bytes.write_u8(0); // lgNomLongs unused for compact
    bytes.write_u8(0); // lgArrLongs unused for compact
    bytes.write_u8(flags & !FLAGS_IS_BIG_ENDIAN); // always serialize as little-endian
    bytes.write_u16_le(sketch.seed_hash);

    if pre_longs == 1 {
        if entries.len() == 1 && !empty {
            bytes.write_u64_le(entries[0]);
        }
        return bytes.into_bytes();
    }

    bytes.write_u32_le(entries.len() as u32);
    bytes.write_f32_le(0.0); // not used by compact sketches; match Java/C++
    if pre_longs == 3 {
        bytes.write_u64_le(theta);
    }
    for hash in entries.iter().copied() {
        bytes.write_u64_le(hash);
    }
    bytes.into_bytes()
}

pub(crate) fn serialize_compressed(sketch: &CompactThetaSketch) -> Vec<u8> {
    if is_suitable_for_compression(sketch) {
        serialize_v4(sketch)
    } else {
        serialize_v3(sketch)
    }
}

pub(crate) fn deserialize(bytes: &[u8]) -> Result<CompactThetaSketch, Error> {
    deserialize_with_seed(bytes, DEFAULT_UPDATE_SEED)
}

pub(crate) fn deserialize_with_seed(bytes: &[u8], expected_seed: u64) -> Result<CompactThetaSketch, Error> {
    fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
        move |_| Error::insufficient_data(tag)
    }

    let mut cursor = SketchSlice::new(bytes);
    let pre0 = cursor.read_u8().map_err(make_error("preamble_longs"))?;
    let pre_longs = pre0 & 0x3f;
    let ser_ver = cursor.read_u8().map_err(make_error("serial_version"))?;
    let family = cursor.read_u8().map_err(make_error("family_id"))?;

    if family != FAMILY_COMPACT {
        return Err(Error::invalid_family(FAMILY_COMPACT, family, "CompactThetaSketch"));
    }

    match ser_ver {
        SERIAL_VERSION_V3 => deserialize_v3(bytes, pre_longs, cursor, expected_seed),
        SERIAL_VERSION_V4 => deserialize_v4(bytes, pre_longs, cursor, expected_seed),
        _ => Err(Error::deserial(format!(
            "unsupported serial version: expected 3 or 4, got {ser_ver}",
        ))),
    }
}

fn deserialize_v3(
    bytes: &[u8],
    pre_longs: u8,
    mut cursor: SketchSlice<'_>,
    expected_seed: u64,
) -> Result<CompactThetaSketch, Error> {
    fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
        move |_| Error::insufficient_data(tag)
    }

    cursor.read_u8().map_err(make_error("lg_nom_longs"))?;
    cursor.read_u8().map_err(make_error("lg_arr_longs"))?;
    let flags = cursor.read_u8().map_err(make_error("flags"))?;

    let big_endian = (flags & FLAGS_IS_BIG_ENDIAN) != 0;
    let read_only = (flags & FLAGS_IS_READ_ONLY) != 0;
    let compact = (flags & FLAGS_IS_COMPACT) != 0;
    if !read_only || !compact {
        return Err(Error::deserial(
            "corrupted: compact sketches must have read-only and compact flags set",
        ));
    }
    if (flags & FLAGS_RESERVED_MASK) != 0 {
        return Err(Error::deserial("corrupted: reserved flag bits must be zero"));
    }

    let empty_flag = (flags & FLAGS_IS_EMPTY) != 0;
    let ordered = (flags & FLAGS_IS_ORDERED) != 0;
    let single_item = (flags & FLAGS_IS_SINGLE_ITEM) != 0;

    let seed_hash = if big_endian {
        cursor.read_u16_be().map_err(make_error("seed_hash"))?
    } else {
        cursor.read_u16_le().map_err(make_error("seed_hash"))?
    };

    if empty_flag {
        if pre_longs != 1 {
            return Err(Error::invalid_preamble_longs(1, pre_longs));
        }
        if bytes.len() != 8 {
            return Err(Error::deserial(format!(
                "invalid empty compact theta sketch size: expected 8, got {}",
                bytes.len()
            )));
        }
        return Ok(CompactThetaSketch {
            entries: vec![],
            theta: MAX_THETA,
            seed_hash,
            ordered,
            empty: true,
        });
    }

    let expected_seed_hash = compute_seed_hash(expected_seed);
    if seed_hash != expected_seed_hash {
        return Err(Error::deserial(format!(
            "incompatible seed hash: expected {expected_seed_hash}, got {seed_hash}",
        )));
    }

    if single_item {
        if pre_longs != 1 {
            return Err(Error::invalid_preamble_longs(1, pre_longs));
        }
        if bytes.len() != 16 {
            return Err(Error::deserial(format!(
                "invalid single-item compact theta sketch size: expected 16, got {}",
                bytes.len()
            )));
        }
        let hash = if big_endian {
            cursor.read_u64_be().map_err(make_error("single_hash"))?
        } else {
            cursor.read_u64_le().map_err(make_error("single_hash"))?
        };
        if hash == 0 || hash >= MAX_THETA {
            return Err(Error::deserial("corrupted: invalid retained hash value"));
        }
        return Ok(CompactThetaSketch {
            entries: vec![hash],
            theta: MAX_THETA,
            seed_hash,
            ordered: true, // single-item sketches are ordered in Java/C++
            empty: false,
        });
    }

    if pre_longs != 2 && pre_longs != 3 {
        return Err(Error::deserial(format!(
            "invalid compact theta preamble_longs: expected 2 or 3, got {pre_longs}",
        )));
    }

    let cur_count = if big_endian {
        cursor.read_u32_be().map_err(make_error("cur_count"))?
    } else {
        cursor.read_u32_le().map_err(make_error("cur_count"))?
    } as usize;
    if big_endian {
        cursor.read_f32_be().map_err(make_error("p_float"))?;
    } else {
        cursor.read_f32_le().map_err(make_error("p_float"))?;
    }

    let theta = if pre_longs == 3 {
        if big_endian {
            cursor.read_u64_be().map_err(make_error("theta_long"))?
        } else {
            cursor.read_u64_le().map_err(make_error("theta_long"))?
        }
    } else {
        MAX_THETA
    };

    let mut entries = Vec::with_capacity(cur_count);
    for _ in 0..cur_count {
        let hash = if big_endian {
            cursor.read_u64_be().map_err(make_error("entries"))?
        } else {
            cursor.read_u64_le().map_err(make_error("entries"))?
        };
        if hash == 0 || hash >= theta {
            return Err(Error::deserial("corrupted: invalid retained hash value"));
        }
        entries.push(hash);
    }

    if ordered && entries.len() > 1 {
        for i in 1..entries.len() {
            if entries[i] <= entries[i - 1] {
                return Err(Error::deserial(
                    "corrupted: ordered compact sketch entries must be strictly increasing",
                ));
            }
        }
    }

    let empty = cur_count == 0 && theta == MAX_THETA;

    Ok(CompactThetaSketch {
        entries,
        theta,
        seed_hash,
        ordered,
        empty,
    })
}

fn deserialize_v4(
    _bytes: &[u8],
    pre_longs: u8,
    mut cursor: SketchSlice<'_>,
    expected_seed: u64,
) -> Result<CompactThetaSketch, Error> {
    fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
        move |_| Error::insufficient_data(tag)
    }

    if pre_longs != 1 && pre_longs != 2 {
        return Err(Error::deserial(format!(
            "invalid compact theta preamble_longs for v4: expected 1 or 2, got {pre_longs}",
        )));
    }

    let entry_bits = cursor.read_u8().map_err(make_error("entry_bits"))?;
    let num_entries_bytes = cursor
        .read_u8()
        .map_err(make_error("num_entries_bytes"))?;
    let flags = cursor.read_u8().map_err(make_error("flags"))?;

    let big_endian = (flags & FLAGS_IS_BIG_ENDIAN) != 0;
    let read_only = (flags & FLAGS_IS_READ_ONLY) != 0;
    let compact = (flags & FLAGS_IS_COMPACT) != 0;
    let ordered = (flags & FLAGS_IS_ORDERED) != 0;
    if !read_only || !compact || !ordered {
        return Err(Error::deserial(
            "corrupted: v4 compact sketches must be read-only, compact, and ordered",
        ));
    }
    if (flags & FLAGS_RESERVED_MASK) != 0 {
        return Err(Error::deserial("corrupted: reserved flag bits must be zero"));
    }

    let empty_flag = (flags & FLAGS_IS_EMPTY) != 0;

    let seed_hash = if big_endian {
        cursor.read_u16_be().map_err(make_error("seed_hash"))?
    } else {
        cursor.read_u16_le().map_err(make_error("seed_hash"))?
    };

    let theta = if pre_longs == 2 {
        if big_endian {
            cursor.read_u64_be().map_err(make_error("theta_long"))?
        } else {
            cursor.read_u64_le().map_err(make_error("theta_long"))?
        }
    } else {
        MAX_THETA
    };

    if empty_flag {
        return Ok(CompactThetaSketch {
            entries: vec![],
            theta,
            seed_hash,
            ordered: true,
            empty: theta == MAX_THETA,
        });
    }

    let expected_seed_hash = compute_seed_hash(expected_seed);
    if seed_hash != expected_seed_hash {
        return Err(Error::deserial(format!(
            "incompatible seed hash: expected {expected_seed_hash}, got {seed_hash}",
        )));
    }

    if num_entries_bytes == 0 || num_entries_bytes > 4 {
        return Err(Error::deserial(format!(
            "corrupted: invalid num_entries_bytes: expected 1..=4, got {num_entries_bytes}",
        )));
    }

    let mut num_entries: u32 = 0;
    if big_endian {
        for _ in 0..num_entries_bytes {
            let b = cursor.read_u8().map_err(make_error("num_entries"))? as u32;
            num_entries = (num_entries << 8) | b;
        }
    } else {
        for i in 0..num_entries_bytes {
            let b = cursor.read_u8().map_err(make_error("num_entries"))? as u32;
            num_entries |= b << (i * 8);
        }
    }

    if num_entries == 0 {
        return Ok(CompactThetaSketch {
            entries: vec![],
            theta,
            seed_hash,
            ordered: true,
            empty: theta == MAX_THETA,
        });
    }

    if entry_bits == 0 || entry_bits > 64 {
        return Err(Error::deserial(format!(
            "corrupted: invalid entry_bits: expected 1..=64, got {entry_bits}",
        )));
    }

    let num_entries_usize = num_entries as usize;
    let mut deltas = vec![0u64; num_entries_usize];

    // unpack blocks of 8 deltas
    let mut i = 0usize;
    while i + 7 < num_entries_usize {
        let mut block = vec![0u8; entry_bits as usize];
        cursor.read_exact(&mut block).map_err(make_error("delta_block8"))?;
        unpack_block8(&mut deltas[i..i + 8], entry_bits, &block);
        i += 8;
    }

    // unpack remainder
    if i < num_entries_usize {
        let rem = num_entries_usize - i;
        let bytes_needed = whole_bytes_to_hold_bits(rem * entry_bits as usize);
        let mut tail = vec![0u8; bytes_needed];
        cursor.read_exact(&mut tail).map_err(make_error("delta_tail"))?;
        unpack_tail(&mut deltas[i..], entry_bits, &tail);
    }

    // undo deltas
    let mut entries = vec![0u64; num_entries_usize];
    let mut previous = 0u64;
    for (dst, delta) in entries.iter_mut().zip(deltas.into_iter()) {
        *dst = previous + delta;
        previous = *dst;
    }

    Ok(CompactThetaSketch {
        entries,
        theta,
        seed_hash,
        ordered: true,
        empty: false,
    })
}

fn serialize_v4(sketch: &CompactThetaSketch) -> Vec<u8> {
    // v4 requires ordered, non-empty, and (unless estimating) not a single item.
    let mut entries = sketch.entries.clone();
    if entries.len() > 1 {
        entries.sort_unstable();
    }

    let is_estimation_mode = sketch.theta < MAX_THETA;
    let pre_longs = if is_estimation_mode { 2 } else { 1 };
    let entry_bits = std::cmp::max(compute_entry_bits(&entries), 1);
    let num_entries_bytes = num_entries_bytes(entries.len());

    // Pre-size exactly like C++: preamble longs (8 bytes each) + num_entries_bytes + packed bits.
    let compressed_bits = entry_bits as usize * entries.len();
    let compressed_bytes = whole_bytes_to_hold_bits(compressed_bits);
    let out_bytes = (pre_longs as usize * 8) + (num_entries_bytes as usize) + compressed_bytes;
    let mut bytes = SketchBytes::with_capacity(out_bytes);

    bytes.write_u8(pre_longs);
    bytes.write_u8(SERIAL_VERSION_V4);
    bytes.write_u8(FAMILY_COMPACT);
    bytes.write_u8(entry_bits);
    bytes.write_u8(num_entries_bytes);

    let mut flags = 0u8;
    flags |= FLAGS_IS_READ_ONLY;
    flags |= FLAGS_IS_COMPACT;
    flags |= FLAGS_IS_ORDERED;
    bytes.write_u8(flags & !FLAGS_IS_BIG_ENDIAN);
    bytes.write_u16_le(sketch.seed_hash);

    if is_estimation_mode {
        bytes.write_u64_le(sketch.theta);
    }

    // num_entries stored little-endian with num_entries_bytes bytes
    let mut n = entries.len() as u32;
    for _ in 0..num_entries_bytes {
        bytes.write_u8((n & 0xff) as u8);
        n >>= 8;
    }

    // pack deltas
    let mut previous = 0u64;
    let mut i = 0usize;
    let mut block = vec![0u8; entry_bits as usize];

    while i + 7 < entries.len() {
        let mut deltas = [0u64; 8];
        for j in 0..8 {
            let entry = entries[i + j];
            deltas[j] = entry - previous;
            previous = entry;
        }
        block.fill(0);
        pack_block8(&deltas, entry_bits, &mut block);
        bytes.write(&block);
        i += 8;
    }

    if i < entries.len() {
        let rem = entries.len() - i;
        let bytes_needed = whole_bytes_to_hold_bits(rem * entry_bits as usize);
        let mut tail = vec![0u8; bytes_needed];
        pack_tail(&entries[i..], entry_bits, &mut previous, &mut tail);
        bytes.write(&tail);
    }

    bytes.into_bytes()
}

fn is_suitable_for_compression(sketch: &CompactThetaSketch) -> bool {
    if !sketch.ordered {
        return false;
    }
    let n = sketch.entries.len();
    if n == 0 {
        return false;
    }
    if n == 1 && sketch.theta == MAX_THETA {
        return false;
    }
    true
}

fn compute_entry_bits(entries: &[u64]) -> u8 {
    let mut previous = 0u64;
    let mut ored = 0u64;
    for &entry in entries {
        let delta = entry - previous;
        ored |= delta;
        previous = entry;
    }
    (64 - ored.leading_zeros()) as u8
}

fn num_entries_bytes(num_entries: usize) -> u8 {
    let n = num_entries as u32;
    let bits = 32 - n.leading_zeros();
    whole_bytes_to_hold_bits(bits as usize) as u8
}

fn whole_bytes_to_hold_bits(bits: usize) -> usize {
    (bits + 7) / 8
}

fn pack_bits(value: u64, mut bits: u8, out: &mut [u8], mut index: usize, offset: u8) -> (usize, u8) {
    if offset > 0 {
        let chunk_bits = 8 - offset;
        let mask = ((1u16 << chunk_bits) - 1) as u8;
        if bits < chunk_bits {
            out[index] |= ((value << (chunk_bits - bits)) as u8) & mask;
            return (index, offset + bits);
        }
        out[index] |= ((value >> (bits - chunk_bits)) as u8) & mask;
        index += 1;
        bits -= chunk_bits;
    }

    while bits >= 8 {
        out[index] = (value >> (bits - 8)) as u8;
        index += 1;
        bits -= 8;
    }
    if bits > 0 {
        out[index] = (value << (8 - bits)) as u8;
        return (index, bits);
    }
    (index, 0)
}

fn unpack_bits(mut bits: u8, input: &[u8], mut index: usize, mut offset: u8) -> (u64, usize, u8) {
    let avail_bits = 8 - offset;
    let chunk_bits = std::cmp::min(avail_bits, bits);
    let mask = ((1u16 << chunk_bits) - 1) as u8;

    let mut value = ((input[index] >> (avail_bits - chunk_bits)) & mask) as u64;
    if avail_bits == chunk_bits {
        index += 1;
    }
    offset = (offset + chunk_bits) & 7;
    bits -= chunk_bits;

    while bits >= 8 {
        value = (value << 8) | (input[index] as u64);
        index += 1;
        bits -= 8;
    }
    if bits > 0 {
        value <<= bits;
        value |= (input[index] >> (8 - bits)) as u64;
        return (value, index, bits);
    }
    (value, index, offset)
}

fn pack_block8(deltas: &[u64; 8], entry_bits: u8, out: &mut [u8]) {
    let mut index = 0usize;
    let mut offset = 0u8;
    for &delta in deltas {
        (index, offset) = pack_bits(delta, entry_bits, out, index, offset);
    }
    debug_assert_eq!(index, entry_bits as usize);
    debug_assert_eq!(offset, 0);
}

fn unpack_block8(out_deltas: &mut [u64], entry_bits: u8, input: &[u8]) {
    let mut index = 0usize;
    let mut offset = 0u8;
    for slot in out_deltas.iter_mut() {
        let (value, new_index, new_offset) = unpack_bits(entry_bits, input, index, offset);
        *slot = value;
        index = new_index;
        offset = new_offset;
    }
}

fn pack_tail(entries: &[u64], entry_bits: u8, previous: &mut u64, out: &mut [u8]) {
    let mut index = 0usize;
    let mut offset = 0u8;
    for &entry in entries {
        let delta = entry - *previous;
        *previous = entry;
        (index, offset) = pack_bits(delta, entry_bits, out, index, offset);
    }
    if offset > 0 {
        index += 1;
    }
    debug_assert_eq!(index, out.len());
}

fn unpack_tail(out_deltas: &mut [u64], entry_bits: u8, input: &[u8]) {
    let mut index = 0usize;
    let mut offset = 0u8;
    for slot in out_deltas.iter_mut() {
        let (value, new_index, new_offset) = unpack_bits(entry_bits, input, index, offset);
        *slot = value;
        index = new_index;
        offset = new_offset;
    }
}
