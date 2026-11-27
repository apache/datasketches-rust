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

use std::collections::HashSet;

use crate::aod::ArrayOfDoublesUnion;
use crate::aod::CompactArrayOfDoublesSketch;
use crate::aod::union::CompactUnionSlots;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::thetafamily::common::constants::FLAGS_IS_EMPTY;
use crate::thetafamily::common::constants::FLAGS_IS_ORDERED;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::fold_hash::FoldHashState;

const PREAMBLE_LONGS: u8 = 1;
const SERIAL_VERSION: u8 = 1;
const SKETCH_TYPE: u8 = 3;
const FLAGS_HAS_ENTRIES: u8 = 1 << 3;
const HEADER_SIZE: usize = 16;

pub(super) fn serialize_compact(sketch: &CompactArrayOfDoublesSketch) -> Vec<u8> {
    let mut bytes = compact_header(
        sketch.is_empty(),
        sketch.is_ordered(),
        sketch.seed_hash(),
        sketch.theta64(),
        sketch.num_values(),
        sketch.num_retained(),
    );
    for &hash in &sketch.hashes {
        bytes.write_u64_le(hash);
    }
    for &value in &sketch.values {
        bytes.write_f64_le(value);
    }
    bytes.into_bytes()
}

pub(super) fn serialize_union(union: &ArrayOfDoublesUnion, ordered: bool) -> Vec<u8> {
    let table = &union.table;
    let Some(CompactUnionSlots {
        theta,
        is_ordered,
        slots,
    }) = union.compact_slots(ordered)
    else {
        return compact_header(true, true, 0, MAX_THETA, table.num_values, 0).into_bytes();
    };
    let mut bytes = compact_header(
        false,
        is_ordered,
        table.seed_hash,
        theta,
        table.num_values,
        slots.len(),
    );
    for &(hash, _) in &slots {
        bytes.write_u64_le(hash);
    }
    for (_, slot) in slots {
        for &value in table.slot_values(slot as usize) {
            bytes.write_f64_le(value);
        }
    }
    bytes.into_bytes()
}

fn compact_header(
    is_empty: bool,
    is_ordered: bool,
    seed_hash: u16,
    theta: u64,
    num_values: u8,
    num_entries: usize,
) -> SketchBytes {
    let payload_size =
        num_entries * (size_of::<u64>() + usize::from(num_values) * size_of::<f64>());
    let mut bytes = SketchBytes::with_capacity(
        HEADER_SIZE + if num_entries > 0 { 8 + payload_size } else { 0 },
    );
    bytes.write_u8(PREAMBLE_LONGS);
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(Family::TUPLE.id);
    bytes.write_u8(SKETCH_TYPE);
    let mut flags = 0;
    if is_empty {
        flags |= FLAGS_IS_EMPTY;
    }
    if num_entries > 0 {
        flags |= FLAGS_HAS_ENTRIES;
    }
    if is_ordered {
        flags |= FLAGS_IS_ORDERED;
    }
    bytes.write_u8(flags);
    bytes.write_u8(num_values);
    bytes.write_u16_le(if is_empty { 0 } else { seed_hash });
    bytes.write_u64_le(theta);
    if num_entries > 0 {
        bytes.write_u32_le(
            u32::try_from(num_entries).expect("ArrayOfDoubles retained entries exceed u32"),
        );
        bytes.write_u32_le(0);
    }
    bytes
}

pub(super) fn deserialize_compact(
    bytes: &[u8],
    seed: u64,
) -> Result<CompactArrayOfDoublesSketch, Error> {
    let expected_seed_hash = compute_seed_hash(seed, ErrorKind::InvalidData)?;
    let mut cursor = SketchSlice::new(bytes);
    let preamble_longs = cursor
        .read_u8()
        .map_err(insufficient_data("preamble_longs"))?;
    let serial_version = cursor
        .read_u8()
        .map_err(insufficient_data("serial_version"))?;
    let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
    let sketch_type = cursor.read_u8().map_err(insufficient_data("sketch_type"))?;
    let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
    let num_values = cursor.read_u8().map_err(insufficient_data("num_values"))?;
    let seed_hash = cursor
        .read_u16_le()
        .map_err(insufficient_data("seed_hash"))?;
    let theta = cursor.read_u64_le().map_err(insufficient_data("theta"))?;

    if preamble_longs != PREAMBLE_LONGS {
        return Err(Error::invalid_preamble_longs(
            &[PREAMBLE_LONGS],
            preamble_longs,
        ));
    }
    if serial_version != SERIAL_VERSION {
        return Err(Error::deserial(format!(
            "unsupported serial version: expected {SERIAL_VERSION}, got {serial_version}"
        )));
    }
    Family::TUPLE.validate_id(family_id)?;
    if sketch_type != SKETCH_TYPE {
        return Err(Error::deserial(format!(
            "unsupported sketch type: expected {SKETCH_TYPE}, got {sketch_type}"
        )));
    }
    if num_values == 0 {
        return Err(Error::deserial("corrupted: num_values must be positive"));
    }
    if !(1..=MAX_THETA).contains(&theta) {
        return Err(Error::deserial(format!(
            "corrupted: theta must be in [1, {MAX_THETA}], got {theta}"
        )));
    }

    let is_empty = flags & FLAGS_IS_EMPTY != 0;
    let has_entries = flags & FLAGS_HAS_ENTRIES != 0;
    let is_ordered = flags & FLAGS_IS_ORDERED != 0;
    if is_empty && has_entries {
        return Err(Error::deserial(
            "corrupted: empty sketch cannot have retained entries",
        ));
    }
    if is_empty {
        return Ok(CompactArrayOfDoublesSketch::empty(
            expected_seed_hash,
            num_values,
        ));
    }

    check_seed_hash(
        expected_seed_hash,
        seed_hash,
        "deserialized CompactArrayOfDoublesSketch",
        ErrorKind::InvalidData,
    )?;

    if !has_entries {
        return Ok(CompactArrayOfDoublesSketch::from_raw_parts(
            false,
            is_ordered,
            seed_hash,
            theta,
            num_values,
            Vec::new(),
            Vec::new(),
        ));
    }

    let num_entries = cursor
        .read_u32_le()
        .map_err(insufficient_data("num_entries"))? as usize;
    cursor
        .read_u32_le()
        .map_err(insufficient_data("<unused_u32>"))?;
    if num_entries == 0 {
        return Err(Error::deserial(
            "corrupted: HAS_ENTRIES is set with zero retained entries",
        ));
    }

    let stride = usize::from(num_values);
    let entry_size = size_of::<u64>() + stride * size_of::<f64>();
    let required_bytes = num_entries.checked_mul(entry_size).ok_or_else(|| {
        Error::insufficient_data_of("ArrayOfDoubles entries", "payload length overflows usize")
    })?;
    let available_bytes = cursor.remaining().len();
    if available_bytes < required_bytes {
        return Err(Error::insufficient_data_of(
            "ArrayOfDoubles entries",
            format_args!("expected {required_bytes} bytes, got {available_bytes}"),
        ));
    }

    let mut hashes = Vec::with_capacity(num_entries);
    let mut seen = (!is_ordered && num_entries > 1).then(|| {
        HashSet::<u64, FoldHashState>::with_capacity_and_hasher(num_entries, FoldHashState::new())
    });
    for _ in 0..num_entries {
        let hash = cursor
            .read_u64_le()
            .map_err(insufficient_data("entry_hash"))?;
        if hash == 0 || hash >= theta {
            return Err(Error::deserial("corrupted: invalid retained hash value"));
        }
        if is_ordered && hashes.last().is_some_and(|&previous| previous >= hash) {
            return Err(Error::deserial(
                "corrupted: ordered hashes are not strictly increasing",
            ));
        }
        if seen.as_mut().is_some_and(|seen| !seen.insert(hash)) {
            return Err(Error::deserial("corrupted: duplicate retained hash"));
        }
        hashes.push(hash);
    }

    let num_items = num_entries * stride;
    let mut values = Vec::with_capacity(num_items);
    for _ in 0..num_items {
        values.push(
            cursor
                .read_f64_le()
                .map_err(insufficient_data("entry_values"))?,
        );
    }
    Ok(CompactArrayOfDoublesSketch::from_raw_parts(
        false, is_ordered, seed_hash, theta, num_values, hashes, values,
    ))
}
