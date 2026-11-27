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

use std::mem::size_of;

use super::MAX_ITEMS_SEEN;
use super::MAX_K;
use super::MIN_K;
use super::ReservoirSketch;
use super::ReservoirUnion;
use super::ReservoirValue;
use super::size::decode_value;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::common::ResizeFactor;
use crate::error::Error;

const SERIAL_VERSION_1: u8 = 1;
const SERIAL_VERSION_2: u8 = 2;

const PREAMBLE_LONGS_EMPTY: u8 = 1;
const PREAMBLE_LONGS_NON_EMPTY: u8 = 2;
const PREAMBLE_LONGS_UNION: u8 = 1;

const PREAMBLE_LONGS_MASK: u8 = 0x3F;
const LG_RESIZE_FACTOR_SHIFT: u8 = 6;
const FLAG_EMPTY: u8 = 0x04;

pub fn serialize_sketch<T: ReservoirValue>(sketch: &ReservoirSketch<T>) -> Vec<u8> {
    let is_empty = sketch.is_empty();
    let rf_bits = sketch.resize_factor().lg_value() << LG_RESIZE_FACTOR_SHIFT;
    let k_u32 = u32::try_from(sketch.k()).expect("k fits in i32::MAX");

    if is_empty {
        let mut bytes = SketchBytes::with_capacity(size_of::<u64>());
        bytes.write_u8(rf_bits | PREAMBLE_LONGS_EMPTY);
        bytes.write_u8(SERIAL_VERSION_2);
        bytes.write_u8(Family::RESERVOIR.id);
        bytes.write_u8(FLAG_EMPTY);
        bytes.write_u32_le(k_u32);
        return bytes.into_bytes();
    }

    let payload_bytes: usize = sketch
        .samples()
        .iter()
        .map(T::serialized_size)
        .fold(0_usize, usize::saturating_add);
    let preamble_bytes = usize::from(PREAMBLE_LONGS_NON_EMPTY) * size_of::<u64>();
    let mut bytes = SketchBytes::with_capacity(preamble_bytes.saturating_add(payload_bytes));

    bytes.write_u8(rf_bits | PREAMBLE_LONGS_NON_EMPTY);
    bytes.write_u8(SERIAL_VERSION_2);
    bytes.write_u8(Family::RESERVOIR.id);
    bytes.write_u8(0);
    bytes.write_u32_le(k_u32);
    bytes.write_u64_le(sketch.total_weight());

    for item in sketch.samples() {
        T::serialize(item, &mut bytes);
    }

    bytes.into_bytes()
}

pub fn deserialize_sketch<T: ReservoirValue>(bytes: &[u8]) -> Result<ReservoirSketch<T>, Error> {
    let mut cursor = SketchSlice::new(bytes);
    let sketch = deserialize_sketch_from_cursor(&mut cursor)?;
    let trailing = cursor.remaining().len();
    if trailing != 0 {
        return Err(Error::deserial(format!(
            "expected end of reservoir sketch image, found {trailing} trailing bytes"
        )));
    }
    Ok(sketch)
}

fn deserialize_sketch_from_cursor<T: ReservoirValue>(
    cursor: &mut SketchSlice<'_>,
) -> Result<ReservoirSketch<T>, Error> {
    let byte0 = cursor
        .read_u8()
        .map_err(insufficient_data("preamble_longs"))?;
    let serial_version = cursor
        .read_u8()
        .map_err(insufficient_data("serial_version"))?;
    let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
    let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
    let raw_k = cursor.read_u32_le().map_err(insufficient_data("k"))?;

    Family::RESERVOIR.validate_id(family_id)?;

    let k = decode_k_field(serial_version, raw_k, "k")?;

    if (flags & !FLAG_EMPTY) != 0 {
        return Err(Error::deserial(format!(
            "unsupported reservoir sketch flags: {flags:#x}"
        )));
    }
    let is_empty = (flags & FLAG_EMPTY) != 0;
    let preamble_longs = byte0 & PREAMBLE_LONGS_MASK;
    let resize_factor = ResizeFactor::from_lg_bits(byte0 >> LG_RESIZE_FACTOR_SHIFT);

    let expected_preamble_longs = if is_empty {
        PREAMBLE_LONGS_EMPTY
    } else {
        PREAMBLE_LONGS_NON_EMPTY
    };
    if preamble_longs != expected_preamble_longs {
        return Err(Error::invalid_preamble_longs(
            &[expected_preamble_longs],
            preamble_longs,
        ));
    }

    if is_empty {
        return Ok(ReservoirSketch::from_parts(k, resize_factor, 0, Vec::new()));
    }

    let total_weight = cursor
        .read_u64_le()
        .map_err(insufficient_data("total_weight"))?;
    if total_weight == 0 {
        return Err(Error::deserial(
            "non-empty reservoir sketch must have total_weight > 0",
        ));
    }
    if total_weight > MAX_ITEMS_SEEN {
        return Err(Error::deserial(format!(
            "total_weight ({total_weight}) exceeds maximum ({MAX_ITEMS_SEEN})"
        )));
    }

    let items_to_read = total_weight.min(k as u64) as usize;
    let min_payload_bytes = items_to_read
        .checked_mul(T::MIN_SERIALIZED_SIZE)
        .ok_or_else(|| Error::deserial("reservoir payload byte size overflows usize"))?;
    let available = cursor.remaining().len();
    if available < min_payload_bytes {
        return Err(Error::insufficient_data_of(
            "reservoir sketch payload",
            format_args!("expected at least {min_payload_bytes} bytes, got {available}"),
        ));
    }

    let initial_cap = if T::MIN_SERIALIZED_SIZE == 0 {
        available.min(items_to_read)
    } else {
        items_to_read
    };
    let mut data = Vec::with_capacity(initial_cap);
    for _ in 0..items_to_read {
        data.push(T::deserialize(cursor)?);
    }

    Ok(ReservoirSketch::from_parts(
        k,
        resize_factor,
        total_weight,
        data,
    ))
}

pub fn serialize_union<T: ReservoirValue>(union: &ReservoirUnion<T>) -> Vec<u8> {
    let max_k_u32 = u32::try_from(union.max_k()).expect("max_k fits in i32::MAX");
    let Some(gadget) = union.gadget() else {
        let mut bytes = SketchBytes::with_capacity(size_of::<u64>());
        bytes.write_u8(PREAMBLE_LONGS_UNION);
        bytes.write_u8(SERIAL_VERSION_2);
        bytes.write_u8(Family::RESERVOIR_UNION.id);
        bytes.write_u8(FLAG_EMPTY);
        bytes.write_u32_le(max_k_u32);
        return bytes.into_bytes();
    };

    debug_assert!(!gadget.is_empty());
    let gadget_bytes = serialize_sketch(gadget);
    let mut bytes = SketchBytes::with_capacity(size_of::<u64>() + gadget_bytes.len());
    bytes.write_u8(PREAMBLE_LONGS_UNION);
    bytes.write_u8(SERIAL_VERSION_2);
    bytes.write_u8(Family::RESERVOIR_UNION.id);
    bytes.write_u8(0);
    bytes.write_u32_le(max_k_u32);
    bytes.write(&gadget_bytes);
    bytes.into_bytes()
}

pub fn deserialize_union<T: ReservoirValue>(bytes: &[u8]) -> Result<ReservoirUnion<T>, Error> {
    let mut cursor = SketchSlice::new(bytes);

    let byte0 = cursor
        .read_u8()
        .map_err(insufficient_data("preamble_longs"))?;
    let serial_version = cursor
        .read_u8()
        .map_err(insufficient_data("serial_version"))?;
    let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
    let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
    let raw_max_k = cursor.read_u32_le().map_err(insufficient_data("max_k"))?;

    Family::RESERVOIR_UNION.validate_id(family_id)?;

    let max_k = decode_k_field(serial_version, raw_max_k, "max_k")?;

    if (flags & !FLAG_EMPTY) != 0 {
        return Err(Error::deserial(format!(
            "unsupported reservoir union flags: {flags:#x}"
        )));
    }
    let is_empty = (flags & FLAG_EMPTY) != 0;
    let preamble_longs = byte0 & PREAMBLE_LONGS_MASK;
    if preamble_longs != PREAMBLE_LONGS_UNION {
        return Err(Error::invalid_preamble_longs(
            &[PREAMBLE_LONGS_UNION],
            preamble_longs,
        ));
    }

    let mut union =
        ReservoirUnion::new(max_k).map_err(|err| Error::deserial(err.message().to_owned()))?;

    if is_empty {
        let trailing = cursor.remaining().len();
        if trailing != 0 {
            return Err(Error::deserial(format!(
                "expected end of empty reservoir union image, found {trailing} trailing bytes"
            )));
        }
        return Ok(union);
    }

    if cursor.remaining().is_empty() {
        return Err(Error::insufficient_data_of(
            "reservoir union gadget",
            "expected serialized gadget sketch, got 0 bytes",
        ));
    }

    let sketch = deserialize_sketch_from_cursor(&mut cursor)?;
    let trailing = cursor.remaining().len();
    if trailing != 0 {
        return Err(Error::deserial(format!(
            "expected end of reservoir union image, found {trailing} trailing bytes"
        )));
    }

    union.merge_owned(sketch);
    Ok(union)
}

fn decode_k_field(
    serial_version: u8,
    raw_word: u32,
    field_name: &'static str,
) -> Result<usize, Error> {
    let k = match serial_version {
        SERIAL_VERSION_2 => {
            let signed = raw_word as i32;
            if signed < 0 {
                return Err(Error::deserial(format!(
                    "{field_name} must be in [{MIN_K}, {MAX_K}], got {signed}"
                )));
            }
            usize::try_from(signed as u32)
                .map_err(|_| Error::deserial(format!("{field_name} exceeds platform usize::MAX")))?
        }
        SERIAL_VERSION_1 => {
            let encoded = (raw_word & 0xFFFF) as u16;
            decode_value(encoded)?
        }
        other => {
            return Err(Error::deserial(format!(
                "unsupported serial version: expected {SERIAL_VERSION_1} or {SERIAL_VERSION_2}, got {other}"
            )));
        }
    };

    if !(MIN_K..=MAX_K).contains(&k) {
        return Err(Error::deserial(format!(
            "{field_name} must be in [{MIN_K}, {MAX_K}], got {k}"
        )));
    }
    Ok(k)
}
