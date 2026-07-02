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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::family::Family;
use crate::error::Error;
use crate::error::ErrorKind;

const PREAMBLE_INTS_SHORT: u8 = 3;
const PREAMBLE_INTS_LONG: u8 = 6;
const SERIAL_VERSION: u8 = 1;
const FLAGS_IS_EMPTY: u8 = 1 << 2;

type Point<T> = Vec<T>;
type Level<T> = Vec<Point<T>>;
type Levels<T> = Vec<Level<T>>;
type SerializeValue<T> = fn(&mut SketchBytes, T);
type DeserializeValue<T> = fn(&mut SketchSlice<'_>) -> std::io::Result<T>;

pub(super) struct DecodedSketch<T> {
    pub(super) k: u16,
    pub(super) dim: u32,
    pub(super) num_retained: u32,
    pub(super) n: u64,
    pub(super) levels: Levels<T>,
}

pub(super) trait SketchSerializationView<T> {
    fn is_empty(&self) -> bool;
    fn k(&self) -> u16;
    fn dim(&self) -> u32;
    fn num_retained(&self) -> u32;
    fn n(&self) -> u64;
    fn levels(&self) -> &[Level<T>];
}

pub(super) fn serialize_f32<S: SketchSerializationView<f32>>(sketch: &S) -> Vec<u8> {
    serialize_inner(sketch, 4, |bytes, value| bytes.write_f32_le(value))
}

pub(super) fn serialize_f64<S: SketchSerializationView<f64>>(sketch: &S) -> Vec<u8> {
    serialize_inner(sketch, 8, |bytes, value| bytes.write_f64_le(value))
}

pub(super) fn deserialize_f32(bytes: &[u8]) -> Result<DecodedSketch<f32>, Error> {
    deserialize_inner(bytes, |cursor| cursor.read_f32_le())
}

pub(super) fn deserialize_f64(bytes: &[u8]) -> Result<DecodedSketch<f64>, Error> {
    deserialize_inner(bytes, |cursor| cursor.read_f64_le())
}

fn serialize_inner<T: Copy, S: SketchSerializationView<T>>(
    sketch: &S,
    value_size: usize,
    write_value: SerializeValue<T>,
) -> Vec<u8> {
    let preamble_ints = if sketch.is_empty() {
        PREAMBLE_INTS_SHORT
    } else {
        PREAMBLE_INTS_LONG
    };
    let mut size_bytes = preamble_ints as usize * 4;
    if !sketch.is_empty() {
        for level in sketch.levels() {
            size_bytes += 4 + (level.len() * sketch.dim() as usize * value_size);
        }
    }

    let mut bytes = SketchBytes::with_capacity(size_bytes);
    bytes.write_u8(preamble_ints);
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(Family::DENSITY.id);
    let flags = if sketch.is_empty() { FLAGS_IS_EMPTY } else { 0 };
    bytes.write_u8(flags);
    bytes.write_u16_le(sketch.k());
    bytes.write_u16_le(0);
    bytes.write_u32_le(sketch.dim());

    if sketch.is_empty() {
        return bytes.into_bytes();
    }

    bytes.write_u32_le(sketch.num_retained());
    bytes.write_u64_le(sketch.n());
    for level in sketch.levels() {
        bytes.write_u32_le(level.len() as u32);
        for point in level {
            for value in point {
                write_value(&mut bytes, *value);
            }
        }
    }
    bytes.into_bytes()
}

fn deserialize_inner<T>(
    bytes: &[u8],
    read_value: DeserializeValue<T>,
) -> Result<DecodedSketch<T>, Error> {
    fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
        move |_| Error::insufficient_data(tag)
    }

    let mut cursor = SketchSlice::new(bytes);
    let preamble_ints = cursor.read_u8().map_err(make_error("preamble_ints"))?;
    let serial_version = cursor.read_u8().map_err(make_error("serial_version"))?;
    let family_id = cursor.read_u8().map_err(make_error("family_id"))?;
    let flags = cursor.read_u8().map_err(make_error("flags"))?;
    let k = cursor.read_u16_le().map_err(make_error("k"))?;
    cursor.read_u16_le().map_err(make_error("unused"))?;
    let dim = cursor.read_u32_le().map_err(make_error("dim"))?;

    Family::DENSITY.validate_id(family_id)?;
    ensure_serial_version_is(SERIAL_VERSION, serial_version)?;
    if k < 2 {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("k must be > 1. Found: {k}"),
        ));
    }

    let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
    let expected_preamble = if is_empty {
        PREAMBLE_INTS_SHORT
    } else {
        PREAMBLE_INTS_LONG
    };
    ensure_preamble_longs_in(&[expected_preamble], preamble_ints)?;
    if is_empty {
        return Ok(DecodedSketch {
            k,
            dim,
            num_retained: 0,
            n: 0,
            levels: vec![Vec::new()],
        });
    }

    let num_retained = cursor.read_u32_le().map_err(make_error("num_retained"))?;
    let n = cursor.read_u64_le().map_err(make_error("n"))?;

    let mut levels = Vec::new();
    let mut remaining = num_retained as i64;
    while remaining > 0 {
        let level_size = cursor.read_u32_le().map_err(make_error("level_size"))?;
        let mut level = Vec::with_capacity(level_size as usize);
        for _ in 0..level_size {
            let mut point = Vec::with_capacity(dim as usize);
            for _ in 0..dim {
                point.push(read_value(&mut cursor).map_err(make_error("point"))?);
            }
            level.push(point);
        }
        remaining -= level_size as i64;
        levels.push(level);
    }
    if remaining != 0 {
        return Err(Error::deserial(
            "invalid number of retained points while decoding density sketch",
        ));
    }

    Ok(DecodedSketch {
        k,
        dim,
        num_retained,
        n,
        levels,
    })
}
