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
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::density::MAX_LEVELS;
use crate::density::MIN_K;
use crate::density::sketch::DensitySketch;
use crate::density::sketch::Level;
use crate::density::value::DensityValue;
use crate::error::Error;

const SERIAL_VERSION: u8 = 1;
const PREAMBLE_INTS_EMPTY: u8 = 3;
const PREAMBLE_INTS_NON_EMPTY: u8 = 6;
const FLAGS_IS_EMPTY: u8 = 1 << 2;

pub fn serialize_sketch<T: DensityValue, K>(sketch: &DensitySketch<T, K>) -> Vec<u8> {
    let is_empty = sketch.is_empty();
    let preamble_ints = if is_empty {
        PREAMBLE_INTS_EMPTY
    } else {
        PREAMBLE_INTS_NON_EMPTY
    };
    let dim = sketch.dim();
    let num_retained = sketch.num_retained();
    let levels = sketch.levels();

    let header_bytes = usize::from(preamble_ints) * size_of::<u32>();
    let payload_bytes = if is_empty {
        0
    } else {
        levels.len() * size_of::<u32>() + num_retained * (dim as usize) * T::BYTE_LEN
    };
    let mut bytes = SketchBytes::with_capacity(header_bytes + payload_bytes);

    bytes.write_u8(preamble_ints);
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(Family::DENSITY.id);
    bytes.write_u8(if is_empty { FLAGS_IS_EMPTY } else { 0 });
    bytes.write_u16_le(sketch.k());
    bytes.write_u16_le(0);
    bytes.write_u32_le(dim);

    if is_empty {
        return bytes.into_bytes();
    }

    let num_retained_u32 = u32::try_from(num_retained).expect("num_retained exceeds u32::MAX");
    bytes.write_u32_le(num_retained_u32);
    bytes.write_u64_le(sketch.total_weight());

    let dim_usize = dim as usize;
    for level in levels {
        let level_len =
            u32::try_from(level.coords.len() / dim_usize).expect("level length exceeds u32::MAX");
        bytes.write_u32_le(level_len);
        for &coord in &level.coords {
            coord.write_le(&mut bytes);
        }
    }

    bytes.into_bytes()
}

pub fn deserialize_sketch<T: DensityValue, K>(
    bytes: &[u8],
    kernel: K,
) -> Result<DensitySketch<T, K>, Error> {
    let mut cursor = SketchSlice::new(bytes);

    let preamble_ints = cursor
        .read_u8()
        .map_err(insufficient_data("preamble_ints"))?;
    let serial_version = cursor
        .read_u8()
        .map_err(insufficient_data("serial_version"))?;
    let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
    let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
    let k = cursor.read_u16_le().map_err(insufficient_data("k"))?;
    let _unused = cursor.read_u16_le().map_err(insufficient_data("unused"))?;
    let dim = cursor.read_u32_le().map_err(insufficient_data("dim"))?;

    Family::DENSITY.validate_id(family_id)?;
    ensure_serial_version_is(SERIAL_VERSION, serial_version)?;

    if (flags & !FLAGS_IS_EMPTY) != 0 {
        return Err(Error::deserial(format!(
            "unsupported density sketch flags: {flags:#x}"
        )));
    }
    let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
    let expected_preamble_ints = if is_empty {
        PREAMBLE_INTS_EMPTY
    } else {
        PREAMBLE_INTS_NON_EMPTY
    };
    if preamble_ints != expected_preamble_ints {
        return Err(Error::invalid_preamble_ints(
            expected_preamble_ints,
            preamble_ints,
        ));
    }

    if k < MIN_K {
        return Err(Error::deserial(format!(
            "k must be at least {MIN_K}, got {k}"
        )));
    }
    if dim == 0 {
        return Err(Error::deserial("dimension must be at least 1, got 0"));
    }
    let dim_usize = usize::try_from(dim)
        .map_err(|_| Error::deserial(format!("dimension {dim} exceeds platform usize::MAX")))?;

    if is_empty {
        let trailing = cursor.remaining().len();
        if trailing != 0 {
            return Err(Error::deserial(format!(
                "expected end of empty density sketch image, found {trailing} trailing bytes"
            )));
        }
        return Ok(DensitySketch::from_parts(
            k,
            dim,
            kernel,
            0,
            0,
            vec![Level::default()],
        ));
    }

    let num_retained = cursor
        .read_u32_le()
        .map_err(insufficient_data("num_retained"))?;
    let n = cursor.read_u64_le().map_err(insufficient_data("n"))?;
    if n == 0 {
        return Err(Error::deserial("non-empty density sketch must have n > 0"));
    }
    if num_retained == 0 {
        return Err(Error::deserial(
            "non-empty density sketch must have num_retained > 0",
        ));
    }
    if u64::from(num_retained) > n {
        return Err(Error::deserial(format!(
            "num_retained ({num_retained}) exceeds total weight n ({n})"
        )));
    }

    let point_bytes = dim_usize
        .checked_mul(T::BYTE_LEN)
        .ok_or_else(|| Error::deserial("point byte size overflows usize"))?;
    let total_retained = usize::try_from(num_retained)
        .map_err(|_| Error::deserial("num_retained exceeds platform usize::MAX"))?;
    let min_payload_bytes = total_retained
        .checked_mul(point_bytes)
        .ok_or_else(|| Error::deserial("retained payload byte size overflows usize"))?;
    if cursor.remaining().len() < min_payload_bytes {
        return Err(Error::insufficient_data_of(
            "density sketch payload",
            format_args!(
                "expected at least {min_payload_bytes} bytes, got {}",
                cursor.remaining().len()
            ),
        ));
    }

    let mut levels = Vec::new();
    let mut remaining = total_retained;
    while remaining > 0 {
        if levels.len() >= MAX_LEVELS {
            return Err(Error::deserial(format!(
                "density sketch cannot have more than {MAX_LEVELS} levels"
            )));
        }
        let level_len_u32 = cursor
            .read_u32_le()
            .map_err(insufficient_data("level_len"))?;
        let level_len = usize::try_from(level_len_u32)
            .map_err(|_| Error::deserial("level length exceeds platform usize::MAX"))?;
        if level_len > remaining {
            return Err(Error::deserial(format!(
                "level length ({level_len}) exceeds remaining retained points ({remaining})"
            )));
        }
        let level_bytes = level_len
            .checked_mul(point_bytes)
            .ok_or_else(|| Error::deserial("level payload byte size overflows usize"))?;
        if cursor.remaining().len() < level_bytes {
            return Err(Error::insufficient_data_of(
                "density sketch level coordinates",
                format_args!(
                    "expected {level_bytes} bytes, got {}",
                    cursor.remaining().len()
                ),
            ));
        }

        let coord_count = level_len
            .checked_mul(dim_usize)
            .ok_or_else(|| Error::deserial("level coordinate count overflows usize"))?;
        let mut coords = Vec::with_capacity(coord_count);
        for _ in 0..coord_count {
            let value =
                T::read_le(&mut cursor).map_err(insufficient_data("density sketch coordinate"))?;
            if !value.is_finite_val() {
                return Err(Error::deserial("density sketch coordinate must be finite"));
            }
            coords.push(value);
        }
        levels.push(Level { coords });
        remaining -= level_len;
    }

    if levels.len() == 1 && u64::from(num_retained) != n {
        return Err(Error::deserial(format!(
            "exact-mode density sketch must have num_retained ({num_retained}) == n ({n})"
        )));
    }
    let trailing = cursor.remaining().len();
    if trailing != 0 {
        return Err(Error::deserial(format!(
            "expected end of density sketch image, found {trailing} trailing bytes"
        )));
    }

    Ok(DensitySketch::from_parts(
        k,
        dim,
        kernel,
        total_retained,
        n,
        levels,
    ))
}
