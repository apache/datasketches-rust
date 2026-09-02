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
use crate::error::Error;

/// Defines the compact binary representation of a KLL item.
///
/// This trait is required only for serialization. In-memory KLL operations support any cloneable
/// item type with a [`KllComparator`](crate::kll::KllComparator). The encoded representation must
/// preserve the comparator's ordering across a round trip.
pub trait KllValue: Clone {
    /// Minimum number of bytes required to encode one value.
    const MIN_SERIALIZED_SIZE: usize;

    /// Returns the number of bytes required to encode `value`.
    fn serialized_size(value: &Self) -> usize;

    /// Serializes `value` into `bytes`.
    fn serialize(value: &Self, bytes: &mut SketchBytes);

    /// Deserializes one value from `input`.
    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

impl KllValue for f32 {
    const MIN_SERIALIZED_SIZE: usize = 4;

    fn serialized_size(_value: &Self) -> usize {
        4
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_f32_le(*value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        input
            .read_f32_le()
            .map_err(|_| Error::insufficient_data("f32"))
    }
}

impl KllValue for f64 {
    const MIN_SERIALIZED_SIZE: usize = 8;

    fn serialized_size(_value: &Self) -> usize {
        8
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(*value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        input
            .read_f64_le()
            .map_err(|_| Error::insufficient_data("f64"))
    }
}

impl KllValue for i64 {
    const MIN_SERIALIZED_SIZE: usize = 8;

    fn serialized_size(_value: &Self) -> usize {
        8
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_i64_le(*value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        input
            .read_i64_le()
            .map_err(|_| Error::insufficient_data("i64"))
    }
}

impl KllValue for String {
    const MIN_SERIALIZED_SIZE: usize = 4;

    fn serialized_size(value: &Self) -> usize {
        4 + value.len()
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_u32_le(value.len() as u32);
        bytes.write(value.as_bytes());
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let len = input
            .read_u32_le()
            .map_err(|_| Error::insufficient_data("string_len"))? as usize;
        let available = input.remaining().len();
        let bytes = input.remaining().get(..len).ok_or_else(|| {
            Error::deserial(format!(
                "insufficient string data: expected {len} bytes, got {available}"
            ))
        })?;
        let value = std::str::from_utf8(bytes)
            .map_err(|error| Error::deserial(format!("invalid UTF-8 string: {error}")))?
            .to_owned();
        input.advance(len as u64);
        Ok(value)
    }
}
