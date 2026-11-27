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

use super::quantiles_float::QuantilesFloat;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// Item encoding for KLL and Classic Quantiles sketches.
///
/// A round trip must preserve an item's ordering relative to other items.
///
/// Built-in numeric encodings are little-endian.
/// A `String` is encoded as a little-endian `u32` byte length followed by UTF-8 bytes.
///
/// # Panics
///
/// Sizing or encoding a `String` longer than `u32::MAX` bytes panics.
///
/// # Cross-language compatibility
///
/// For DataSketches Java's `ArrayOfStringsSerDe`, limit each string to `i32::MAX` bytes.
///
/// Java's natural string order compares UTF-16 code units.
/// In Java, use lexicographic order over unsigned UTF-8 bytes to match Rust's `String`.
pub trait QuantilesValue: Clone {
    /// Minimum encoded size in bytes.
    const MIN_SERIALIZED_SIZE: usize;

    /// Returns the encoded length of `value` in bytes.
    fn serialized_size(value: &Self) -> usize;

    /// Appends the encoding of `value` to `bytes`.
    fn serialize(value: &Self, bytes: &mut SketchBytes);

    /// Reads one value and advances `input`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the value is truncated or malformed.
    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

impl QuantilesValue for QuantilesFloat<f32> {
    const MIN_SERIALIZED_SIZE: usize = size_of::<f32>();

    fn serialized_size(_value: &Self) -> usize {
        size_of::<f32>()
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_f32_le(**value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let value = input.read_f32_le().map_err(insufficient_data("f32 item"))?;
        Self::new(value).map_err(|_| Error::deserial("floating-point item must not be NaN"))
    }
}

impl QuantilesValue for QuantilesFloat<f64> {
    const MIN_SERIALIZED_SIZE: usize = size_of::<f64>();

    fn serialized_size(_value: &Self) -> usize {
        size_of::<f64>()
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(**value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let value = input.read_f64_le().map_err(insufficient_data("f64 item"))?;
        Self::new(value).map_err(|_| Error::deserial("floating-point item must not be NaN"))
    }
}

impl QuantilesValue for i64 {
    const MIN_SERIALIZED_SIZE: usize = 8;

    fn serialized_size(_value: &Self) -> usize {
        8
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_i64_le(*value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        input.read_i64_le().map_err(insufficient_data("i64 item"))
    }
}

fn string_byte_length(value: &str) -> u32 {
    u32::try_from(value.len()).expect("string byte length exceeds u32::MAX")
}

impl QuantilesValue for String {
    const MIN_SERIALIZED_SIZE: usize = 4;

    fn serialized_size(value: &Self) -> usize {
        Self::MIN_SERIALIZED_SIZE + string_byte_length(value) as usize
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_u32_le(string_byte_length(value));
        bytes.write(value.as_bytes());
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let len = input
            .read_u32_le()
            .map_err(insufficient_data("string length"))? as usize;
        let available_bytes = input.remaining().len();
        if available_bytes < len {
            return Err(Error::insufficient_data_of(
                "string payload",
                format_args!("expected {len} bytes, got {available_bytes}"),
            ));
        }
        let value = std::str::from_utf8(&input.remaining()[..len])
            .map_err(|error| Error::deserial(format!("invalid UTF-8 string: {error}")))?
            .to_owned();
        input.advance(len);
        Ok(value)
    }
}
