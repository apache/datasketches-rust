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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// Defines the binary representation of an item stored in a
/// [`ReservoirSketch`](crate::reservoir::ReservoirSketch) or
/// [`ReservoirUnion`](crate::reservoir::ReservoirUnion).
///
/// This trait is required only for serialization and deserialization. In-memory reservoir
/// operations support any item type `T`.
///
/// # Cross-language compatibility
///
/// The built-in `i64`, `f64`, and `String` implementations match DataSketches Java
/// (`ReservoirLongsSketch`, `ArrayOfLongsSerDe`, `ArrayOfDoublesSerDe`, `ArrayOfStringsSerDe`) and
/// DataSketches Go (`ItemSketchLongSerDe`, `ItemSketchDoubleSerDe`, `ItemSketchStringSerDe`).
pub trait ReservoirValue: Sized {
    /// Minimum number of bytes required to encode one value.
    const MIN_SERIALIZED_SIZE: usize;

    /// Returns the number of bytes required to encode `value`.
    fn serialized_size(value: &Self) -> usize;

    /// Serializes `value` into `bytes`.
    fn serialize(value: &Self, bytes: &mut SketchBytes);

    /// Deserializes one value from `input`.
    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

macro_rules! impl_primitive_reservoir_value {
    ($ty:ty, $read:ident, $write:ident, $label:literal) => {
        impl ReservoirValue for $ty {
            const MIN_SERIALIZED_SIZE: usize = size_of::<$ty>();

            #[inline]
            fn serialized_size(_value: &Self) -> usize {
                size_of::<$ty>()
            }

            #[inline]
            fn serialize(value: &Self, bytes: &mut SketchBytes) {
                bytes.$write(*value);
            }

            #[inline]
            fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
                input.$read().map_err(insufficient_data($label))
            }
        }
    };
}

impl_primitive_reservoir_value!(i8, read_i8, write_i8, "reservoir i8 item");
impl_primitive_reservoir_value!(u8, read_u8, write_u8, "reservoir u8 item");
impl_primitive_reservoir_value!(i16, read_i16_le, write_i16_le, "reservoir i16 item");
impl_primitive_reservoir_value!(u16, read_u16_le, write_u16_le, "reservoir u16 item");
impl_primitive_reservoir_value!(i32, read_i32_le, write_i32_le, "reservoir i32 item");
impl_primitive_reservoir_value!(u32, read_u32_le, write_u32_le, "reservoir u32 item");
impl_primitive_reservoir_value!(i64, read_i64_le, write_i64_le, "reservoir i64 item");
impl_primitive_reservoir_value!(u64, read_u64_le, write_u64_le, "reservoir u64 item");
impl_primitive_reservoir_value!(f32, read_f32_le, write_f32_le, "reservoir f32 item");
impl_primitive_reservoir_value!(f64, read_f64_le, write_f64_le, "reservoir f64 item");

impl ReservoirValue for String {
    const MIN_SERIALIZED_SIZE: usize = size_of::<u32>();

    #[inline]
    fn serialized_size(value: &Self) -> usize {
        size_of::<u32>() + value.len()
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        let len = u32::try_from(value.len()).expect("string length exceeds u32::MAX");
        bytes.write_u32_le(len);
        bytes.write(value.as_bytes());
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let len = input
            .read_u32_le()
            .map_err(insufficient_data("reservoir string length"))? as usize;
        let available_bytes = input.remaining().len();
        if available_bytes < len {
            return Err(Error::insufficient_data_of(
                "reservoir string payload",
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
