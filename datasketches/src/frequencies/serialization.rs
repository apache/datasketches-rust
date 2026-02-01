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
use std::hash::Hash;

/// Family ID for frequency sketches.
pub const FAMILY_ID: u8 = 10;
/// Serialization version.
pub const SERIAL_VERSION: u8 = 1;

/// Preamble longs for empty sketch.
pub const PREAMBLE_LONGS_EMPTY: u8 = 1;
/// Preamble longs for non-empty sketch.
pub const PREAMBLE_LONGS_NONEMPTY: u8 = 4;

/// Empty flag mask (both bits for compatibility).
pub const EMPTY_FLAG_MASK: u8 = 5;

/// Trait for serializing and deserializing frequent item values.
pub trait FrequentItemValue: Sized + Eq + Hash + Clone {
    /// Returns the size in bytes required to serialize the given item.
    fn serialize_size(item: &Self) -> usize;
    /// Serializes the item into the given byte buffer.
    fn serialize_value(&self, bytes: &mut SketchBytes);
    /// Deserializes an item from the given byte cursor.
    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

impl FrequentItemValue for String {
    fn serialize_size(item: &Self) -> usize {
        size_of::<u32>() + item.len()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        let bs = self.as_bytes();
        bytes.write_u32_le(bs.len() as u32);
        bytes.write(bs);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let len = cursor.read_u32_le().map_err(|_| {
            Error::insufficient_data("failed to read string item length".to_string())
        })?;

        let mut slice = vec![0; len as usize];
        cursor.read_exact(&mut slice).map_err(|_| {
            Error::insufficient_data("failed to read string item bytes".to_string())
        })?;

        String::from_utf8(slice)
            .map_err(|_| Error::deserial("invalid UTF-8 string payload".to_string()))
    }
}

macro_rules! impl_primitive {
    ($name:ty, $read:ident, $write:ident) => {
        impl FrequentItemValue for $name {
            fn serialize_size(_item: &Self) -> usize {
                size_of::<$name>()
            }

            fn serialize_value(&self, bytes: &mut SketchBytes) {
                bytes.$write(*self);
            }

            fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
                cursor.$read().map_err(|_| {
                    Error::insufficient_data(
                        concat!("failed to read ", stringify!($name), " item bytes").to_string(),
                    )
                })
            }
        }
    };
}

impl_primitive!(i8, read_i8, write_i8);
impl_primitive!(u8, read_u8, write_u8);
impl_primitive!(i16, read_i16_le, write_i16_le);
impl_primitive!(u16, read_u16_le, write_u16_le);
impl_primitive!(i32, read_i32_le, write_i32_le);
impl_primitive!(u32, read_u32_le, write_u32_le);
impl_primitive!(i64, read_i64_le, write_i64_le);
impl_primitive!(u64, read_u64_le, write_u64_le);
