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

//! Binary serialization of Tuple sketches.
//!
//! The Tuple compact format reuses the uncompressed Theta layout (preamble, flags, theta) but uses
//! the Tuple family id, carries a sketch-type byte, and stores the user summary bytes after each
//! retained hash. See the C++/Java reference implementations for the on-disk format.
//!
//! A Tuple sketch keeps a user-defined summary next to every retained key. Because the summary
//! type is opaque to the sketch, (de)serialization of summaries is delegated to the summary type
//! itself via the [`TupleSummaryValue`] trait, mirroring `FrequentItemValue`.

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::error::Error;

/// Current serial version written by this implementation.
pub const SERIAL_VERSION: u8 = 3;
/// Legacy serial version still accepted on read.
pub const SERIAL_VERSION_LEGACY: u8 = 1;

/// Current sketch-type byte written by this implementation.
pub const SKETCH_TYPE: u8 = 1;
/// Legacy sketch-type byte still accepted on read.
pub const SKETCH_TYPE_LEGACY: u8 = 5;

/// Trait for values that can be stored as Tuple sketch summaries.
///
/// Implement this trait for a summary type to make it (de)serializable by
/// [`CompactTupleSketch`](crate::tuple::CompactTupleSketch). The encoding is entirely up to the
/// implementation; both fixed-width summaries (such as a `u64` counter) and variable-width
/// summaries (such as an array of doubles whose length is encoded in the bytes) are supported by
/// advancing the cursor past exactly the bytes read.
pub trait TupleSummaryValue: Sized {
    /// Returns the size in bytes required to serialize this summary.
    fn serialize_size(&self) -> usize;

    /// Serializes the summary into the byte buffer.
    fn serialize_value(&self, bytes: &mut SketchBytes);

    /// Deserializes a summary from the byte cursor, advancing it past the bytes consumed.
    ///
    /// # Errors
    ///
    /// Returns an error if the cursor holds too few bytes or the encoding is otherwise malformed.
    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

macro_rules! impl_primitive_summary {
    ($name:ty, $read:ident, $write:ident) => {
        impl TupleSummaryValue for $name {
            fn serialize_size(&self) -> usize {
                size_of::<$name>()
            }

            fn serialize_value(&self, bytes: &mut SketchBytes) {
                bytes.$write(*self);
            }

            fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
                cursor.$read().map_err(|_| {
                    Error::insufficient_data(
                        concat!("failed to read ", stringify!($name), " summary bytes").to_string(),
                    )
                })
            }
        }
    };
}

impl_primitive_summary!(u32, read_u32_le, write_u32_le);
impl_primitive_summary!(u64, read_u64_le, write_u64_le);
impl_primitive_summary!(i32, read_i32_le, write_i32_le);
impl_primitive_summary!(i64, read_i64_le, write_i64_le);
impl_primitive_summary!(f32, read_f32_le, write_f32_le);
impl_primitive_summary!(f64, read_f64_le, write_f64_le);
