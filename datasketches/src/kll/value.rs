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

use std::cmp::Ordering;
use std::fmt;
use std::mem::size_of;
use std::ops::Deref;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// A non-NaN floating-point adapter for [`KllSketch`](crate::kll::KllSketch).
///
/// KLL requires a totally ordered item domain, while primitive floats are unordered in the
/// presence of NaN. Construction therefore rejects NaN. Other values retain their numerical
/// order: signed zeros compare equal and infinities are allowed.
///
/// The inner float is available through [`into_inner`](Self::into_inner) or immutable
/// dereferencing.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub struct KllFloat<T>(T);

impl<T> KllFloat<T> {
    /// Returns the wrapped floating-point value.
    #[inline(always)]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for KllFloat<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for KllFloat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for KllFloat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl KllFloat<f32> {
    /// Creates a non-NaN KLL value.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is NaN.
    #[inline(always)]
    pub fn new(value: f32) -> Result<Self, Error> {
        if value.is_nan() {
            Err(Error::invalid_argument("KLL float must not be NaN"))
        } else {
            Ok(Self(value))
        }
    }
}

impl Eq for KllFloat<f32> {}

impl Ord for KllFloat<f32> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl KllFloat<f64> {
    /// Creates a non-NaN KLL value.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is NaN.
    #[inline(always)]
    pub fn new(value: f64) -> Result<Self, Error> {
        if value.is_nan() {
            Err(Error::invalid_argument("KLL float must not be NaN"))
        } else {
            Ok(Self(value))
        }
    }
}

impl Eq for KllFloat<f64> {}

impl Ord for KllFloat<f64> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

/// Defines the compact binary representation of a KLL item.
///
/// This trait is required only for serialization. In-memory KLL operations support any cloneable,
/// totally ordered item type. The encoded representation must preserve that ordering across a
/// round trip.
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

impl KllValue for KllFloat<f32> {
    const MIN_SERIALIZED_SIZE: usize = size_of::<f32>();

    fn serialized_size(_value: &Self) -> usize {
        size_of::<f32>()
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_f32_le(value.0);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let value = input
            .read_f32_le()
            .map_err(insufficient_data("KLL f32 item"))?;
        Self::new(value).map_err(|_| Error::deserial("KLL float must not be NaN"))
    }
}

impl KllValue for KllFloat<f64> {
    const MIN_SERIALIZED_SIZE: usize = size_of::<f64>();

    fn serialized_size(_value: &Self) -> usize {
        size_of::<f64>()
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(value.0);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let value = input
            .read_f64_le()
            .map_err(insufficient_data("KLL f64 item"))?;
        Self::new(value).map_err(|_| Error::deserial("KLL float must not be NaN"))
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
            .map_err(insufficient_data("KLL i64 item"))
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
            .map_err(insufficient_data("KLL string length"))? as usize;
        let available_bytes = input.remaining().len();
        if available_bytes < len {
            return Err(Error::insufficient_data_of(
                "KLL string payload",
                format_args!("expected {len} bytes, got {available_bytes}"),
            ));
        }
        let value = std::str::from_utf8(&input.remaining()[..len])
            .map_err(|error| Error::deserial(format!("invalid UTF-8 string: {error}")))?
            .to_owned();
        input.advance(len as u64);
        Ok(value)
    }
}
