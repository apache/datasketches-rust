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

//! REQ item types and serialization.

use std::cmp::Ordering;
use std::fmt;
use std::mem::size_of;
use std::ops::Deref;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// A non-NaN floating-point adapter for [`ReqSketch`](crate::req::ReqSketch).
///
/// REQ requires a totally ordered item domain, while primitive floats are unordered in the
/// presence of NaN. Construction therefore rejects NaN. Other values retain their numerical
/// order: signed zeros compare equal and infinities are allowed.
///
/// The inner float is available through [`into_inner`](Self::into_inner) or immutable
/// dereferencing.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub struct ReqFloat<T>(T);

impl<T> ReqFloat<T> {
    /// Returns the wrapped floating-point value.
    #[inline(always)]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for ReqFloat<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for ReqFloat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for ReqFloat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ReqFloat<f32> {
    /// Creates a non-NaN value.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is NaN.
    #[inline(always)]
    pub fn new(value: f32) -> Result<Self, Error> {
        if value.is_nan() {
            Err(Error::invalid_argument("REQ float must not be NaN"))
        } else {
            Ok(Self(value))
        }
    }
}

impl Eq for ReqFloat<f32> {}

impl Ord for ReqFloat<f32> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl ReqFloat<f64> {
    /// Creates a non-NaN value.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is NaN.
    #[inline(always)]
    pub fn new(value: f64) -> Result<Self, Error> {
        if value.is_nan() {
            Err(Error::invalid_argument("REQ float must not be NaN"))
        } else {
            Ok(Self(value))
        }
    }
}

impl Eq for ReqFloat<f64> {}

impl Ord for ReqFloat<f64> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

/// Binary serialization for REQ items.
///
/// This trait is not required for in-memory sketch operations. Implement it only when a custom
/// item type must be used with [`ReqSketch::serialize`](crate::req::ReqSketch::serialize) and
/// [`ReqSketch::deserialize`](crate::req::ReqSketch::deserialize). The encoded form must preserve
/// the item's ordering across a round trip.
pub trait ReqValue: Sized {
    /// Returns the serialized size of `item` in bytes.
    fn serialize_size(item: &Self) -> usize;

    /// Serializes this value into `bytes`.
    fn serialize_value(&self, bytes: &mut SketchBytes);

    /// Deserializes one value from `cursor`.
    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

impl ReqValue for i32 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_i32_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_i32_le()
            .map_err(insufficient_data("failed to read i32 from REQ sketch"))
    }
}

impl ReqValue for i64 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_i64_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_i64_le()
            .map_err(insufficient_data("failed to read i64 from REQ sketch"))
    }
}

impl ReqValue for u32 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_u32_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_u32_le()
            .map_err(insufficient_data("failed to read u32 from REQ sketch"))
    }
}

impl ReqValue for u64 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_u64_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_u64_le()
            .map_err(insufficient_data("failed to read u64 from REQ sketch"))
    }
}

impl ReqValue for ReqFloat<f32> {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_f32_le(self.0);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let value = cursor
            .read_f32_le()
            .map_err(insufficient_data("failed to read f32 from REQ sketch"))?;
        Self::new(value).map_err(|_| Error::deserial("REQ float must not be NaN"))
    }
}

impl ReqValue for ReqFloat<f64> {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(self.0);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let value = cursor
            .read_f64_le()
            .map_err(insufficient_data("failed to read f64 from REQ sketch"))?;
        Self::new(value).map_err(|_| Error::deserial("REQ float must not be NaN"))
    }
}
