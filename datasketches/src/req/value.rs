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

//! Trait for types storable in a [`ReqSketch`](crate::req::ReqSketch).

use std::cmp::Ordering;
use std::mem::size_of;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// Trait for types that can be stored in a [`ReqSketch`](crate::req::ReqSketch).
///
/// Provides ordering and binary serialization compatible with the Apache DataSketches
/// REQ wire format used by the C++ and Java reference implementations.
pub trait ReqValue: Sized + Clone + PartialOrd {
    /// Compares two values. See each implementation for its ordering semantics.
    fn compare(&self, other: &Self) -> Ordering;

    /// Returns true if this value is the floating-point NaN sentinel.
    ///
    /// Default: false (integer types are never NaN). Float impls override
    /// to delegate to [`f32::is_nan`] / [`f64::is_nan`].
    #[inline(always)]
    fn is_nan(&self) -> bool {
        false
    }

    /// Number of bytes this value will occupy when serialized.
    fn serialize_size(item: &Self) -> usize;

    /// Serialize this value into the byte buffer.
    fn serialize_value(&self, bytes: &mut SketchBytes);

    /// Deserialize a value from the byte cursor.
    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

impl ReqValue for i32 {
    #[inline(always)]
    fn compare(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }

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
    #[inline(always)]
    fn compare(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }

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
    #[inline(always)]
    fn compare(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }

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
    #[inline(always)]
    fn compare(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }

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

impl ReqValue for f32 {
    #[inline(always)]
    fn compare(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }

    #[inline(always)]
    fn is_nan(&self) -> bool {
        f32::is_nan(*self)
    }

    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_f32_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_f32_le()
            .map_err(insufficient_data("failed to read f32 from REQ sketch"))
    }
}

impl ReqValue for f64 {
    #[inline(always)]
    fn compare(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }

    #[inline(always)]
    fn is_nan(&self) -> bool {
        f64::is_nan(*self)
    }

    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_f64_le()
            .map_err(insufficient_data("failed to read f64 from REQ sketch"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: ReqValue + PartialEq + std::fmt::Debug>(v: T) {
        let mut bytes = SketchBytes::with_capacity(T::serialize_size(&v));
        v.serialize_value(&mut bytes);
        let raw = bytes.into_bytes();
        assert_eq!(raw.len(), T::serialize_size(&v));
        let mut cursor = SketchSlice::new(&raw);
        let got = T::deserialize_value(&mut cursor).unwrap();
        assert_eq!(got, v);
    }

    #[test]
    fn round_trip_integers() {
        round_trip(0_i32);
        round_trip(i32::MIN);
        round_trip(i32::MAX);
        round_trip(0_i64);
        round_trip(i64::MIN);
        round_trip(i64::MAX);
        round_trip(0_u32);
        round_trip(u32::MAX);
        round_trip(0_u64);
        round_trip(u64::MAX);
    }

    #[test]
    fn round_trip_floats() {
        round_trip(0.0_f32);
        round_trip(-1.5_f32);
        round_trip(f32::MIN);
        round_trip(f32::MAX);
        round_trip(f32::INFINITY);
        round_trip(f32::NEG_INFINITY);
        round_trip(0.0_f64);
        round_trip(-1.5_f64);
        round_trip(f64::MIN);
        round_trip(f64::MAX);
        round_trip(f64::INFINITY);
        round_trip(f64::NEG_INFINITY);
    }

    #[test]
    fn compare_for_f32_uses_numeric_order() {
        assert_eq!(<f32 as ReqValue>::compare(&-0.0, &0.0), Ordering::Equal);
        assert_eq!(
            <f32 as ReqValue>::compare(&f32::NEG_INFINITY, &f32::INFINITY),
            Ordering::Less
        );
    }

    #[test]
    fn compare_for_f64_uses_numeric_order() {
        assert_eq!(<f64 as ReqValue>::compare(&-0.0, &0.0), Ordering::Equal);
        assert_eq!(
            <f64 as ReqValue>::compare(&f64::NEG_INFINITY, &f64::INFINITY),
            Ordering::Less
        );
    }

    #[test]
    #[should_panic]
    fn compare_for_floats_rejects_nan() {
        <f64 as ReqValue>::compare(&f64::NAN, &0.0);
    }

    #[test]
    fn compare_for_integers_matches_ord() {
        assert_eq!(<i64 as ReqValue>::compare(&3, &5), Ordering::Less);
        assert_eq!(<i64 as ReqValue>::compare(&5, &5), Ordering::Equal);
        assert_eq!(<i64 as ReqValue>::compare(&7, &5), Ordering::Greater);
    }
}
