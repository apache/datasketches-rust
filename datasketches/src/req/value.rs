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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::error::Error;

/// Trait for types that can be stored in a [`ReqSketch`](crate::req::ReqSketch).
///
/// Provides total ordering (so floating-point types with NaN are well-defined under
/// sketch operations) and binary serialization compatible with the Apache DataSketches
/// REQ wire format used by the C++ and Java reference implementations.
pub trait ReqValue: Sized + Clone + PartialOrd {
    /// Total ordering used for sketch operations (sort, compaction, rank, quantile).
    ///
    /// For integer types this is equivalent to [`Ord::cmp`]. For floating-point types
    /// this delegates to [`f32::total_cmp`] / [`f64::total_cmp`] so NaN comparisons are
    /// deterministic.
    fn total_cmp(&self, other: &Self) -> Ordering;

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

macro_rules! impl_req_value_primitive {
    // Form with explicit is_nan body (for float types).
    ($t:ty, $read:ident, $write:ident, $cmp:expr, nan: $nan:expr) => {
        impl ReqValue for $t {
            #[inline(always)]
            fn total_cmp(&self, other: &Self) -> Ordering {
                $cmp(self, other)
            }

            fn serialize_size(_item: &Self) -> usize {
                std::mem::size_of::<$t>()
            }

            fn serialize_value(&self, bytes: &mut SketchBytes) {
                bytes.$write(*self);
            }

            fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
                cursor.$read().map_err(|_| {
                    Error::insufficient_data(concat!(
                        "failed to read ",
                        stringify!($t),
                        " from REQ sketch"
                    ))
                })
            }

            #[inline(always)]
            fn is_nan(&self) -> bool {
                $nan(self)
            }
        }
    };
    // Form without is_nan (for integer types — default returns false).
    ($t:ty, $read:ident, $write:ident, $cmp:expr) => {
        impl ReqValue for $t {
            #[inline(always)]
            fn total_cmp(&self, other: &Self) -> Ordering {
                $cmp(self, other)
            }

            fn serialize_size(_item: &Self) -> usize {
                std::mem::size_of::<$t>()
            }

            fn serialize_value(&self, bytes: &mut SketchBytes) {
                bytes.$write(*self);
            }

            fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
                cursor.$read().map_err(|_| {
                    Error::insufficient_data(concat!(
                        "failed to read ",
                        stringify!($t),
                        " from REQ sketch"
                    ))
                })
            }
        }
    };
}

impl_req_value_primitive!(i32, read_i32_le, write_i32_le, Ord::cmp);
impl_req_value_primitive!(i64, read_i64_le, write_i64_le, Ord::cmp);
impl_req_value_primitive!(u32, read_u32_le, write_u32_le, Ord::cmp);
impl_req_value_primitive!(u64, read_u64_le, write_u64_le, Ord::cmp);
impl_req_value_primitive!(f32, read_f32_le, write_f32_le,
    |a: &f32, b: &f32| if let Some(o) = a.partial_cmp(b) { o } else { f32::total_cmp(a, b) },
    nan: |x: &f32| f32::is_nan(*x));
impl_req_value_primitive!(f64, read_f64_le, write_f64_le,
    |a: &f64, b: &f64| if let Some(o) = a.partial_cmp(b) { o } else { f64::total_cmp(a, b) },
    nan: |x: &f64| f64::is_nan(*x));

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
    fn total_cmp_handles_nan_for_floats() {
        // Pure NaN comparisons under PartialOrd return None; total_cmp must give a definite
        // Ordering.
        let nan = f64::NAN;
        let one = 1.0_f64;
        assert_ne!(<f64 as ReqValue>::total_cmp(&nan, &one), Ordering::Equal);
        assert_eq!(<f64 as ReqValue>::total_cmp(&nan, &nan), Ordering::Equal);
    }

    #[test]
    fn total_cmp_for_integers_matches_ord() {
        assert_eq!(<i64 as ReqValue>::total_cmp(&3, &5), Ordering::Less);
        assert_eq!(<i64 as ReqValue>::total_cmp(&5, &5), Ordering::Equal);
        assert_eq!(<i64 as ReqValue>::total_cmp(&7, &5), Ordering::Greater);
    }
}
