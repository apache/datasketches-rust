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

//! Naturally extended integer hash value wrappers.
//!
//! Signed values are widened to `i64`; unsigned values are widened to `u64`.

use std::hash::Hash;
use std::hash::Hasher;

use super::value::HashStrategy;
use super::value::Value;

/// An integer value wrapper that uses Rust's natural integer widening before hashing.
///
/// This strategy is compatible with how datasketches-cpp's `BloomFilter` hashes integers.
pub type NaturalExtend<T> = Value<T, NaturalExtendStrategy>;

/// Hashing strategy for [`NaturalExtend`].
#[doc(hidden)]
pub struct NaturalExtendStrategy;

macro_rules! impl_natural_extend {
    ($t:ty, |$v:ident| $extended:expr) => {
        impl HashStrategy<$t> for NaturalExtendStrategy {
            fn hash<H: Hasher>(value: &$t, state: &mut H) {
                let $v = *value;
                let extended = $extended;
                extended.hash(state);
            }
        }
    };
}

/// Create a naturally extended hashable value from an `i8` value.
pub fn from_i8(v: i8) -> NaturalExtend<i8> {
    NaturalExtend::new(v)
}

/// Create a naturally extended hashable value from a `u8` value.
///
/// `255u8` naturally extends like `255u64`, not like `-1i8`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::natural_extend::from_u8;
/// # use datasketches::hash_value::sign_extend::from_u8 as signed_from_u8;
/// assert_eq!(calculate_hash(from_u8(255)), calculate_hash(255u64));
/// assert_ne!(calculate_hash(from_u8(255)), calculate_hash(signed_from_u8(255)));
/// ```
pub fn from_u8(v: u8) -> NaturalExtend<u8> {
    NaturalExtend::new(v)
}

/// Create a naturally extended hashable value from an `i16` value.
pub fn from_i16(v: i16) -> NaturalExtend<i16> {
    NaturalExtend::new(v)
}

/// Create a naturally extended hashable value from a `u16` value.
pub fn from_u16(v: u16) -> NaturalExtend<u16> {
    NaturalExtend::new(v)
}

/// Create a naturally extended hashable value from an `i32` value.
pub fn from_i32(v: i32) -> NaturalExtend<i32> {
    NaturalExtend::new(v)
}

/// Create a naturally extended hashable value from a `u32` value.
pub fn from_u32(v: u32) -> NaturalExtend<u32> {
    NaturalExtend::new(v)
}

/// Create a naturally extended hashable value from an `i64` value.
pub fn from_i64(v: i64) -> NaturalExtend<i64> {
    NaturalExtend::new(v)
}

/// Create a naturally extended hashable value from a `u64` value.
pub fn from_u64(v: u64) -> NaturalExtend<u64> {
    NaturalExtend::new(v)
}

impl_natural_extend!(i8, |v| v as i64);
impl_natural_extend!(u8, |v| v as u64);
impl_natural_extend!(i16, |v| v as i64);
impl_natural_extend!(u16, |v| v as u64);
impl_natural_extend!(i32, |v| v as i64);
impl_natural_extend!(u32, |v| v as u64);
impl_natural_extend!(i64, |v| v);
impl_natural_extend!(u64, |v| v);
