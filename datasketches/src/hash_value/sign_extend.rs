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

//! Sign-extended integer hash value wrappers.
//!
//! Values narrower than 64 bits are sign-extended to 64 bits, then hashed as `u64`. Unsigned
//! narrow values are first interpreted as the signed integer of the same width.

use std::hash::Hash;
use std::hash::Hasher;

use super::value::HashStrategy;
use super::value::Value;

/// An integer value wrapper that sign-extends the value before hashing.
///
/// This strategy is compatible with how datasketches-cpp's `HllSketch` and `CpcSketch` hash
/// integers.
pub type SignExtend<T> = Value<T, SignExtendStrategy>;

/// Hashing strategy for [`SignExtend`].
#[doc(hidden)]
pub struct SignExtendStrategy;

macro_rules! impl_sign_extend {
    ($t:ty, |$v:ident| $extended:expr) => {
        impl HashStrategy<$t> for SignExtendStrategy {
            fn hash<H: Hasher>(value: &$t, state: &mut H) {
                let $v = *value;
                let extended = $extended as u64;
                extended.hash(state);
            }
        }
    };
}

/// Create a sign-extended hashable value from an `i8` value.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::sign_extend::{from_i8, from_u8};
/// assert_eq!(calculate_hash(from_i8(-1)), calculate_hash(from_u8(255)));
/// ```
pub fn from_i8(v: i8) -> SignExtend<i8> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u8` value.
///
/// `255u8` sign-extends like `-1i8`, not like `255u64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::natural_extend::from_u8 as natural_from_u8;
/// # use datasketches::hash_value::sign_extend::{from_i8, from_u8};
/// assert_eq!(calculate_hash(from_u8(255)), calculate_hash(from_i8(-1)));
/// assert_ne!(calculate_hash(from_u8(255)), calculate_hash(natural_from_u8(255)));
/// ```
pub fn from_u8(v: u8) -> SignExtend<u8> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from an `i16` value.
pub fn from_i16(v: i16) -> SignExtend<i16> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u16` value.
pub fn from_u16(v: u16) -> SignExtend<u16> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from an `i32` value.
pub fn from_i32(v: i32) -> SignExtend<i32> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u32` value.
pub fn from_u32(v: u32) -> SignExtend<u32> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from an `i64` value.
pub fn from_i64(v: i64) -> SignExtend<i64> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u64` value.
pub fn from_u64(v: u64) -> SignExtend<u64> {
    SignExtend::new(v)
}

impl_sign_extend!(i8, |v| v as i64);
impl_sign_extend!(u8, |v| (v as i8) as i64);
impl_sign_extend!(i16, |v| v as i64);
impl_sign_extend!(u16, |v| (v as i16) as i64);
impl_sign_extend!(i32, |v| v as i64);
impl_sign_extend!(u32, |v| (v as i32) as i64);
impl_sign_extend!(i64, |v| v);
impl_sign_extend!(u64, |v| v);
