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

//! Integer hash value wrappers.
//!
//! datasketches-cpp uses different integer extension strategies in different sketches. This module
//! exposes both strategies explicitly instead of naming all integer wrappers "canonical".

use std::hash::Hash;
use std::hash::Hasher;

use super::value::HashStrategy;
use super::value::Value;

/// An integer value wrapper that sign-extends the value before hashing.
///
/// Values narrower than 64 bits are sign-extended to 64 bits, then hashed as `u64`. Unsigned
/// narrow values are first interpreted as the signed integer of the same width.
pub type SignExtend<T> = Value<T, SignExtendStrategy>;

/// An integer value wrapper that uses Rust's natural integer widening before hashing.
///
/// Signed values are widened to `i64`; unsigned values are widened to `u64`.
pub type Natural<T> = Value<T, NaturalStrategy>;

/// Hashing strategy for [`SignExtend`].
#[doc(hidden)]
pub struct SignExtendStrategy;

/// Hashing strategy for [`Natural`].
#[doc(hidden)]
pub struct NaturalStrategy;

macro_rules! impl_from {
    ($wrapper:ident, $ctor:ident, $t:ty) => {
        impl From<$t> for $wrapper<$t> {
            fn from(value: $t) -> Self {
                $ctor(value)
            }
        }
    };
}

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

macro_rules! impl_natural {
    ($t:ty, |$v:ident| $extended:expr) => {
        impl HashStrategy<$t> for NaturalStrategy {
            fn hash<H: Hasher>(value: &$t, state: &mut H) {
                let $v = *value;
                let extended = $extended;
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
/// # use datasketches::hash_value::{calculate_hash, sign_extend_i8, sign_extend_u8};
/// assert_eq!(calculate_hash(sign_extend_i8(-1)), calculate_hash(sign_extend_u8(255)));
/// ```
pub fn sign_extend_i8(v: i8) -> SignExtend<i8> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u8` value.
///
/// `255u8` sign-extends like `-1i8`, not like `255u64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::{calculate_hash, natural_u8, sign_extend_i8, sign_extend_u8};
/// assert_eq!(calculate_hash(sign_extend_u8(255)), calculate_hash(sign_extend_i8(-1)));
/// assert_ne!(calculate_hash(sign_extend_u8(255)), calculate_hash(natural_u8(255)));
/// ```
pub fn sign_extend_u8(v: u8) -> SignExtend<u8> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from an `i16` value.
pub fn sign_extend_i16(v: i16) -> SignExtend<i16> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u16` value.
pub fn sign_extend_u16(v: u16) -> SignExtend<u16> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from an `i32` value.
pub fn sign_extend_i32(v: i32) -> SignExtend<i32> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u32` value.
pub fn sign_extend_u32(v: u32) -> SignExtend<u32> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from an `i64` value.
pub fn sign_extend_i64(v: i64) -> SignExtend<i64> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u64` value.
pub fn sign_extend_u64(v: u64) -> SignExtend<u64> {
    SignExtend::new(v)
}

/// Create a naturally extended hashable value from an `i8` value.
pub fn natural_i8(v: i8) -> Natural<i8> {
    Natural::new(v)
}

/// Create a naturally extended hashable value from a `u8` value.
///
/// `255u8` naturally extends like `255u64`, not like `-1i8`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::{calculate_hash, natural_u8, sign_extend_u8};
/// assert_eq!(calculate_hash(natural_u8(255)), calculate_hash(255u64));
/// assert_ne!(calculate_hash(natural_u8(255)), calculate_hash(sign_extend_u8(255)));
/// ```
pub fn natural_u8(v: u8) -> Natural<u8> {
    Natural::new(v)
}

/// Create a naturally extended hashable value from an `i16` value.
pub fn natural_i16(v: i16) -> Natural<i16> {
    Natural::new(v)
}

/// Create a naturally extended hashable value from a `u16` value.
pub fn natural_u16(v: u16) -> Natural<u16> {
    Natural::new(v)
}

/// Create a naturally extended hashable value from an `i32` value.
pub fn natural_i32(v: i32) -> Natural<i32> {
    Natural::new(v)
}

/// Create a naturally extended hashable value from a `u32` value.
pub fn natural_u32(v: u32) -> Natural<u32> {
    Natural::new(v)
}

/// Create a naturally extended hashable value from an `i64` value.
pub fn natural_i64(v: i64) -> Natural<i64> {
    Natural::new(v)
}

/// Create a naturally extended hashable value from a `u64` value.
pub fn natural_u64(v: u64) -> Natural<u64> {
    Natural::new(v)
}

impl_from!(SignExtend, sign_extend_i8, i8);
impl_from!(SignExtend, sign_extend_u8, u8);
impl_from!(SignExtend, sign_extend_i16, i16);
impl_from!(SignExtend, sign_extend_u16, u16);
impl_from!(SignExtend, sign_extend_i32, i32);
impl_from!(SignExtend, sign_extend_u32, u32);
impl_from!(SignExtend, sign_extend_i64, i64);
impl_from!(SignExtend, sign_extend_u64, u64);

impl_from!(Natural, natural_i8, i8);
impl_from!(Natural, natural_u8, u8);
impl_from!(Natural, natural_i16, i16);
impl_from!(Natural, natural_u16, u16);
impl_from!(Natural, natural_i32, i32);
impl_from!(Natural, natural_u32, u32);
impl_from!(Natural, natural_i64, i64);
impl_from!(Natural, natural_u64, u64);

impl_sign_extend!(i8, |v| v as i64);
impl_sign_extend!(u8, |v| (v as i8) as i64);
impl_sign_extend!(i16, |v| v as i64);
impl_sign_extend!(u16, |v| (v as i16) as i64);
impl_sign_extend!(i32, |v| v as i64);
impl_sign_extend!(u32, |v| (v as i32) as i64);
impl_sign_extend!(i64, |v| v);
impl_sign_extend!(u64, |v| v);

impl_natural!(i8, |v| v as i64);
impl_natural!(u8, |v| v as u64);
impl_natural!(i16, |v| v as i64);
impl_natural!(u16, |v| v as u64);
impl_natural!(i32, |v| v as i64);
impl_natural!(u32, |v| v as u64);
impl_natural!(i64, |v| v);
impl_natural!(u64, |v| v);
