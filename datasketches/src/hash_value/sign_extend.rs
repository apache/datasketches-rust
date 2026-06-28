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
//! [`SignExtend`] first sign-extends values to 64-bits and then hashes the resulting integers.
//!
//! This strategy is the same as how datasketches-cpp hashes short integers for `HllSketch` and
//! `CpcSketch`.

use std::hash::Hash;
use std::hash::Hasher;

use super::value::HashStrategy;
use super::value::Value;

/// An integer value wrapper that sign-extends the value before hashing.
///
/// See the [module level documentation](super) for more.
pub type SignExtend<T> = Value<T, SignExtendStrategy>;

/// Hashing strategy for [`SignExtend`].
#[doc(hidden)]
pub struct SignExtendStrategy;

/// Create a sign-extended hashable value from an `i8` value.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::sign_extend::{from_i8, from_u8};
/// assert_eq!(calculate_hash(from_i8(-1)), calculate_hash(from_u8(255)));
/// assert_eq!(calculate_hash(from_i8(-1)), calculate_hash(-1i64));
/// assert_eq!(calculate_hash(from_i8(42)), calculate_hash(42i64));
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
/// # use datasketches::hash_value::sign_extend::{from_i8, from_u8};
/// assert_eq!(calculate_hash(from_u8(255)), calculate_hash(from_i8(-1)));
/// assert_eq!(calculate_hash(from_u8(255)), calculate_hash(-1i64));
/// assert_eq!(calculate_hash(from_u8(1)), calculate_hash(1i64));
/// ```
pub fn from_u8(v: u8) -> SignExtend<u8> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from an `i16` value.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::sign_extend::{from_i16, from_u16};
/// assert_eq!(
///     calculate_hash(from_i16(-1)),
///     calculate_hash(from_u16(65535))
/// );
/// assert_eq!(calculate_hash(from_i16(-1)), calculate_hash(-1i64));
/// assert_eq!(calculate_hash(from_i16(42)), calculate_hash(42i64));
/// ```
pub fn from_i16(v: i16) -> SignExtend<i16> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u16` value.
///
/// `65535u16` sign-extends like `-1i16`, not like `65535u64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::sign_extend::{from_i16, from_u16};
/// assert_eq!(
///     calculate_hash(from_u16(65535)),
///     calculate_hash(from_i16(-1))
/// );
/// assert_eq!(calculate_hash(from_u16(65535)), calculate_hash(-1i64));
/// assert_eq!(calculate_hash(from_u16(42)), calculate_hash(42i64));
/// ```
pub fn from_u16(v: u16) -> SignExtend<u16> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from an `i32` value.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::sign_extend::{from_i32, from_u32};
/// assert_eq!(
///     calculate_hash(from_i32(-1)),
///     calculate_hash(from_u32(4294967295))
/// );
/// assert_eq!(calculate_hash(from_i32(-1)), calculate_hash(-1i64));
/// assert_eq!(calculate_hash(from_i32(42)), calculate_hash(42i64));
/// ```
pub fn from_i32(v: i32) -> SignExtend<i32> {
    SignExtend::new(v)
}

/// Create a sign-extended hashable value from a `u32` value.
///
/// `4294967295u32` sign-extends like `-1i32`, not like `4294967295u64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::sign_extend::{from_i32, from_u32};
/// assert_eq!(
///     calculate_hash(from_u32(4294967295)),
///     calculate_hash(from_i32(-1))
/// );
/// assert_eq!(calculate_hash(from_u32(4294967295)), calculate_hash(-1i64));
/// assert_eq!(calculate_hash(from_u32(42)), calculate_hash(42i64));
/// ```
pub fn from_u32(v: u32) -> SignExtend<u32> {
    SignExtend::new(v)
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

impl_sign_extend!(i8, |v| v as i64);
impl_sign_extend!(u8, |v| (v as i8) as i64);
impl_sign_extend!(i16, |v| v as i64);
impl_sign_extend!(u16, |v| (v as i16) as i64);
impl_sign_extend!(i32, |v| v as i64);
impl_sign_extend!(u32, |v| (v as i32) as i64);
