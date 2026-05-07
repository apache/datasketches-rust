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

//! Hashable value wrappers for sketches.
//!
//! Sketch update APIs accept any value that implements [`Hash`]. For most Rust values,
//! passing the value directly is sufficient. This module provides value wrappers for
//! cases where the default implementation does not match a sketch's compatibility rules.
//!
//! ## Floating Point
//!
//! [`float::Canonical`] maps `f32` and `f64` values through a canonical `f64` bit pattern before
//! hashing. Signed zero values hash the same, all NaN values use one canonical NaN bit pattern,
//! and equal `f32`/`f64` values hash the same.
//!
//! * [`canonical_f32`]
//! * [`canonical_f64`]
//!
//! ## Integers
//!
//! datasketches-cpp uses more than one integer extension strategy. [`integer::SignExtend`] first
//! sign-extends values to 64 bits and then hashes the resulting `u64`. [`integer::NaturalExtend`] widens
//! signed values to `i64` and unsigned values to `u64`.
//!
//! * [`sign_extend_i8`], [`sign_extend_u8`]
//! * [`sign_extend_i16`], [`sign_extend_u16`]
//! * [`sign_extend_i32`], [`sign_extend_u32`]
//! * [`sign_extend_i64`], [`sign_extend_u64`]
//! * [`natural_i8`], [`natural_u8`]
//! * [`natural_i16`], [`natural_u16`]
//! * [`natural_i32`], [`natural_u32`]
//! * [`natural_i64`], [`natural_u64`]
//!
//! ## Bytes
//!
//! [`bytes::Raw`] hashes byte and string inputs as raw bytes without Rust's slice or string length
//! prefix.
//!
//! Empty byte and string inputs have zero bytes to hash. datasketches-cpp skips empty strings
//! before hashing, so check `is_empty` before updating a sketch when that behavior matters.
//!
//! * [`raw_vec`]
//! * [`raw_string`]
//! * [`raw_slice`]
//! * [`raw_str`]

pub mod bytes;
pub mod float;
pub mod integer;
mod value;

use std::hash::Hash;
use std::hash::Hasher;

pub use self::bytes::Raw as RawBytes;
pub use self::bytes::raw_slice;
pub use self::bytes::raw_str;
pub use self::bytes::raw_string;
pub use self::bytes::raw_vec;
pub use self::float::Canonical as CanonicalFloat;
pub use self::float::canonical_f32;
pub use self::float::canonical_f64;
pub use self::integer::NaturalExtend;
pub use self::integer::SignExtend;
pub use self::integer::natural_extend_i8;
pub use self::integer::natural_extend_i16;
pub use self::integer::natural_extend_i32;
pub use self::integer::natural_extend_i64;
pub use self::integer::natural_extend_u8;
pub use self::integer::natural_extend_u16;
pub use self::integer::natural_extend_u32;
pub use self::integer::natural_extend_u64;
pub use self::integer::sign_extend_i8;
pub use self::integer::sign_extend_i16;
pub use self::integer::sign_extend_i32;
pub use self::integer::sign_extend_i64;
pub use self::integer::sign_extend_u8;
pub use self::integer::sign_extend_u16;
pub use self::integer::sign_extend_u32;
pub use self::integer::sign_extend_u64;
pub use self::value::Value;

#[doc(hidden)] // for doctest
pub fn calculate_hash<T: Hash>(t: T) -> u64 {
    use std::hash::DefaultHasher;

    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
