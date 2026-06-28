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

//! Raw byte and string hash value wrappers.
//!
//! [`RawBytes`] hashes byte and string inputs as raw bytes without Rust's slice or string length
//! prefix.
//!
//! Empty byte and string inputs have zero bytes to hash. Other datasketches implementations skip
//! empty strings before hashing, so check `is_empty` before updating a sketch when that behavior
//! matters.

use std::hash::Hasher;

use super::value::HashStrategy;
use super::value::Value;

/// A byte or string value wrapper that hashes raw bytes.
///
/// See the [module level documentation](super) for more.
pub type RawBytes<T> = Value<T, RawBytesStrategy>;

/// Hashing strategy for [`RawBytes`].
#[doc(hidden)]
pub struct RawBytesStrategy;

/// Create a raw-byte hashable value from a byte vector.
///
/// This hashes the vector contents without Rust's slice length prefix.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::raw_bytes::{from_slice, from_vec};
/// assert_eq!(
///     calculate_hash(from_vec(b"abc".to_vec())),
///     calculate_hash(from_slice(b"abc"))
/// );
/// assert!(from_vec(vec![]).is_empty());
/// assert!(!from_vec(b"abc".to_vec()).is_empty());
/// ```
pub fn from_vec(v: Vec<u8>) -> RawBytes<Vec<u8>> {
    RawBytes::new(v)
}

/// Create a raw-byte hashable value from a string.
///
/// This hashes the UTF-8 bytes of the string without Rust's string length prefix.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::raw_bytes::{from_str, from_string};
/// assert_eq!(
///     calculate_hash(from_string("abc".to_owned())),
///     calculate_hash(from_str("abc"))
/// );
/// assert!(from_string(String::new()).is_empty());
/// assert!(!from_string("abc".to_string()).is_empty());
/// ```
pub fn from_string(v: String) -> RawBytes<String> {
    RawBytes::new(v)
}

/// Create a raw-byte hashable value from a byte slice.
///
/// This hashes the slice contents without Rust's slice length prefix.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::raw_bytes::{from_slice, from_vec};
/// assert_eq!(
///     calculate_hash(from_slice(b"abc")),
///     calculate_hash(from_vec(b"abc".to_vec()))
/// );
/// assert!(from_slice(&[]).is_empty());
/// assert!(!from_slice(b"abc").is_empty());
/// ```
pub fn from_slice(v: &[u8]) -> RawBytes<&[u8]> {
    RawBytes::new(v)
}

/// Create a raw-byte hashable value from a string slice.
///
/// This hashes the UTF-8 bytes of the string slice without Rust's string length prefix.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::raw_bytes::{from_str, from_string};
/// assert_eq!(
///     calculate_hash(from_str("abc")),
///     calculate_hash(from_string("abc".to_owned()))
/// );
/// assert!(from_str("").is_empty());
/// assert!(!from_str("abc").is_empty());
/// ```
pub fn from_str(v: &str) -> RawBytes<&str> {
    RawBytes::new(v)
}

macro_rules! impl_raw_bytes {
    ($t:ty, |$v:ident| $as_slice:expr) => {
        impl HashStrategy<$t> for RawBytesStrategy {
            fn hash<H: Hasher>(value: &$t, state: &mut H) {
                let $v = value;
                let slice = $as_slice;
                state.write(slice);
            }
        }
    };
}

impl_raw_bytes!(Vec<u8>, |v| v.as_slice());
impl_raw_bytes!(&[u8], |v| v);
impl_raw_bytes!(String, |v| v.as_bytes());
impl_raw_bytes!(&str, |v| v.as_bytes());
