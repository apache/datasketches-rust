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

//! Byte and string hash value wrappers.
//!
//! [`Raw`] hashes byte and string values as raw bytes without Rust's slice or string length prefix.

use std::hash::Hasher;

use super::value::HashStrategy;
use super::value::Value;

/// A byte or string value wrapper that hashes raw bytes.
pub type Raw<T> = Value<T, RawStrategy>;

/// Hashing strategy for [`Raw`].
#[doc(hidden)]
pub struct RawStrategy;

/// Create a raw-byte hashable value from a byte vector.
///
/// This hashes the vector contents without Rust's slice length prefix.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::{calculate_hash, raw_slice, raw_vec};
/// assert_eq!(
///     calculate_hash(raw_vec(b"abc".to_vec())),
///     calculate_hash(raw_slice(b"abc"))
/// );
/// assert!(raw_vec(Vec::new()).is_empty());
/// ```
pub fn raw_vec(v: Vec<u8>) -> Raw<Vec<u8>> {
    Raw::new(v)
}

/// Create a raw-byte hashable value from a string.
///
/// This hashes the UTF-8 bytes of the string without Rust's string length prefix.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::{calculate_hash, raw_str, raw_string};
/// assert_eq!(
///     calculate_hash(raw_string("abc".to_owned())),
///     calculate_hash(raw_str("abc"))
/// );
/// assert!(raw_string(String::new()).is_empty());
/// ```
pub fn raw_string(v: String) -> Raw<String> {
    Raw::new(v)
}

/// Create a raw-byte hashable value from a byte slice.
///
/// This hashes the slice contents without Rust's slice length prefix.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::{calculate_hash, raw_slice, raw_vec};
/// assert_eq!(
///     calculate_hash(raw_slice(b"abc")),
///     calculate_hash(raw_vec(b"abc".to_vec()))
/// );
/// assert_ne!(calculate_hash(raw_slice(b"ab")), calculate_hash(raw_slice(b"abc")));
/// assert!(raw_slice(&[]).is_empty());
/// ```
pub fn raw_slice(v: &[u8]) -> Raw<&[u8]> {
    Raw::new(v)
}

/// Create a raw-byte hashable value from a string slice.
///
/// This hashes the UTF-8 bytes of the string slice without Rust's string length prefix.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::{calculate_hash, raw_str, raw_string};
/// assert_eq!(
///     calculate_hash(raw_str("abc")),
///     calculate_hash(raw_string("abc".to_owned()))
/// );
/// assert_ne!(calculate_hash(raw_str("ab")), calculate_hash(raw_str("abc")));
/// assert!(raw_str("").is_empty());
/// ```
pub fn raw_str(v: &str) -> Raw<&str> {
    Raw::new(v)
}

impl From<Vec<u8>> for Raw<Vec<u8>> {
    fn from(value: Vec<u8>) -> Self {
        raw_vec(value)
    }
}

impl From<String> for Raw<String> {
    fn from(value: String) -> Self {
        raw_string(value)
    }
}

impl<'a> From<&'a [u8]> for Raw<&'a [u8]> {
    fn from(value: &'a [u8]) -> Self {
        raw_slice(value)
    }
}

impl<'a> From<&'a str> for Raw<&'a str> {
    fn from(value: &'a str) -> Self {
        raw_str(value)
    }
}

impl<T: AsRef<[u8]>> Raw<T> {
    /// Returns `true` if this value has a length of zero bytes.
    ///
    /// datasketches-cpp ignores empty byte and string inputs before hashing in some update paths.
    /// Check this method before updating a sketch when matching that behavior matters.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::hash_value::{raw_slice, raw_str, raw_string, raw_vec};
    /// assert!(raw_vec(Vec::new()).is_empty());
    /// assert!(raw_string(String::new()).is_empty());
    /// assert!(raw_slice(&[]).is_empty());
    /// assert!(raw_str("").is_empty());
    ///
    /// assert!(!raw_vec(b"abc".to_vec()).is_empty());
    /// assert!(!raw_string("abc".to_owned()).is_empty());
    /// assert!(!raw_slice(b"abc").is_empty());
    /// assert!(!raw_str("abc").is_empty());
    /// ```
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }
}

impl<T: AsRef<[u8]>> HashStrategy<T> for RawStrategy {
    fn hash<H: Hasher>(value: &T, state: &mut H) {
        state.write(value.as_ref());
    }
}
