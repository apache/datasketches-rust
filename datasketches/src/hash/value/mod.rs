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
//! ## Floating-point Numbers
//!
//! [`canonical_float::CanonicalFloat`] maps `f32` and `f64` values through a canonical `f64` bit
//! pattern before hashing. Signed zero values hash the same, all NaN values use one canonical NaN
//! bit pattern, and equal `f32`/`f64` values hash the same.
//!
//! This strategy is the same as how other datasketches implementations hash floating-point numbers.
//!
//! Read the docs of concrete value wrapper for more details and examples.
//!
//! * [`canonical_float::from_f32`]
//! * [`canonical_float::from_f64`]
//!
//! ## Integers
//!
//! [`sign_extend::SignExtend`] first sign-extends values to 64-bits, and then hashes the resulting
//! integers. This strategy is the same as how datasketches-cpp hashes short integers for
//! `HllSketch` and `CpcSketch`.
//!
//! Read the docs of concrete value wrapper for more details and examples.
//!
//! * [`sign_extend::from_i8`], [`sign_extend::from_u8`]
//! * [`sign_extend::from_i16`], [`sign_extend::from_u16`]
//! * [`sign_extend::from_i32`], [`sign_extend::from_u32`]
//!
//! [`natural_extend::NaturalExtend`] first widens signed values to `i64` and unsigned values to
//! `u64`, and then hashes the resulting integers. This strategy is the same as how datasketches-cpp
//! hashes short integers for `BloomFilter`.
//!
//! Read the docs of concrete value wrapper for more details and examples.
//!
//! * [`natural_extend::from_i8`], [`natural_extend::from_u8`]
//! * [`natural_extend::from_i16`], [`natural_extend::from_u16`]
//! * [`natural_extend::from_i32`], [`natural_extend::from_u32`]
//!
//! ## Raw Bytes
//!
//! [`raw_bytes::RawBytes`] hashes byte and string inputs as raw bytes without Rust's slice or
//! string length prefix.
//!
//! Empty byte and string inputs have zero bytes to hash. Other datasketches implementations skip
//! empty strings before hashing, so check `is_empty` before updating a sketch when that behavior
//! matters.
//!
//! Read the docs of concrete value wrapper for more details and examples.
//!
//! * [`raw_bytes::from_vec`]
//! * [`raw_bytes::from_string`]
//! * [`raw_bytes::from_slice`]
//! * [`raw_bytes::from_str`]

pub mod canonical_float;
pub mod natural_extend;
pub mod raw_bytes;
pub mod sign_extend;

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;

/// A value wrapper that hashes its inner value with strategy `S`.
///
/// Most users should prefer the strategy-specific constructors.
#[doc(hidden)]
pub struct Value<T, S> {
    value: T,
    strategy: PhantomData<fn() -> S>,
}

impl<T, S> Value<T, S> {
    /// Create a value wrapper.
    #[inline(always)]
    pub fn new(value: T) -> Self {
        Self {
            value,
            strategy: PhantomData,
        }
    }

    /// Get the value out.
    #[inline(always)]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T, S> Deref for Value<T, S> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T, S> DerefMut for Value<T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T: Clone, S> Clone for Value<T, S> {
    fn clone(&self) -> Self {
        Self::new(self.value.clone())
    }
}

impl<T: Copy, S> Copy for Value<T, S> {}

impl<T: PartialEq, S> PartialEq for Value<T, S> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq, S> Eq for Value<T, S> {}

impl<T: PartialOrd, S> PartialOrd for Value<T, S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl<T: Ord, S> Ord for Value<T, S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<T: fmt::Debug, S> fmt::Debug for Value<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.value, f)
    }
}

impl<T: fmt::Display, S> fmt::Display for Value<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl<T, S: HashStrategy<T>> Hash for Value<T, S> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        S::hash(&self.value, state);
    }
}

#[doc(hidden)]
pub trait HashStrategy<T> {
    fn hash<H: Hasher>(value: &T, state: &mut H);
}

#[doc(hidden)] // for doctest
pub fn calculate_hash<T: Hash>(t: T) -> u64 {
    use std::hash::DefaultHasher;

    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
