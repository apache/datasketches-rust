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
//! cases where the default implementation does not match the required input encoding.
//!
//! ## Encoding strategies
//!
//! * [`canonical_float`] widens floating-point values to `f64`, normalizes signed zero to `+0.0`,
//!   and maps all NaNs to one bit pattern before hashing.
//! * [`sign_extend`] interprets integers as signed at their original width and sign-extends them to
//!   64 bits before hashing.
//! * [`natural_extend`] widens signed integers to `i64` and unsigned integers to `u64` before
//!   hashing.
//! * [`raw_bytes`] hashes byte slices as their contents and strings as UTF-8 bytes, without length
//!   prefixes or terminators.
//!
//! Each strategy module provides constructors and examples.

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
    #[inline(always)]
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
