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

//! Floating-point hash value wrappers.
//!
//! [`Canonical`] maps `f32` and `f64` through the same canonical `f64` bit pattern before hashing.

use std::hash::Hash;
use std::hash::Hasher;

use super::value::HashStrategy;
use super::value::Value;

/// A floating-point value wrapper that uses canonical floating-point hashing.
///
/// The wrapper canonicalizes signed zero and NaN bit patterns, and hashes `f32` values through
/// their `f64` representation.
pub type Canonical<T> = Value<T, CanonicalStrategy>;

/// Hashing strategy for [`Canonical`].
#[doc(hidden)]
pub struct CanonicalStrategy;

/// Create a canonical hashable value from a `f32` value.
///
/// `f32` values are converted to `f64` before hashing, so `canonical_f32(5.0)` hashes the same as
/// `canonical_f64(5.0)`. Signed zero values hash the same, and all NaN values use one canonical
/// NaN bit pattern.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_f32, canonical_f64};
/// assert_eq!(
///     calculate_hash(canonical_f32(0.0)),
///     calculate_hash(canonical_f32(-0.0))
/// );
/// assert_eq!(
///     calculate_hash(canonical_f32(5.0)),
///     calculate_hash(canonical_f64(5.0))
/// );
/// ```
pub fn canonical_f32(v: f32) -> Canonical<f32> {
    Canonical::new(v)
}

/// Create a canonical hashable value from a `f64` value.
///
/// Signed zero values hash the same, and all NaN values use one canonical NaN bit pattern.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_f32, canonical_f64};
/// assert_eq!(
///     calculate_hash(canonical_f64(0.0)),
///     calculate_hash(canonical_f64(-0.0))
/// );
/// assert_eq!(
///     calculate_hash(canonical_f32(5.0)),
///     calculate_hash(canonical_f64(5.0))
/// );
/// ```
pub fn canonical_f64(v: f64) -> Canonical<f64> {
    Canonical::new(v)
}

impl From<f32> for Canonical<f32> {
    fn from(value: f32) -> Self {
        canonical_f32(value)
    }
}

impl From<f64> for Canonical<f64> {
    fn from(value: f64) -> Self {
        canonical_f64(value)
    }
}

impl HashStrategy<f32> for CanonicalStrategy {
    fn hash<H: Hasher>(value: &f32, state: &mut H) {
        canonical_f64(*value as f64).hash(state);
    }
}

impl HashStrategy<f64> for CanonicalStrategy {
    fn hash<H: Hasher>(value: &f64, state: &mut H) {
        let canonical = if value.is_nan() {
            // Java's Double.doubleToLongBits() NaN value.
            0x7ff8000000000000u64
        } else {
            // -0.0 + 0.0 == +0.0 under IEEE754 roundTiesToEven rounding mode,
            // which Rust guarantees. Thus, by adding a positive zero we
            // canonicalize signed zero without any branches in one instruction.
            (value + 0.0).to_bits()
        };
        canonical.hash(state);
    }
}
