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

//! Canonical floating-point hash value wrappers.
//!
//! [`CanonicalFloat`] maps `f32` and `f64` values through a canonical `f64` bit pattern before
//! hashing. Signed zero values hash the same, all NaN values use one canonical NaN bit pattern,
//! and equal `f32`/`f64` values hash the same.
//!
//! This strategy is compatible with how other datasketches hashes floating-point numbers.

use std::hash::Hash;
use std::hash::Hasher;

use crate::common::float::canonical_f64_bits;
use crate::hash::value::HashStrategy;
use crate::hash::value::Value;

/// A floating-point value wrapper that uses canonical floating-point hashing.
///
/// See the [module level documentation](super) for more.
pub type CanonicalFloat<T> = Value<T, CanonicalFloatStrategy>;

/// Hashing strategy for [`CanonicalFloat`].
#[doc(hidden)]
pub struct CanonicalFloatStrategy;

/// Creates a canonical hashable value from an `f32` value.
///
/// `f32` values are converted to `f64` before hashing. Values that are not exactly representable
/// in `f32` may hash differently from the corresponding `f64` value. Signed zero values hash the
/// same, and all NaN values use one canonical NaN bit pattern.
///
/// # Examples
///
/// ```
/// use datasketches::hash::value::calculate_hash;
/// use datasketches::hash::value::canonical_float;
///
/// assert_eq!(
///     calculate_hash(canonical_float::from_f32(0.0)),
///     calculate_hash(canonical_float::from_f32(-0.0))
/// );
/// assert_eq!(
///     calculate_hash(canonical_float::from_f32(5.0)),
///     calculate_hash(canonical_float::from_f64(5.0))
/// );
/// assert_ne!(
///     calculate_hash(canonical_float::from_f32(3.15)),
///     calculate_hash(canonical_float::from_f64(3.15))
/// );
/// ```
#[inline(always)]
pub fn from_f32(v: f32) -> CanonicalFloat<f32> {
    CanonicalFloat::new(v)
}

/// Creates a canonical hashable value from an `f64` value.
///
/// Signed zero values hash the same, and all NaN values use one canonical NaN bit pattern.
///
/// # Examples
///
/// ```
/// use datasketches::hash::value::calculate_hash;
/// use datasketches::hash::value::canonical_float;
///
/// assert_eq!(
///     calculate_hash(canonical_float::from_f64(0.0)),
///     calculate_hash(canonical_float::from_f64(-0.0))
/// );
/// assert_eq!(
///     calculate_hash(canonical_float::from_f32(5.0)),
///     calculate_hash(canonical_float::from_f64(5.0))
/// );
/// assert_ne!(
///     calculate_hash(canonical_float::from_f32(3.15)),
///     calculate_hash(canonical_float::from_f64(3.15))
/// );
/// ```
#[inline(always)]
pub fn from_f64(v: f64) -> CanonicalFloat<f64> {
    CanonicalFloat::new(v)
}

impl HashStrategy<f32> for CanonicalFloatStrategy {
    fn hash<H: Hasher>(value: &f32, state: &mut H) {
        let value = *value as f64;
        let canonical_value = from_f64(value);
        canonical_value.hash(state);
    }
}

impl HashStrategy<f64> for CanonicalFloatStrategy {
    fn hash<H: Hasher>(value: &f64, state: &mut H) {
        canonical_f64_bits(*value).hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::value::calculate_hash;

    #[test]
    fn canonical_hash_equates_signed_zeros_and_nans() {
        assert_eq!(
            calculate_hash(from_f64(-0.0)),
            calculate_hash(from_f64(0.0))
        );

        let positive_nan = f64::from_bits(0x7ff8000000000001);
        let negative_nan = f64::from_bits(0xfff8000000000002);
        assert_eq!(
            calculate_hash(from_f64(positive_nan)),
            calculate_hash(from_f64(negative_nan))
        );
    }
}
