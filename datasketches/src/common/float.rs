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

#[cfg(feature = "req")]
use std::cmp::Ordering;

/// Returns a canonical `f64` bit pattern for DataSketches hashing.
#[inline(always)]
pub(crate) fn canonical_f64_bits(value: f64) -> u64 {
    if value.is_nan() {
        // Java's Double.doubleToLongBits() NaN value.
        0x7ff8000000000000u64
    } else {
        // -0.0 + 0.0 == +0.0 under IEEE754 roundTiesToEven rounding mode,
        // which Rust guarantees. Thus, adding a positive zero canonicalizes
        // signed zero without a branch.
        (value + 0.0).to_bits()
    }
}

/// Compares `f32` values with signed zeros equal and all NaNs equal and ordered last.
#[cfg(feature = "req")]
#[inline(always)]
pub(crate) fn canonical_cmp_f32(left: &f32, right: &f32) -> Ordering {
    match left.partial_cmp(right) {
        Some(ordering) => ordering,
        None => left.is_nan().cmp(&right.is_nan()),
    }
}

/// Compares `f64` values with signed zeros equal and all NaNs equal and ordered last.
#[cfg(feature = "req")]
#[inline(always)]
pub(crate) fn canonical_cmp_f64(left: &f64, right: &f64) -> Ordering {
    match left.partial_cmp(right) {
        Some(ordering) => ordering,
        None => left.is_nan().cmp(&right.is_nan()),
    }
}
