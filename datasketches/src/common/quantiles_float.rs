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

use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;

use crate::error::Error;

/// A non-NaN float with a total order for quantile queries.
///
/// Infinities sort numerically. `-0.0` and `+0.0` compare equal and retain their signs.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub struct QuantilesFloat<T>(T);

impl<T> QuantilesFloat<T> {
    /// Returns the wrapped floating-point value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for QuantilesFloat<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for QuantilesFloat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for QuantilesFloat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

macro_rules! impl_quantiles_float {
    ($float:ty) => {
        impl QuantilesFloat<$float> {
            /// Wraps a floating-point value for use in a quantiles sketch.
            ///
            /// # Errors
            ///
            /// Returns `InvalidArgument` if `value` is NaN.
            #[inline]
            pub fn new(value: $float) -> Result<Self, Error> {
                if value.is_nan() {
                    return Err(Error::invalid_argument(
                        "floating-point item must not be NaN",
                    ));
                }
                Ok(Self(value))
            }
        }

        impl Eq for QuantilesFloat<$float> {}

        impl Ord for QuantilesFloat<$float> {
            #[inline]
            fn cmp(&self, other: &Self) -> Ordering {
                // Construction rules out NaN, so these comparisons cover every possible value.
                if self.0 < other.0 {
                    Ordering::Less
                } else if self.0 > other.0 {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }
    };
}

impl_quantiles_float!(f32);
impl_quantiles_float!(f64);
