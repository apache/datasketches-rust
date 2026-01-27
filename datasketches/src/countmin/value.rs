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

use crate::error::Error;

mod private {
    // Sealed trait to prevent external implementations of CountMinValue.
    pub trait Sealed {}
}

/// Value type used for counters and weights in a Count-Min sketch.
pub trait CountMinValue: private::Sealed + Copy + Ord {
    /// Zero value for counters and weights.
    const ZERO: Self;

    /// One value for unit updates.
    const ONE: Self;

    /// Maximum representable value for initializing minima.
    const MAX: Self;

    /// Adds with wrapping semantics.
    fn wrapping_add(self, other: Self) -> Self;

    /// Returns the absolute value for weight tracking.
    fn abs(self) -> Self;

    /// Converts into `f64` for bound computations.
    fn to_f64(self) -> f64;

    /// Converts from `f64` by truncating toward zero.
    fn from_f64(value: f64) -> Self;

    /// Returns the raw transmutation in 8 bits.
    fn to_bits(self) -> [u8; 8];

    /// Constructs from the raw transmutation in 8 bits.
    fn try_from_bits(bytes: [u8; 8]) -> Result<Self, Error>;
}

pub trait UnsignedCountValue: CountMinValue {
    fn halve(self) -> Self;
    fn decay(self, decay: f64) -> Self;
}

macro_rules! impl_signed {
    ($name:ty, $min:expr, $max:expr) => {
        impl private::Sealed for $name {}

        impl CountMinValue for $name {
            const ZERO: Self = 0;
            const ONE: Self = 1;
            const MAX: Self = $max;

            #[inline(always)]
            fn wrapping_add(self, other: Self) -> Self {
                self.wrapping_add(other)
            }

            #[inline(always)]
            fn abs(self) -> Self {
                if self >= 0 { self } else { self.wrapping_neg() }
            }

            #[inline(always)]
            fn to_f64(self) -> f64 {
                self as f64
            }

            #[inline(always)]
            fn from_f64(value: f64) -> Self {
                value.trunc() as $name
            }

            #[inline(always)]
            fn to_bits(self) -> [u8; 8] {
                let value = self as i64;
                value.to_le_bytes()
            }

            #[inline(always)]
            fn try_from_bits(bytes: [u8; 8]) -> Result<Self, Error> {
                let value = i64::from_le_bytes(bytes);
                if value < $min as i64 || value > $max as i64 {
                    return Err(Error::deserial(format!(
                        "value {} out of range for {}",
                        value,
                        stringify!($name)
                    )));
                }
                Ok(value as $name)
            }
        }
    };
}

impl_signed!(i8, i8::MIN, i8::MAX);
impl_signed!(i16, i16::MIN, i16::MAX);
impl_signed!(i32, i32::MIN, i32::MAX);
impl_signed!(i64, i64::MIN, i64::MAX);

macro_rules! impl_unsigned {
    ($name:ty, $max:expr) => {
        impl private::Sealed for $name {}

        impl CountMinValue for $name {
            const ZERO: Self = 0;
            const ONE: Self = 1;
            const MAX: Self = $max;

            #[inline(always)]
            fn wrapping_add(self, other: Self) -> Self {
                self.wrapping_add(other)
            }

            #[inline(always)]
            fn abs(self) -> Self {
                self
            }

            #[inline(always)]
            fn to_f64(self) -> f64 {
                self as f64
            }

            #[inline(always)]
            fn from_f64(value: f64) -> Self {
                value.trunc() as $name
            }

            #[inline(always)]
            fn to_bits(self) -> [u8; 8] {
                let value = self as u64;
                value.to_le_bytes()
            }

            #[inline(always)]
            fn try_from_bits(bytes: [u8; 8]) -> Result<Self, Error> {
                let value = u64::from_le_bytes(bytes);
                if value > $max as u64 {
                    return Err(Error::deserial(format!(
                        "value {} out of range for {}",
                        value,
                        stringify!($name)
                    )));
                }
                Ok(value as $name)
            }
        }

        impl UnsignedCountValue for $name {
            #[inline(always)]
            fn halve(self) -> Self {
                self / 2
            }

            #[inline(always)]
            fn decay(self, decay: f64) -> Self {
                let value = self.to_f64() * decay;
                Self::from_f64(value)
            }
        }
    };
}

impl_unsigned!(u8, u8::MAX);
impl_unsigned!(u16, u16::MAX);
impl_unsigned!(u32, u32::MAX);
impl_unsigned!(u64, u64::MAX);
