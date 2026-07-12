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

/// Marker trait identifying the value types supported by
/// [`CountMinSketch`](crate::countmin::CountMinSketch).
pub trait CountMinValue: private::CountMinValue {}

/// Marker trait identifying the unsigned value types supported by
/// [`CountMinSketch`](crate::countmin::CountMinSketch).
///
/// This marker enables unsigned-only operations such as halving and decay.
pub trait UnsignedCountMinValue: CountMinValue + private::UnsignedCountMinValue {}

mod private {
    use std::ops::Add;

    use crate::error::Error;

    pub trait CountMinValue: Sized + Copy + Ord + Add<Output = Self> {
        const ZERO: Self;
        const ONE: Self;
        const MAX: Self;

        fn abs(self) -> Self;
        fn scale(self, factor: f64) -> Self;
        fn to_bytes(self) -> [u8; 8];
        fn try_from_bytes(bytes: [u8; 8]) -> Result<Self, Error>;
    }

    pub trait UnsignedCountMinValue: CountMinValue {
        fn halve(self) -> Self;
    }
}

macro_rules! impl_signed {
    ($name:ty, $min:expr, $max:expr) => {
        impl private::CountMinValue for $name {
            const ZERO: Self = 0;
            const ONE: Self = 1;
            const MAX: Self = $max;

            #[inline(always)]
            fn abs(self) -> Self {
                if self >= 0 { self } else { -self }
            }

            #[inline(always)]
            fn scale(self, factor: f64) -> Self {
                ((self as f64) * factor).trunc() as $name
            }

            #[inline(always)]
            fn to_bytes(self) -> [u8; 8] {
                let value = self as i64;
                value.to_le_bytes()
            }

            #[inline(always)]
            fn try_from_bytes(bytes: [u8; 8]) -> Result<Self, Error> {
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

        impl CountMinValue for $name {}
    };
}

impl_signed!(i8, i8::MIN, i8::MAX);
impl_signed!(i16, i16::MIN, i16::MAX);
impl_signed!(i32, i32::MIN, i32::MAX);
impl_signed!(i64, i64::MIN, i64::MAX);

macro_rules! impl_unsigned {
    ($name:ty, $max:expr) => {
        impl private::CountMinValue for $name {
            const ZERO: Self = 0;
            const ONE: Self = 1;
            const MAX: Self = $max;

            #[inline(always)]
            fn abs(self) -> Self {
                self
            }

            #[inline(always)]
            fn scale(self, factor: f64) -> Self {
                ((self as f64) * factor).trunc() as $name
            }

            #[inline(always)]
            fn to_bytes(self) -> [u8; 8] {
                let value = self as u64;
                value.to_le_bytes()
            }

            #[inline(always)]
            fn try_from_bytes(bytes: [u8; 8]) -> Result<Self, Error> {
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

        impl private::UnsignedCountMinValue for $name {
            #[inline(always)]
            fn halve(self) -> Self {
                self >> 1
            }
        }

        impl CountMinValue for $name {}
        impl UnsignedCountMinValue for $name {}
    };
}

impl_unsigned!(u8, u8::MAX);
impl_unsigned!(u16, u16::MAX);
impl_unsigned!(u32, u32::MAX);
impl_unsigned!(u64, u64::MAX);
