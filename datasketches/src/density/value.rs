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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;

/// Sealed marker trait identifying coordinate value types supported by
/// [`DensitySketch`](crate::density::DensitySketch).
///
/// Implemented for `f32` and `f64`.
pub trait DensityValue: private::DensityValue {}

mod private {
    use std::fmt;

    use crate::codec::SketchBytes;
    use crate::codec::SketchSlice;

    pub trait DensityValue: Copy + Send + Sync + fmt::Debug + 'static {
        const BYTE_LEN: usize;

        fn is_finite_val(self) -> bool;
        fn to_f64(self) -> f64;
        fn write_le(self, buf: &mut SketchBytes);
        fn read_le(cursor: &mut SketchSlice<'_>) -> std::io::Result<Self>;
    }
}

macro_rules! impl_density_value {
    ($ty:ty, $write_fn:ident, $read_fn:ident) => {
        impl private::DensityValue for $ty {
            const BYTE_LEN: usize = size_of::<Self>();

            #[inline]
            fn is_finite_val(self) -> bool {
                self.is_finite()
            }

            #[inline]
            fn to_f64(self) -> f64 {
                f64::from(self)
            }

            #[inline]
            fn write_le(self, buf: &mut SketchBytes) {
                buf.$write_fn(self);
            }

            #[inline]
            fn read_le(cursor: &mut SketchSlice<'_>) -> std::io::Result<Self> {
                cursor.$read_fn()
            }
        }

        impl DensityValue for $ty {}
    };
}

impl_density_value!(f32, write_f32_le, read_f32_le);
impl_density_value!(f64, write_f64_le, read_f64_le);
