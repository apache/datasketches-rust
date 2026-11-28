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

use std::ops::AddAssign;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

mod sealed {
    pub trait Sealed {}

    impl Sealed for f32 {}
    impl Sealed for f64 {}
}

/// A fixed-width numeric value supported by the ArrayOfNumbers compact wire format.
///
/// This trait is sealed and currently implemented only for `f32` and `f64`.
pub trait ArrayOfNumbersValue: sealed::Sealed + Copy + Default + AddAssign + 'static {
    #[doc(hidden)]
    /// Writes this value using the compact ArrayOfNumbers little-endian encoding.
    fn write_le(self, bytes: &mut SketchBytes);

    #[doc(hidden)]
    /// Reads this value from the compact ArrayOfNumbers little-endian encoding.
    fn read_le(cursor: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

impl ArrayOfNumbersValue for f32 {
    fn write_le(self, bytes: &mut SketchBytes) {
        bytes.write_f32_le(self);
    }

    fn read_le(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_f32_le()
            .map_err(insufficient_data("entry_values"))
    }
}

impl ArrayOfNumbersValue for f64 {
    fn write_le(self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(self);
    }

    fn read_le(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_f64_le()
            .map_err(insufficient_data("entry_values"))
    }
}

/// Combines value arrays of type `T` for keys shared by two ArrayOfNumbers sketches.
pub trait ArrayOfNumbersCombiner<T> {
    /// Combines `incoming` into `existing` in place.
    fn combine(&self, existing: &mut [T], incoming: &[T]);
}

/// Combines the value arrays of keys shared by two ArrayOfDoubles sketches during intersection.
pub trait ArrayOfDoublesCombiner {
    /// Combines `incoming` into `existing` in place.
    ///
    /// Both slices have length equal to the intersection operator's configured `num_values`.
    fn combine(&self, existing: &mut [f64], incoming: &[f64]);
}

impl<C: ArrayOfDoublesCombiner> ArrayOfNumbersCombiner<f64> for C {
    fn combine(&self, existing: &mut [f64], incoming: &[f64]) {
        ArrayOfDoublesCombiner::combine(self, existing, incoming);
    }
}

pub fn validate_num_values(num_values: u8) -> Result<(), Error> {
    if num_values == 0 {
        return Err(Error::invalid_argument(
            "num_values must be greater than 0, got 0",
        ));
    }
    Ok(())
}

pub fn check_num_values(expected: u8, actual: usize, operation: &str) -> Result<(), Error> {
    if actual != usize::from(expected) {
        return Err(Error::invalid_argument(format!(
            "num_values mismatch for {operation}: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}
