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

use super::serialization::decode_uvarint64;
use super::serialization::decode_varfloat64;
use super::serialization::encode_uvarint64;
use super::serialization::encode_varfloat64;
use super::serialization::uvarint64_size;
use super::serialization::varfloat64_size;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

const MAX_EXACT_F64_INT: u64 = (1_u64 << 53) - 1;
const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;

pub const CONFIG_FLOAT_WEIGHTS: u32 = 1 << 31;

/// Marker trait identifying the bucket weight types supported by
/// [`DdSketch`](super::DdSketch).
///
/// Implemented for `u64` (exact associative integer weights, the default) and `f64` (non-negative
/// finite floating-point weights for fractional counts and full compatibility with the Java and Go
/// reference implementations).
pub trait DdSketchWeight: private::DdSketchWeight {}

mod private {
    use std::cmp::Ordering;
    use std::fmt;

    use crate::codec::SketchBytes;
    use crate::codec::SketchSlice;
    use crate::error::Error;

    pub trait DdSketchWeight: Sized + Copy + PartialOrd + fmt::Debug + fmt::Display {
        const ZERO: Self;
        const ONE: Self;
        const TYPE_NAME: &'static str;
        const NATIVE_CONFIG_FLAG: u32;
        const NATIVE_MIN_BYTES: usize;
        const NATIVE_ZERO_BYTES: usize;
        const ALLOWS_SUBUNIT_SUM_UNDERFLOW: bool;

        fn is_zero(self) -> bool;
        fn is_one(self) -> bool;
        fn validate_update_weight(self) -> Self;
        fn checked_add(self, rhs: Self) -> Option<Self>;
        fn add_unchecked(self, rhs: Self) -> Self;
        fn sub_saturating(self, rhs: Self) -> Self;
        fn to_f64(self) -> f64;
        fn total_cmp(self, rhs: Self) -> Ordering;
        fn target_rank(rank: f64, total: Self) -> Self;

        fn native_size(self) -> usize;
        fn encode_native(self, out: &mut SketchBytes);
        fn decode_native(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<Self, Error>;

        fn check_compat_encode(self, label: &'static str) -> Result<(), Error>;
        fn compat_size(self) -> usize;
        fn encode_compat(self, out: &mut SketchBytes);
        fn decode_compat(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<Self, Error>;
        fn validate_compat_summary_count(
            saw_count: bool,
            summary_count: Self,
            bucket_total: Self,
        ) -> Result<(), Error>;
    }
}

impl DdSketchWeight for u64 {}
impl DdSketchWeight for f64 {}

impl private::DdSketchWeight for u64 {
    const ZERO: Self = 0;
    const ONE: Self = 1;
    const TYPE_NAME: &'static str = "u64";
    const NATIVE_CONFIG_FLAG: u32 = 0;
    const NATIVE_MIN_BYTES: usize = 1;
    const NATIVE_ZERO_BYTES: usize = 1;
    const ALLOWS_SUBUNIT_SUM_UNDERFLOW: bool = false;

    #[inline]
    fn is_zero(self) -> bool {
        self == 0
    }

    #[inline]
    fn is_one(self) -> bool {
        self == 1
    }

    #[inline]
    fn validate_update_weight(self) -> Self {
        self
    }

    #[inline]
    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    #[inline]
    fn add_unchecked(self, rhs: Self) -> Self {
        self + rhs
    }

    #[inline]
    fn sub_saturating(self, rhs: Self) -> Self {
        self.saturating_sub(rhs)
    }

    #[inline]
    fn to_f64(self) -> f64 {
        self as f64
    }

    #[inline]
    fn total_cmp(self, rhs: Self) -> Ordering {
        self.cmp(&rhs)
    }

    #[inline]
    fn target_rank(rank: f64, total: Self) -> Self {
        debug_assert!(total > 0);
        quantile_target_index(rank, total - 1)
    }

    #[inline]
    fn native_size(self) -> usize {
        uvarint64_size(self)
    }

    #[inline]
    fn encode_native(self, out: &mut SketchBytes) {
        encode_uvarint64(out, self);
    }

    #[inline]
    fn decode_native(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<Self, Error> {
        decode_uvarint64(cursor, tag)
    }

    #[inline]
    fn check_compat_encode(self, label: &'static str) -> Result<(), Error> {
        if self > MAX_EXACT_F64_INT {
            return Err(Error::invalid_argument(format!(
                "DDSketch {label} ({self}) exceeds maximum exact compatibility count ({MAX_EXACT_F64_INT})"
            )));
        }
        Ok(())
    }

    #[inline]
    fn compat_size(self) -> usize {
        varfloat64_size(self as f64)
    }

    #[inline]
    fn encode_compat(self, out: &mut SketchBytes) {
        encode_varfloat64(out, self as f64);
    }

    #[inline]
    fn decode_compat(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<Self, Error> {
        let raw = decode_varfloat64(cursor, tag)?;
        if !raw.is_finite() || raw < 0.0 || raw.fract() != 0.0 || raw >= TWO_POW_64 {
            return Err(Error::deserial(format!(
                "invalid reference DDSketch {tag}: expected non-negative integer < 2^64, got {raw}"
            )));
        }
        Ok(raw as u64)
    }

    #[inline]
    fn validate_compat_summary_count(
        saw_count: bool,
        summary_count: Self,
        bucket_total: Self,
    ) -> Result<(), Error> {
        if !saw_count || summary_count != bucket_total {
            return Err(Error::deserial(format!(
                "reference DDSketch exact_count ({summary_count}) does not match total bucket weight ({bucket_total})"
            )));
        }
        Ok(())
    }
}

impl private::DdSketchWeight for f64 {
    const ZERO: Self = 0.0;
    const ONE: Self = 1.0;
    const TYPE_NAME: &'static str = "f64";
    const NATIVE_CONFIG_FLAG: u32 = CONFIG_FLOAT_WEIGHTS;
    const NATIVE_MIN_BYTES: usize = size_of::<f64>();
    const NATIVE_ZERO_BYTES: usize = size_of::<f64>();
    const ALLOWS_SUBUNIT_SUM_UNDERFLOW: bool = true;

    #[inline]
    fn is_zero(self) -> bool {
        self == 0.0
    }

    #[inline]
    fn is_one(self) -> bool {
        self == 1.0
    }

    #[inline]
    fn validate_update_weight(self) -> Self {
        assert!(
            self.is_finite() && self >= 0.0,
            "weight must be finite and non-negative, got {self}"
        );
        if self == 0.0 { 0.0 } else { self }
    }

    #[inline]
    fn checked_add(self, rhs: Self) -> Option<Self> {
        let sum = self + rhs;
        sum.is_finite().then_some(sum)
    }

    #[inline]
    fn add_unchecked(self, rhs: Self) -> Self {
        self + rhs
    }

    #[inline]
    fn sub_saturating(self, rhs: Self) -> Self {
        (self - rhs).max(0.0)
    }

    #[inline]
    fn to_f64(self) -> f64 {
        self
    }

    #[inline]
    fn total_cmp(self, rhs: Self) -> Ordering {
        f64::total_cmp(&self, &rhs)
    }

    #[inline]
    fn target_rank(rank: f64, total: Self) -> Self {
        debug_assert!(total > 0.0);
        if rank == 0.0 {
            return 0.0;
        }
        if rank == 1.0 {
            return total;
        }
        (rank * (total - 1.0)).max(0.0)
    }

    #[inline]
    fn native_size(self) -> usize {
        size_of::<f64>()
    }

    #[inline]
    fn encode_native(self, out: &mut SketchBytes) {
        out.write_f64_le(if self == 0.0 { 0.0 } else { self });
    }

    #[inline]
    fn decode_native(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<Self, Error> {
        let value = cursor.read_f64_le().map_err(insufficient_data(tag))?;
        if !value.is_finite() || value.is_sign_negative() {
            return Err(Error::deserial(format!(
                "invalid DDSketch {tag}: expected non-negative finite f64, got {value}"
            )));
        }
        Ok(value)
    }

    #[inline]
    fn check_compat_encode(self, _label: &'static str) -> Result<(), Error> {
        Ok(())
    }

    #[inline]
    fn compat_size(self) -> usize {
        varfloat64_size(self)
    }

    #[inline]
    fn encode_compat(self, out: &mut SketchBytes) {
        encode_varfloat64(out, self);
    }

    #[inline]
    fn decode_compat(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<Self, Error> {
        let raw = decode_varfloat64(cursor, tag)?;
        if !raw.is_finite() || raw < 0.0 {
            return Err(Error::deserial(format!(
                "invalid reference DDSketch {tag}: expected non-negative finite f64, got {raw}"
            )));
        }
        Ok(if raw == 0.0 { 0.0 } else { raw })
    }

    #[inline]
    fn validate_compat_summary_count(
        saw_count: bool,
        summary_count: Self,
        _bucket_total: Self,
    ) -> Result<(), Error> {
        // The compatibility format encodes both `FlagCount` and individual bin weights via
        // `varfloat64` (`v + 1.0`), so fractional counts can differ in the last bits from the sum
        // of decoded bin weights. Require a positive finite `FlagCount`, matching Java and Go.
        if !saw_count || !summary_count.is_finite() || summary_count <= 0.0 {
            return Err(Error::deserial(format!(
                "invalid reference DDSketch exact_count ({summary_count}) for non-empty sketch"
            )));
        }
        Ok(())
    }
}

#[inline]
fn quantile_target_index(rank: f64, last: u64) -> u64 {
    if rank == 0.0 || last == 0 {
        return 0;
    }
    if rank == 1.0 {
        return last;
    }
    if last <= MAX_EXACT_F64_INT {
        // When `last < 2^53`, `last as f64` is exact; evaluating the `f64` product matches
        // `sketches-java` (`(int) (q * (count - 1))`) so decimal ranks such as `0.95 * 100.0 ==
        // 95.0` select item 95 rather than truncating the slightly smaller binary value of `0.95`
        // to item 94.
        return ((rank * (last as f64)).floor() as u64).min(last);
    }
    // Multiply the exact 53-bit IEEE 754 significand by `last` in u128 before shifting so ranks
    // retain full precision when `total_weight` exceeds 2^53.
    let bits = rank.to_bits();
    let biased_exp = ((bits >> 52) & 0x7ff) as u32;
    if biased_exp == 0 {
        return 0;
    }
    let significand = (bits & 0x000f_ffff_ffff_ffff) | (1_u64 << 52);
    let product = u128::from(significand) * u128::from(last);
    let shift = 1075 - biased_exp;
    if shift >= 128 {
        0
    } else {
        (product >> shift) as u64
    }
}
