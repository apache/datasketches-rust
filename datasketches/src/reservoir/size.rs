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

//! Legacy 16-bit fixed-point reservoir size decoding for serial version 1 images.
//!
//! Matches `org.apache.datasketches.sampling.ReservoirSize` in `datasketches-java`.

use crate::error::Error;

const BINS_PER_OCTAVE: u64 = 2048;
const EXPONENT_MASK: u16 = 0x1F;
const EXPONENT_SHIFT: u16 = 11;
const INDEX_MASK: u16 = 0x07FF;
const MAX_ENC_VALUE: u16 = 0xF7FF; // p = 30, i = 2047 -> 2_146_959_360

/// Decodes a 16-bit version-1 encoded reservoir size into its integer value.
pub fn decode_value(encoded: u16) -> Result<usize, Error> {
    if encoded > MAX_ENC_VALUE {
        return Err(Error::deserial(format!(
            "invalid encoded reservoir size: maximum is {MAX_ENC_VALUE:#06x}, got {encoded:#06x}"
        )));
    }

    let p = u32::from((encoded >> EXPONENT_SHIFT) & EXPONENT_MASK);
    let i = u64::from(encoded & INDEX_MASK);

    // `(1 << p) * (1.0 + i / 2048.0)` is exact in IEEE 754 `f64` because `1 / 2048 = 2^-11` and
    // `2048 + i` fits in 12 bits; integer shift avoids floating-point conversion completely.
    let decoded = ((BINS_PER_OCTAVE + i) << p) >> EXPONENT_SHIFT;
    usize::try_from(decoded)
        .map_err(|_| Error::deserial(format!("decoded reservoir size {decoded} overflows usize")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_ABS_VALUE: usize = 2_146_959_360;

    fn compute_size_for_test(k: usize) -> u16 {
        assert!((1..=MAX_ABS_VALUE).contains(&k));
        let k_u32 = k as u32;
        let p = k_u32.ilog2() as u16;
        if k_u32.is_power_of_two() {
            return (p & EXPONENT_MASK) << EXPONENT_SHIFT;
        }
        let inv_ln_2 = 1.0 / 2.0_f64.ln();
        let m = 2.0_f64.powf((k as f64).ln() * inv_ln_2 - f64::from(p));
        let i = ((m * BINS_PER_OCTAVE as f64).floor() as i64 - BINS_PER_OCTAVE as i64) + 1;
        if i == BINS_PER_OCTAVE as i64 {
            return ((p + 1) & EXPONENT_MASK) << EXPONENT_SHIFT;
        }
        ((p & EXPONENT_MASK) << EXPONENT_SHIFT) | (i as u16 & INDEX_MASK)
    }

    #[test]
    fn decode_matches_java_float_formula_exhaustively() {
        const INV_BPO: f64 = 1.0 / 2048.0;
        for encoded in 0..=MAX_ENC_VALUE {
            let p = u32::from((encoded >> EXPONENT_SHIFT) & EXPONENT_MASK);
            let i = u32::from(encoded & INDEX_MASK);
            let expected = ((1_u32 << p) as f64 * (f64::from(i) * INV_BPO + 1.0)) as usize;
            assert_eq!(
                decode_value(encoded).unwrap(),
                expected,
                "encoded={encoded:#06x}"
            );
        }
    }

    #[test]
    fn decode_rejects_values_above_max_enc_value() {
        for encoded in [MAX_ENC_VALUE + 1, 0xF800, u16::MAX] {
            assert!(decode_value(encoded).is_err());
        }
    }

    #[test]
    fn compute_and_decode_round_trip_powers_and_non_powers() {
        for k in [1, 2, 10, 128, 1000, 4096, 32768] {
            let enc = compute_size_for_test(k);
            assert_eq!(decode_value(enc).unwrap(), k);
        }
        for k in [5000, 32767, 95_000, MAX_ABS_VALUE] {
            let enc = compute_size_for_test(k);
            let dec = decode_value(enc).unwrap();
            assert!(dec >= k, "decoded {dec} < requested {k}");
        }
    }
}
