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

// The UDD1 mapping is defined by these operations and round-to-nearest f64 arithmetic,
// not by a platform libm. All nonzero floating intermediates are normal.
// Keep its constants and rounding rules stable: they determine serialized bucket keys.
const LOG_UNIT: i64 = 1 << 52;
const MIN_LOG: i64 = -1074 * LOG_UNIT;
// floor(log2((16 + i) / 16) * 2^52), i = 0..=16.
const ANCHORS: [u64; 17] = [
    0,
    393_897_619_263_793,
    765_274_173_176_528,
    1_116_566_257_159_425,
    1_449_835_248_174_818,
    1_766_840_599_037_378,
    2_069_096_066_497_155,
    2_357_913_430_203_731,
    2_634_436_900_273_512,
    2_899_670_496_349_637,
    3_154_500_053_615_717,
    3_399_711_073_450_041,
    3_636_003_326_134_361,
    3_864_002_889_947_501,
    4_084_272_148_448_331,
    4_297_318_147_896_633,
    4_503_599_627_370_496,
];

#[derive(Clone, Copy, Debug)]
pub struct Mapping {
    step: u64,
    reciprocal: u64,
    offset: u64,
}

impl Mapping {
    pub fn new(accuracy: f64) -> Self {
        let step = log2_fixed(1.0 + accuracy) - log2_fixed(1.0 - accuracy);
        // ceil(L / step) - ceil(MIN_LOG / step) is floor((L + offset) / step).
        // MIN_LOG is negative; offset makes the numerator nonnegative over the f64 domain.
        // For supported accuracies, 0 < step <= 54 * LOG_UNIT, so the numerator is below
        // (1074 + 1024 + 54) * LOG_UNIT < 2^64 even though it can exceed i64::MAX.
        let step = step as u64;
        Self {
            step,
            reciprocal: u64::MAX / step,
            offset: MIN_LOG.unsigned_abs() + step - 1 - MIN_LOG.unsigned_abs() % step,
        }
    }

    pub fn index(self, magnitude: f64) -> u64 {
        // The mathematical sum is in [0, 2^64); wrapping addition represents it even when L < 0.
        self.divide((log2_fixed(magnitude) as u64).wrapping_add(self.offset))
    }

    fn divide(self, numerator: u64) -> u64 {
        // reciprocal / 2^64 underestimates 1 / step by at most 2^-64.
        // For a u64 numerator, the resulting quotient is exact or one too small.
        // Its product with step cannot exceed numerator; one remainder check gives exact division.
        let quotient = ((u128::from(numerator) * u128::from(self.reciprocal)) >> 64) as u64;
        quotient + u64::from(numerator - quotient * self.step >= self.step)
    }

    pub fn max_level(self) -> u32 {
        64 - self.index(f64::MAX).leading_zeros()
    }
}

// Approximates log2(x) in signed Q52 for every positive finite f64, including subnormals.
// Normalize exactly to x = 2^exponent * significand / 2^52. Within each interval,
// z = (m - anchor) / (m + anchor) is in [0, 1/33]. The identity
// log2(m / anchor) = 2/ln(2) * (z + z^3/3 + z^5/5 + ...) gives a positive polynomial.
// Five terms leave a truncation error below 5.3e-18 before arithmetic rounding.
//
// Monotonicity also holds after rounding: the numerator conversion is exact; increasing
// significand by one increases the rounded denominator by at most two, and the denominator
// exceeds twice the numerator. Thus even the rounded-operand ratio is nondecreasing.
// Correctly rounded division, positive polynomial operations, and integer truncation preserve
// that order. Capping at the next anchor joins intervals monotonically, including powers of two.
// Integer exponent assembly avoids cancellation near one. No transcendental calls are used.
fn log2_fixed(value: f64) -> i64 {
    let bits = value.to_bits();
    let raw_exponent = (bits >> 52) as i64;
    let (exponent, significand) = if raw_exponent == 0 {
        let top = 63 - bits.leading_zeros();
        (i64::from(top) - 1074, bits << (52 - top))
    } else {
        (raw_exponent - 1023, (bits & ((1 << 52) - 1)) | (1 << 52))
    };
    // Normalization puts significand in [2^52, 2^53); masking also makes the table bounds
    // explicit to the compiler without changing the segment selected by UDD1.
    let segment = ((significand >> 48) & 15) as usize;
    // Scaling the original integer numerator and denominator by 2^-52 commutes with
    // round-to-nearest: all nonzero operands/results are normal, and subtraction is exact.
    // This produces the identical z bits while avoiding integer-to-float conversions.
    let normalized = f64::from_bits((1023u64 << 52) | (significand & ((1 << 52) - 1)));
    let anchor = f64::from_bits((1023u64 << 52) | ((segment as u64) << 48));
    let z = (normalized - anchor) / (normalized + anchor);
    let square = z * z;
    let mut polynomial = 1.0 / 9.0;
    for coefficient in [1.0 / 7.0, 1.0 / 5.0, 1.0 / 3.0, 1.0] {
        polynomial = coefficient + square * polynomial;
    }
    let offset = (z * polynomial * (2.0 * std::f64::consts::LOG2_E * LOG_UNIT as f64)) as u64;
    let fraction = (ANCHORS[segment] + offset).min(ANCHORS[segment + 1]);
    exponent * LOG_UNIT + fraction as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotone_at_segment_and_exponent_boundaries() {
        let mut boundaries = vec![1, (1 << 52) - 1, 1 << 52, f64::MAX.to_bits()];
        for exponent in 1..2047u64 {
            for segment in 0..16 {
                boundaries.push((exponent << 52) | (segment << 48));
            }
        }
        for top in 0..52 {
            boundaries.push(1 << top);
        }
        for bits in boundaries {
            let lo = bits.saturating_sub(8).max(1);
            let hi = (bits + 8).min(f64::MAX.to_bits());
            for bits in lo..hi {
                assert!(log2_fixed(f64::from_bits(bits)) <= log2_fixed(f64::from_bits(bits + 1)));
            }
        }
    }

    #[test]
    fn monotone_interior_and_mapping_range() {
        let mut seed = 42u64;
        let mappings =
            [1e-9, 0.001, 0.01, 0.5, f64::from_bits(1.0f64.to_bits() - 1)].map(Mapping::new);
        for mapping in mappings {
            assert_eq!(mapping.index(f64::from_bits(1)), 0);
            assert!(mapping.max_level() < 64);
        }
        for _ in 0..100_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let bits = 1 + seed % (f64::MAX.to_bits() - 1);
            let x = f64::from_bits(bits);
            let next = f64::from_bits(bits + 1);
            assert!(log2_fixed(x) <= log2_fixed(next));
            for mapping in mappings {
                assert!(mapping.index(x) <= mapping.index(next));
                assert!(mapping.index(x) <= mapping.index(f64::MAX));
            }
        }
    }

    #[test]
    fn reciprocal_division_is_exact_at_integer_boundaries() {
        let mut seed = 42u64;
        let configured_steps = [1e-9, 0.001, 0.01, 0.5, f64::from_bits(1.0f64.to_bits() - 1)]
            .map(|accuracy| Mapping::new(accuracy).step);
        let mut steps = (1..=255).collect::<Vec<u64>>();
        for exponent in 1..64 {
            let power = 1u64 << exponent;
            steps.extend([power - 1, power, power + 1]);
        }
        steps.extend(configured_steps);
        steps.push(u64::MAX);
        steps.sort_unstable();
        steps.dedup();
        for step in steps {
            let mapping = Mapping {
                step,
                reciprocal: u64::MAX / step,
                offset: 0,
            };
            // Small numerators exercise every quotient and remainder in a bounded grid.
            if step <= 255 {
                for numerator in 0..4096 {
                    assert_eq!(mapping.divide(numerator), numerator / step);
                }
            }
            let last_multiple = (u64::MAX / step) * step;
            for numerator in [
                0,
                1,
                last_multiple.saturating_sub(1),
                last_multiple,
                last_multiple.saturating_add(1),
                u64::MAX - 1,
                mapping.step - 1,
                mapping.step,
                mapping.step.saturating_add(1),
                u64::MAX,
            ] {
                assert_eq!(mapping.divide(numerator), numerator / mapping.step);
            }
            for _ in 0..256 {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                assert_eq!(mapping.divide(seed), seed / mapping.step);
                let multiple = (seed / mapping.step) * mapping.step;
                for numerator in [
                    multiple.saturating_sub(1),
                    multiple,
                    multiple.saturating_add(1),
                ] {
                    assert_eq!(mapping.divide(numerator), numerator / mapping.step);
                }
            }
        }
    }

    #[test]
    fn translated_indices_match_signed_ceiling() {
        let mut seed = 42u64;
        let mut values = vec![
            f64::from_bits(1),
            f64::MIN_POSITIVE,
            0.5,
            1.0,
            2.0,
            f64::MAX,
        ];
        for _ in 0..4096 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            values.push(f64::from_bits(1 + seed % f64::MAX.to_bits()));
        }
        let ceil = |value: i128, step: i128| -(-value).div_euclid(step);
        for accuracy in [1e-9, 0.001, 0.01, 0.5, f64::from_bits(1.0f64.to_bits() - 1)] {
            let mapping = Mapping::new(accuracy);
            for &value in &values {
                // Signed wide arithmetic is independent of the optimized unsigned translation.
                let coordinate = i128::from(log2_fixed(value));
                let numerator = coordinate + i128::from(mapping.offset);
                assert!((0..=i128::from(u64::MAX)).contains(&numerator));
                let step = i128::from(mapping.step);
                let expected = ceil(coordinate, step) - ceil(i128::from(MIN_LOG), step);
                assert_eq!(i128::from(mapping.index(value)), expected);
            }
        }
    }

    #[test]
    fn scaled_logarithm_matches_udd1_integer_operands() {
        // This reference retains the unscaled UDD1 operands and rounding sequence.
        // Comparing Q52 coordinates also protects bucket boundaries for every accuracy.
        let reference = |significand: u64| {
            let segment = ((significand - (1 << 52)) >> 48) as usize;
            let anchor = (16 + segment as u64) << 48;
            let z = (significand - anchor) as f64 / (significand + anchor) as f64;
            let square = z * z;
            let polynomial = 1.0
                + square
                    * (1.0 / 3.0
                        + square * (1.0 / 5.0 + square * (1.0 / 7.0 + square * (1.0 / 9.0))));
            let offset =
                (z * polynomial * (2.0 * std::f64::consts::LOG2_E * LOG_UNIT as f64)) as u64;
            (ANCHORS[segment] + offset).min(ANCHORS[segment + 1]) as i64
        };
        let check = |significand: u64| {
            let value = f64::from_bits((1023 << 52) | (significand - (1 << 52)));
            assert_eq!(
                log2_fixed(value),
                reference(significand),
                "{significand:#x}"
            );
        };
        for segment in 0..16 {
            let first = (1 << 52) + (segment << 48);
            let last = first + (1 << 48) - 1;
            // Consecutive significands cover both directions of ties-to-even denominator rounding.
            for delta in 0..4096 {
                check(first + delta);
                check(last - delta);
            }
        }
        let mut seed = 42u64;
        for _ in 0..100_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            check((1 << 52) | (seed & ((1 << 52) - 1)));
        }
        for top in 0..=52 {
            for bits in [
                (1u64 << top).saturating_sub(1).max(1),
                1 << top,
                (1 << top) + 1,
            ] {
                let top = 63 - bits.leading_zeros();
                let expected = (i64::from(top) - 1074) * LOG_UNIT + reference(bits << (52 - top));
                assert_eq!(log2_fixed(f64::from_bits(bits)), expected);
            }
        }
    }

    #[test]
    fn udd1_bucket_boundary_fixtures() {
        // Frozen from the original UDD1 mapping before reciprocal division or operand scaling.
        // Check adjacent f64s at bucket transitions, where even a one-bit change matters.
        let fixtures = [
            (1e-9, 0x000fffffff9d4163u64, 18_021_826_274u64),
            (1e-9, 0x3fb99999994f48bdu64, 371_068_734_765u64),
            (1e-9, 0x3feffffffeed1f43u64, 372_220_027_284u64),
            (1e-9, 0x3ff3fffffff649e8u64, 372_331_599_058u64),
            (1e-9, 0x7fefffffff43a5e6u64, 727_111_375_459u64),
            (0.001, 0x000ff88fd6b7d652u64, 18_021u64),
            (0.001, 0x3fefefa1e2d85c14u64, 372_219u64),
            (0.001, 0x54b247a6d904b1c7u64, 487_349u64),
            (0.001, 0x7feffc195daab813u64, 727_111u64),
            (0.01, 0x000fe2a1ff8747cdu64, 1_802u64),
            (0.01, 0x3fef5dc83cd4e932u64, 37_220u64),
            (0.01, 0x3ff3eff7bc7a494du64, 37_232u64),
            (0.01, 0x7fef65643b949679u64, 72_708u64),
            (0.5, 0x000cfd1c8ffd9c2cu64, 33u64),
            (0.5, 0x3fd5555555555557u64, 677u64),
            (0.5, 0x3ff0000000000001u64, 678u64),
            (0.5, 0x7fed906a378b57ffu64, 1_324u64),
            (0.9999999999999999, 0x0001000000000001u64, 1u64),
            (0.9999999999999999, 0x3ff0000000000001u64, 20u64),
            (0.9999999999999999, 0x7cb0000000000001u64, 38u64),
        ];
        for (accuracy, bits, index) in fixtures {
            let mapping = Mapping::new(accuracy);
            assert_eq!(mapping.index(f64::from_bits(bits - 1)), index - 1);
            assert_eq!(mapping.index(f64::from_bits(bits)), index);
            assert_eq!(mapping.index(f64::from_bits(bits + 1)), index);
        }
    }

    #[test]
    fn powers_of_two_are_exact() {
        for exponent in -1074..=1023 {
            let bits = if exponent < -1022 {
                1 << (exponent + 1074)
            } else {
                ((exponent + 1023) as u64) << 52
            };
            assert_eq!(log2_fixed(f64::from_bits(bits)), exponent * LOG_UNIT);
        }
    }
}
