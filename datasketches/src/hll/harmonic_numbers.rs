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

//! Harmonic-number approximation used by the bitmap estimator, following DataSketches C++:
//! <https://github.com/apache/datasketches-cpp/blob/5a055521/hll/include/HarmonicNumbers-internal.hpp>

const NUM_EXACT: usize = 25;
const EULER_MASCHERONI: f64 = 0.577_215_664_901_532_9;

/// Exact harmonic numbers H(n) for n = 0..24
static EXACT_HARMONIC: [f64; NUM_EXACT] = [
    0.0,                        // H(0)
    1.0,                        // H(1)
    1.5,                        // H(2)
    11.0 / 6.0,                 // H(3)
    25.0 / 12.0,                // H(4)
    137.0 / 60.0,               // H(5)
    49.0 / 20.0,                // H(6)
    363.0 / 140.0,              // H(7)
    761.0 / 280.0,              // H(8)
    7129.0 / 2520.0,            // H(9)
    7381.0 / 2520.0,            // H(10)
    83711.0 / 27720.0,          // H(11)
    86021.0 / 27720.0,          // H(12)
    1145993.0 / 360360.0,       // H(13)
    1171733.0 / 360360.0,       // H(14)
    1195757.0 / 360360.0,       // H(15)
    2436559.0 / 720720.0,       // H(16)
    42142223.0 / 12252240.0,    // H(17)
    14274301.0 / 4084080.0,     // H(18)
    275295799.0 / 77597520.0,   // H(19)
    55835135.0 / 15519504.0,    // H(20)
    18858053.0 / 5173168.0,     // H(21)
    19093197.0 / 5173168.0,     // H(22)
    444316699.0 / 118982864.0,  // H(23)
    1347822955.0 / 356948592.0, // H(24)
];

/// Computes `H(n) = 1 + 1/2 + ... + 1/n`, with `H(0) = 0`.
fn harmonic_number(n: usize) -> f64 {
    if n < NUM_EXACT {
        return EXACT_HARMONIC[n];
    }

    let x = n as f64;
    let inv_sq = 1.0 / (x * x);
    let mut sum = x.ln() + EULER_MASCHERONI + (1.0 / (2.0 * x));

    // The cutoff at 25 and expansion through n^-8 are chosen together for f64 precision.
    let mut pow = inv_sq; // n^-2
    sum -= pow * (1.0 / 12.0);

    pow *= inv_sq; // n^-4
    sum += pow * (1.0 / 120.0);

    pow *= inv_sq; // n^-6
    sum -= pow * (1.0 / 252.0);

    pow *= inv_sq; // n^-8
    sum += pow * (1.0 / 240.0);

    sum
}

/// Expected draws needed to hit `num_bits_set` distinct slots under the coupon collector model.
/// Requires `num_bits_set <= bit_vector_length`.
pub fn bitmap_estimate(bit_vector_length: u32, num_bits_set: u32) -> f64 {
    let k = bit_vector_length;
    let num_set = num_bits_set;

    let h_k = harmonic_number(k as usize);
    let h_diff = harmonic_number((k - num_set) as usize);

    (k as f64) * (h_k - h_diff)
}

#[cfg(test)]
mod tests {
    use googletest::assert_that;
    use googletest::prelude::gt;
    use googletest::prelude::lt;
    use googletest::prelude::near;

    use super::*;

    #[test]
    fn test_exact_harmonic_numbers() {
        assert_that!(harmonic_number(1), near(1.0, 1e-10));

        assert_that!(harmonic_number(2), near(1.5, 1e-10));

        assert_that!(harmonic_number(3), near(11.0 / 6.0, 1e-10));

        let expected = 1.0
            + 1.0 / 2.0
            + 1.0 / 3.0
            + 1.0 / 4.0
            + 1.0 / 5.0
            + 1.0 / 6.0
            + 1.0 / 7.0
            + 1.0 / 8.0
            + 1.0 / 9.0
            + 1.0 / 10.0;
        assert_that!(harmonic_number(10), near(expected, 1e-10));
    }

    #[test]
    fn test_asymptotic_harmonic() {
        // For large n, H(n) ≈ ln(n) + γ + 1/(2n)
        let n = 1000usize;
        let h_n = harmonic_number(n);
        let approx = (n as f64).ln() + EULER_MASCHERONI + 1.0 / (2.0 * n as f64);

        assert_that!(h_n, near(approx, h_n * 0.001));
    }

    #[test]
    fn test_bitmap_estimate_empty() {
        let est = bitmap_estimate(1024, 0);
        assert_that!(est, near(0.0, 1e-6));
    }

    #[test]
    fn test_bitmap_estimate_full() {
        let k = 1024;
        let est = bitmap_estimate(k, k);

        // With all slots hit, estimate should be >> k
        assert_that!(est, gt(k as f64));

        // H(k) - H(0) = H(k), so estimate = k * H(k)
        let expected = k as f64 * harmonic_number(k as usize);
        assert_that!(est, near(expected, 1e-6));
    }

    #[test]
    fn test_bitmap_estimate_half() {
        let k = 1024;
        let est = bitmap_estimate(k, k / 2);

        assert_that!(est, gt(0.0));
        assert_that!(est, lt(k as f64 * harmonic_number(k as usize)));
    }
}
