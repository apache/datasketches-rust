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

use datasketches::common::NumStdDev;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;
use googletest::assert_that;
use googletest::prelude::near;

const NUM_STD_DEVS: [NumStdDev; 3] = [NumStdDev::One, NumStdDev::Two, NumStdDev::Three];

/// Builds a sketch in HLL mode from sequential values, either in order (HIP) or
/// out of order by unioning two halves of the same stream.
fn hll_mode_sketch(lg_k: u8, hll_type: HllType, n: u64, out_of_order: bool) -> HllSketch {
    if out_of_order {
        let mut even = HllSketch::new(lg_k, hll_type).unwrap();
        let mut odd = HllSketch::new(lg_k, hll_type).unwrap();
        for value in 0..n {
            if value % 2 == 0 {
                even.update(value);
            } else {
                odd.update(value);
            }
        }
        let mut union = HllUnion::new(lg_k).unwrap();
        union.update(&even);
        union.update(&odd);
        union.to_sketch(hll_type)
    } else {
        let mut sketch = HllSketch::new(lg_k, hll_type).unwrap();
        for value in 0..n {
            sketch.update(value);
        }
        sketch
    }
}

/// The lower bound of an HLL-mode sketch is clamped to the number of non-zero
/// registers, so it can never drop below the count of registers that have been hit.
///
/// The expected values are what the C++ implementation reports for these same
/// sketches. Without the clamp the wider intervals fall below the register count.
#[test]
fn matches_cross_language_lower_bound_reference_vectors() {
    #[allow(clippy::type_complexity)]
    let cases: [(u8, HllType, u64, bool, [f64; 3]); 12] = [
        (4, HllType::Hll4, 12, false, [9.946968965192236, 8.0, 8.0]),
        (4, HllType::Hll6, 12, false, [9.946968965192236, 8.0, 8.0]),
        (4, HllType::Hll8, 12, false, [9.946968965192236, 8.0, 8.0]),
        (
            7,
            HllType::Hll4,
            40,
            false,
            [35.559701910130165, 34.0, 34.0],
        ),
        (
            7,
            HllType::Hll6,
            40,
            false,
            [35.559701910130165, 34.0, 34.0],
        ),
        (
            7,
            HllType::Hll8,
            40,
            false,
            [35.559701910130165, 34.0, 34.0],
        ),
        (
            10,
            HllType::Hll4,
            300,
            false,
            [292.94619692474237, 285.3925905431874, 278.0973781689599],
        ),
        (
            10,
            HllType::Hll8,
            300,
            false,
            [292.94619692474237, 285.3925905431874, 278.0973781689599],
        ),
        (
            12,
            HllType::Hll4,
            1000,
            false,
            [983.4590264990582, 970.8060901625487, 958.4309756722355],
        ),
        (
            7,
            HllType::Hll4,
            40,
            true,
            [36.64056606724886, 34.00628712351393, 34.0],
        ),
        (
            10,
            HllType::Hll8,
            300,
            true,
            [289.0443708484499, 279.6835562340517, 270.6408842468243],
        ),
        (
            12,
            HllType::Hll4,
            1000,
            true,
            [975.4088934832242, 962.8595281321908, 950.5857105083956],
        ),
    ];

    for (lg_k, hll_type, n, out_of_order, expected) in cases {
        let sketch = hll_mode_sketch(lg_k, hll_type, n, out_of_order);
        for (index, num_std_dev) in NUM_STD_DEVS.into_iter().enumerate() {
            let actual = sketch.lower_bound(num_std_dev);
            assert_that!(
                actual,
                near(expected[index], 1e-9),
                "lg_k={lg_k} type={hll_type:?} n={n} out_of_order={out_of_order} \
                 num_std_dev={num_std_dev:?}"
            );
        }
    }
}
