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

use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;
use googletest::assert_that;
use googletest::prelude::near;

const HLL_TYPES: [HllType; 3] = [HllType::Hll4, HllType::Hll6, HllType::Hll8];

fn make_sketch(hll_type: HllType, values: impl Iterator<Item = u64>) -> HllSketch {
    let mut sketch = HllSketch::new(11, hll_type).unwrap();
    for value in values {
        sketch.update(value);
    }
    sketch
}

#[test]
fn composite_estimate_matches_coupon_estimate_in_list_and_set_modes() {
    for hll_type in HLL_TYPES {
        for n in [0, 3, 100] {
            let sketch = make_sketch(hll_type, 0..n);
            assert_eq!(sketch.composite_estimate(), sketch.estimate());
        }
    }
}

#[test]
fn composite_estimate_is_independent_of_insertion_order() {
    for hll_type in HLL_TYPES {
        for n in [1_000, 10_000, 100_000] {
            let forward = make_sketch(hll_type, 0..n);
            let reverse = make_sketch(hll_type, (0..n).rev());
            assert_ne!(forward.estimate(), reverse.estimate());
            assert_that!(
                forward.composite_estimate(),
                near(reverse.composite_estimate(), 1e-9),
                "type={hll_type:?}, n={n}"
            );
        }
    }
}

#[test]
fn composite_estimate_matches_estimate_after_register_merge() {
    for hll_type in HLL_TYPES {
        for n in [1_000, 10_000, 100_000] {
            let sketch = make_sketch(hll_type, 0..n);
            let mut union = HllUnion::new(sketch.lg_config_k()).unwrap();
            union.update(&sketch);
            union.update(&sketch);
            assert_that!(sketch.composite_estimate(), near(union.estimate(), 1e-9));
            assert_eq!(union.composite_estimate(), union.estimate());
            for result_type in HLL_TYPES {
                assert_that!(
                    union.to_sketch(result_type).composite_estimate(),
                    near(union.estimate(), 1e-9)
                );
            }
        }
    }
}

#[test]
fn composite_estimate_preserves_hip_state_and_subsequent_updates() {
    for hll_type in HLL_TYPES {
        let mut sketch = make_sketch(hll_type, 0..10_000);
        let mut control = sketch.clone();
        let estimate = sketch.estimate();
        let bytes = sketch.serialize();
        assert_ne!(sketch.composite_estimate(), estimate);
        assert_eq!(sketch.estimate(), estimate);
        assert_eq!(sketch.serialize(), bytes);

        let decoded = HllSketch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.composite_estimate(), sketch.composite_estimate());
        assert_eq!(decoded.estimate(), estimate);

        for value in 10_000..20_000_u64 {
            sketch.update(value);
            control.update(value);
        }
        assert_eq!(sketch.serialize(), control.serialize());
    }
}

#[test]
fn union_composite_estimate_works_with_direct_updates_and_single_sketch() {
    for n in [0, 3, 100, 10_000] {
        let sketch = make_sketch(HllType::Hll8, 0..n);
        let mut direct = HllUnion::new(11).unwrap();
        for value in 0..n {
            direct.update_value(value);
        }
        let direct_estimate = direct.estimate();
        assert_eq!(direct.composite_estimate(), sketch.composite_estimate());
        assert_eq!(direct.estimate(), direct_estimate);

        for hll_type in HLL_TYPES {
            let sketch = make_sketch(hll_type, 0..n);
            let mut union = HllUnion::new(11).unwrap();
            union.update(&sketch);
            let hip_estimate = union.estimate();
            assert_eq!(hip_estimate, sketch.estimate());
            assert_that!(
                union.composite_estimate(),
                near(sketch.composite_estimate(), 1e-9)
            );
            assert_eq!(union.estimate(), hip_estimate);
            union.reset();
            assert_eq!(union.composite_estimate(), 0.0);
        }
    }
}
