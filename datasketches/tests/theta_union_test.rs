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

#![cfg(feature = "theta")]

use datasketches::theta::CompactThetaSketch;
use datasketches::theta::ThetaSketch;
use datasketches::theta::ThetaUnion;

fn sketch_with_range(lg_k: u8, start: i64, count: i64) -> ThetaSketch {
    let mut sketch = ThetaSketch::builder().lg_k(lg_k).build();
    for value in start..start + count {
        sketch.update(value);
    }
    sketch
}

fn assert_estimate_close(sketch: &CompactThetaSketch, expected: f64, tolerance: f64) {
    assert!(
        (sketch.estimate() - expected).abs() <= tolerance,
        "estimate={}, expected={}, tolerance={}, theta={}, retained={}",
        sketch.estimate(),
        expected,
        tolerance,
        sketch.theta(),
        sketch.num_retained()
    );
}

#[test]
fn test_empty_union() {
    let sketch = ThetaSketch::builder().build();
    let mut union = ThetaUnion::builder().build();
    let result = union.result();
    assert_eq!(result.num_retained(), 0);
    assert!(result.is_empty());
    assert!(!result.is_estimation_mode());

    union.update(&sketch).unwrap();
    let result = union.result();
    assert_eq!(result.num_retained(), 0);
    assert!(result.is_empty());
    assert!(!result.is_estimation_mode());
}

#[test]
fn test_non_empty_no_retained_keys() {
    let mut sketch = ThetaSketch::builder().sampling_probability(0.001).build();
    sketch.update(1u64);

    let mut union = ThetaUnion::builder().build();
    union.update(&sketch).unwrap();
    let result = union.result();
    assert_eq!(result.num_retained(), 0);
    assert!(!result.is_empty());
    assert!(result.is_estimation_mode());
    assert!((result.theta() - 0.001).abs() < 1e-10);
}

#[test]
fn test_exact_mode_half_overlap() {
    let mut sketch1 = ThetaSketch::builder().build();
    for value in 0i64..1000i64 {
        sketch1.update(value);
    }

    let mut sketch2 = ThetaSketch::builder().build();
    for value in 500i64..1500i64 {
        sketch2.update(value);
    }

    let mut union = ThetaUnion::builder().build();
    union.update(&sketch1).unwrap();
    union.update(&sketch2).unwrap();
    let result = union.result();
    assert!(!result.is_empty());
    assert!(!result.is_estimation_mode());
    assert_eq!(result.estimate(), 1500.0);

    union.reset();
    let reset = union.result();
    assert_eq!(reset.num_retained(), 0);
    assert!(reset.is_empty());
    assert!(!reset.is_estimation_mode());
}

#[test]
fn test_exact_mode_half_overlap_compact() {
    let mut sketch1 = ThetaSketch::builder().build();
    for value in 0i64..1000i64 {
        sketch1.update(value);
    }
    let compact1 = CompactThetaSketch::deserialize(&sketch1.compact(true).serialize()).unwrap();

    let mut sketch2 = ThetaSketch::builder().build();
    for value in 500i64..1500i64 {
        sketch2.update(value);
    }
    let compact2 = CompactThetaSketch::deserialize(&sketch2.compact(true).serialize()).unwrap();

    let mut union = ThetaUnion::builder().build();
    union.update(&compact1).unwrap();
    union.update(&compact2).unwrap();
    let result = union.result();
    assert!(!result.is_empty());
    assert!(!result.is_estimation_mode());
    assert_eq!(result.estimate(), 1500.0);
}

#[test]
fn test_estimation_mode_half_overlap() {
    let mut sketch1 = ThetaSketch::builder().build();
    for value in 0i64..10000i64 {
        sketch1.update(value);
    }

    let mut sketch2 = ThetaSketch::builder().build();
    for value in 5000i64..15000i64 {
        sketch2.update(value);
    }

    let mut union = ThetaUnion::builder().build();
    union.update(&sketch1).unwrap();
    union.update(&sketch2).unwrap();
    let result = union.result();
    assert!(!result.is_empty());
    assert!(result.is_estimation_mode());
    assert!(
        (result.estimate() - 15000.0).abs() <= 15000.0 * 0.01,
        "estimate={}, theta={}, retained={}",
        result.estimate(),
        result.theta(),
        result.num_retained()
    );
}

#[test]
fn test_seed_mismatch() {
    let mut sketch = ThetaSketch::builder().build();
    sketch.update(1u64);

    let mut union = ThetaUnion::builder().seed(123).build();
    assert!(union.update(&sketch).is_err());
}

#[test]
fn test_larger_k() {
    let mut sketch1 = ThetaSketch::builder().lg_k(14).build();
    for value in 0i64..16384i64 {
        sketch1.update(value);
    }

    let mut sketch2 = ThetaSketch::builder().lg_k(14).build();
    for value in 0i64..26384i64 {
        sketch2.update(value);
    }

    let mut sketch3 = ThetaSketch::builder().lg_k(14).build();
    for value in 0i64..86384i64 {
        sketch3.update(value);
    }

    let mut union1 = ThetaUnion::builder().lg_k(16).build();
    union1.update(&sketch2).unwrap();
    union1.update(&sketch1).unwrap();
    union1.update(&sketch3).unwrap();
    let result1 = union1.result();
    assert_eq!(result1.estimate(), sketch3.estimate());

    let mut union2 = ThetaUnion::builder().lg_k(16).build();
    union2.update(&sketch1).unwrap();
    union2.update(&sketch3).unwrap();
    union2.update(&sketch2).unwrap();
    let result2 = union2.result();
    assert_eq!(result2.estimate(), sketch3.estimate());
}

#[test]
fn test_exact_union_no_overlap() {
    let lg_k = 9;
    let k = 1i64 << lg_k;
    let sketch1 = sketch_with_range(lg_k, 0, k / 2);
    let sketch2 = sketch_with_range(lg_k, k / 2, k / 2);

    let mut union = ThetaUnion::builder().lg_k(lg_k).build();
    union.update(&sketch1).unwrap();
    union.update(&sketch2).unwrap();

    let result = union.result();
    assert!(!result.is_empty());
    assert!(!result.is_estimation_mode());
    assert_eq!(result.estimate(), k as f64);
}

#[test]
fn test_estimation_union_no_overlap() {
    let lg_k = 12;
    let k = 1i64 << lg_k;
    let sketch1 = sketch_with_range(lg_k, 0, 2 * k);
    let sketch2 = sketch_with_range(lg_k, 2 * k, 2 * k);

    let mut union = ThetaUnion::builder().lg_k(lg_k).build();
    union.update(&sketch1).unwrap();
    union.update(&sketch2).unwrap();

    assert_estimate_close(&union.result(), (4 * k) as f64, 0.05 * (4 * k) as f64);
}

#[test]
fn test_exact_union_with_overlap() {
    let lg_k = 9;
    let k = 1i64 << lg_k;
    let sketch1 = sketch_with_range(lg_k, 0, k / 2);
    let sketch2 = sketch_with_range(lg_k, 0, k);

    let mut union = ThetaUnion::builder().lg_k(lg_k).build();
    union.update(&sketch1).unwrap();
    union.update(&sketch2).unwrap();

    let result = union.result();
    assert!(!result.is_empty());
    assert!(!result.is_estimation_mode());
    assert_eq!(result.estimate(), k as f64);
}

#[test]
fn test_ordered_and_unordered_compact_inputs() {
    let lg_k = 12;
    let k = 1i64 << lg_k;
    let sketch1 = sketch_with_range(lg_k, 0, 2 * k);
    let sketch2 = sketch_with_range(lg_k + 1, 2 * k, 2 * k);
    let compact_ordered = sketch2.compact(true);
    let compact_unordered = sketch2.compact(false);

    let mut ordered_union = ThetaUnion::builder().lg_k(lg_k).build();
    ordered_union.update(&sketch1).unwrap();
    ordered_union.update(&compact_ordered).unwrap();

    let mut unordered_union = ThetaUnion::builder().lg_k(lg_k).build();
    unordered_union.update(&sketch1).unwrap();
    unordered_union.update(&compact_unordered).unwrap();

    assert_eq!(
        ordered_union.result().estimate(),
        unordered_union.result().estimate()
    );
    assert_estimate_close(
        &ordered_union.result(),
        (4 * k) as f64,
        0.05 * (4 * k) as f64,
    );
}

#[test]
fn test_result_ordering_forms_have_same_estimate() {
    let sketch1 = sketch_with_range(12, 0, 8192);
    let sketch2 = sketch_with_range(12, 8192, 1024);

    let mut union = ThetaUnion::builder().lg_k(12).build();
    union.update(&sketch1).unwrap();
    union.update(&sketch2).unwrap();

    let unordered = union.result_with_ordered(false);
    let ordered = union.result_with_ordered(true);

    assert!(!unordered.is_ordered());
    assert!(ordered.is_ordered());
    assert_eq!(unordered.estimate(), ordered.estimate());
}

#[test]
fn test_multi_union() {
    let lg_k = 13;
    let ranges = [
        (0, 100_000),
        (100_000, 26_797),
        (126_797, 26_797),
        (153_594, 26_797),
    ];
    let mut union = ThetaUnion::builder().lg_k(lg_k).build();

    for (start, count) in ranges {
        let sketch = sketch_with_range(lg_k, start, count);
        union.update(&sketch).unwrap();
    }

    assert_estimate_close(&union.result(), 180_391.0, 180_391.0 * 0.02);
}

#[test]
fn test_result_does_not_reset_union() {
    let lg_k = 9;
    let k = 1i64 << lg_k;
    let compact1 = sketch_with_range(lg_k, 0, k).compact(true);
    let compact2 = sketch_with_range(lg_k, k, k).compact(true);

    let mut union = ThetaUnion::builder().lg_k(lg_k).build();
    union.update(&compact1).unwrap();
    union.update(&compact2).unwrap();
    let first = union.result();
    let second = union.result();

    assert_eq!(first.estimate(), second.estimate());
    assert!(!second.is_empty());
}

#[test]
fn test_union_full_overlap() {
    let lg_k = 9;
    let k = 1i64 << lg_k;
    let compact1 = sketch_with_range(lg_k, 0, k).compact(true);
    let compact2 = sketch_with_range(lg_k, 0, k).compact(true);

    let mut union = ThetaUnion::builder().lg_k(lg_k).build();
    union.update(&compact1).unwrap();
    union.update(&compact2).unwrap();
    let result = union.result();

    assert_eq!(result.estimate(), k as f64);
}

#[test]
fn test_ordered_input_early_stop_matches_unordered_input() {
    let lg_k = 10;
    let k = 1i64 << lg_k;
    let mut value = 0i64;

    for _ in 0..10 {
        let sketch1 = sketch_with_range(lg_k, value, 4 * k);
        let sketch2 = sketch_with_range(lg_k, value + 2 * k, 4 * k);
        value += 6 * k;
        let ordered1 = sketch1.compact(true);
        let ordered2 = sketch2.compact(true);
        let unordered1 = sketch1.compact(false);
        let unordered2 = sketch2.compact(false);

        let mut ordered_union = ThetaUnion::builder().lg_k(lg_k + 1).build();
        ordered_union.update(&ordered1).unwrap();
        ordered_union.update(&ordered2).unwrap();

        let mut unordered_union = ThetaUnion::builder().lg_k(lg_k + 1).build();
        unordered_union.update(&unordered1).unwrap();
        unordered_union.update(&unordered2).unwrap();

        assert_eq!(
            ordered_union.result().estimate(),
            unordered_union.result().estimate()
        );
    }
}

#[test]
fn test_union_cutback_to_k() {
    let lg_k = 10;
    let k = 1i64 << lg_k;
    let compact1 = sketch_with_range(lg_k, 0, 3 * k).compact(true);
    let compact2 = sketch_with_range(lg_k, 6 * k, 3 * k).compact(true);

    let mut union = ThetaUnion::builder().lg_k(lg_k).build();
    union.update(&compact1).unwrap();
    union.update(&compact2).unwrap();
    let result = union.result();

    assert_estimate_close(&result, (6 * k) as f64, (6 * k) as f64 * 0.06);
    assert!(result.num_retained() <= k as usize);
}

#[test]
fn test_union_empty_valid_rules() {
    let empty1 = ThetaSketch::builder().build().compact(true);
    let empty2 = ThetaSketch::builder().build().compact(true);
    let mut one = ThetaSketch::builder().build();
    one.update(1i64);
    let one = one.compact(true);

    let mut empty_union = ThetaUnion::builder().lg_k(5).build();
    empty_union.update(&empty1).unwrap();
    empty_union.update(&empty2).unwrap();
    assert!(empty_union.result().is_empty());

    let mut left_non_empty_union = ThetaUnion::builder().lg_k(5).build();
    left_non_empty_union.update(&one).unwrap();
    left_non_empty_union.update(&empty2).unwrap();
    assert!(!left_non_empty_union.result().is_empty());

    let mut right_non_empty_union = ThetaUnion::builder().lg_k(5).build();
    right_non_empty_union.update(&empty1).unwrap();
    right_non_empty_union.update(&one).unwrap();
    assert!(!right_non_empty_union.result().is_empty());
}

#[test]
fn test_trim_to_k() {
    let hi_sketch = sketch_with_range(10, 0, 3749);
    let lo_sketch = sketch_with_range(9, 10_000, 1783);

    let mut union = ThetaUnion::builder().lg_k(10).build();
    union.update(&hi_sketch).unwrap();
    union.update(&lo_sketch).unwrap();
    let result = union.result();

    assert_eq!(result.num_retained(), 1024);
}

#[test]
fn test_builder_lg_k() {
    let sketch = sketch_with_range(10, 0, 1000);
    let mut union = ThetaUnion::builder().lg_k(10).build();
    union.update(&sketch).unwrap();

    assert_eq!(union.result().estimate(), 1000.0);
}

#[derive(Clone, Copy, Debug)]
enum CornerSketchState {
    Empty,
    Exact,
    Estimation,
    Degenerate,
}

fn corner_sketch(state: CornerSketchState, p: f32, value: i64) -> ThetaSketch {
    let builder = ThetaSketch::builder().lg_k(5);
    let mut sketch = match state {
        CornerSketchState::Empty | CornerSketchState::Exact => builder.build(),
        CornerSketchState::Estimation | CornerSketchState::Degenerate => {
            builder.sampling_probability(p).build()
        }
    };
    if !matches!(state, CornerSketchState::Empty) {
        sketch.update(value);
    }
    sketch
}

#[test]
fn test_corner_case_union_states() {
    const GT_MIDP_VALUE: i64 = 3;
    const MIDP: f32 = 0.5;
    const GT_LOWP_VALUE: i64 = 6;
    const LOWP: f32 = 0.1;
    const LT_LOWP_VALUE: i64 = 4;

    let cases = [
        (
            CornerSketchState::Empty,
            1.0,
            0,
            CornerSketchState::Empty,
            1.0,
            0,
            1.0,
            0,
            true,
        ),
        (
            CornerSketchState::Empty,
            1.0,
            0,
            CornerSketchState::Exact,
            1.0,
            GT_MIDP_VALUE,
            1.0,
            1,
            false,
        ),
        (
            CornerSketchState::Empty,
            1.0,
            0,
            CornerSketchState::Degenerate,
            LOWP,
            GT_LOWP_VALUE,
            LOWP as f64,
            0,
            false,
        ),
        (
            CornerSketchState::Empty,
            1.0,
            0,
            CornerSketchState::Estimation,
            LOWP,
            LT_LOWP_VALUE,
            LOWP as f64,
            1,
            false,
        ),
        (
            CornerSketchState::Exact,
            1.0,
            GT_MIDP_VALUE,
            CornerSketchState::Empty,
            1.0,
            0,
            1.0,
            1,
            false,
        ),
        (
            CornerSketchState::Exact,
            1.0,
            GT_MIDP_VALUE,
            CornerSketchState::Exact,
            1.0,
            GT_MIDP_VALUE,
            1.0,
            1,
            false,
        ),
        (
            CornerSketchState::Exact,
            1.0,
            LT_LOWP_VALUE,
            CornerSketchState::Degenerate,
            LOWP,
            GT_LOWP_VALUE,
            LOWP as f64,
            1,
            false,
        ),
        (
            CornerSketchState::Exact,
            1.0,
            LT_LOWP_VALUE,
            CornerSketchState::Estimation,
            LOWP,
            LT_LOWP_VALUE,
            LOWP as f64,
            1,
            false,
        ),
        (
            CornerSketchState::Estimation,
            LOWP,
            LT_LOWP_VALUE,
            CornerSketchState::Empty,
            1.0,
            0,
            LOWP as f64,
            1,
            false,
        ),
        (
            CornerSketchState::Estimation,
            LOWP,
            LT_LOWP_VALUE,
            CornerSketchState::Exact,
            1.0,
            LT_LOWP_VALUE,
            LOWP as f64,
            1,
            false,
        ),
        (
            CornerSketchState::Estimation,
            MIDP,
            LT_LOWP_VALUE,
            CornerSketchState::Degenerate,
            LOWP,
            GT_LOWP_VALUE,
            LOWP as f64,
            1,
            false,
        ),
        (
            CornerSketchState::Estimation,
            MIDP,
            LT_LOWP_VALUE,
            CornerSketchState::Estimation,
            LOWP,
            LT_LOWP_VALUE,
            LOWP as f64,
            1,
            false,
        ),
        (
            CornerSketchState::Degenerate,
            LOWP,
            GT_LOWP_VALUE,
            CornerSketchState::Empty,
            1.0,
            0,
            LOWP as f64,
            0,
            false,
        ),
        (
            CornerSketchState::Degenerate,
            LOWP,
            GT_LOWP_VALUE,
            CornerSketchState::Exact,
            1.0,
            LT_LOWP_VALUE,
            LOWP as f64,
            1,
            false,
        ),
        (
            CornerSketchState::Degenerate,
            MIDP,
            GT_MIDP_VALUE,
            CornerSketchState::Degenerate,
            LOWP,
            GT_LOWP_VALUE,
            LOWP as f64,
            0,
            false,
        ),
        (
            CornerSketchState::Degenerate,
            MIDP,
            GT_MIDP_VALUE,
            CornerSketchState::Estimation,
            LOWP,
            LT_LOWP_VALUE,
            LOWP as f64,
            1,
            false,
        ),
    ];

    for (
        state_a,
        p_a,
        value_a,
        state_b,
        p_b,
        value_b,
        expected_theta,
        expected_count,
        expected_empty,
    ) in cases
    {
        let sketch_a = corner_sketch(state_a, p_a, value_a);
        let sketch_b = corner_sketch(state_b, p_b, value_b);

        let mut union = ThetaUnion::builder().build();
        union.update(&sketch_a).unwrap();
        union.update(&sketch_b).unwrap();
        let result = union.result();

        assert!(
            (result.theta() - expected_theta).abs() < 1e-6,
            "state_a={state_a:?}, state_b={state_b:?}, theta={}",
            result.theta()
        );
        assert_eq!(
            result.num_retained(),
            expected_count,
            "state_a={state_a:?}, state_b={state_b:?}"
        );
        assert_eq!(
            result.is_empty(),
            expected_empty,
            "state_a={state_a:?}, state_b={state_b:?}"
        );

        let compact_a = sketch_a.compact(true);
        let compact_b = sketch_b.compact(true);
        let mut union = ThetaUnion::builder().build();
        union.update(&compact_a).unwrap();
        union.update(&compact_b).unwrap();
        let compact_result = union.result();

        assert!((compact_result.theta() - expected_theta).abs() < 1e-6);
        assert_eq!(compact_result.num_retained(), expected_count);
        assert_eq!(compact_result.is_empty(), expected_empty);
    }
}
