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

use datasketches::density::DensitySketch;
use datasketches::error::ErrorKind;

#[test]
fn merge_empty_combinations() {
    // Empty + empty.
    let mut left = DensitySketch::<f64>::new(10, 3).unwrap();
    let right = DensitySketch::<f64>::new(10, 3).unwrap();
    left.merge(&right).unwrap();
    assert!(left.is_empty());
    assert!(!left.is_estimation_mode());
    assert_eq!(left.total_weight(), 0);
    assert_eq!(left.num_retained(), 0);

    // Empty + non-empty (multi-level estimation mode).
    let mut non_empty = DensitySketch::<f64>::new(10, 3).unwrap();
    for i in 0..40 {
        let v = i as f64;
        non_empty.update(&[v, v * 0.5, -v]).unwrap();
    }
    assert!(non_empty.is_estimation_mode());

    left.merge(&non_empty).unwrap();
    assert!(!left.is_empty());
    assert_eq!(left.is_estimation_mode(), non_empty.is_estimation_mode());
    assert_eq!(left.total_weight(), 40);
    assert_eq!(left.num_retained(), non_empty.num_retained());
    assert_eq!(
        left.estimate(&[10.0, 5.0, -10.0]).unwrap(),
        non_empty.estimate(&[10.0, 5.0, -10.0]).unwrap()
    );
    assert_eq!(left.serialize(), non_empty.serialize());

    // Non-empty + empty preserves state exactly.
    let before = left.serialize();
    left.merge(&right).unwrap();
    assert_eq!(left.serialize(), before);
}

#[test]
fn merge_rejects_dim_mismatch_even_when_empty() {
    let mut empty_dim2 = DensitySketch::<f64>::new(10, 2).unwrap();
    let empty_dim3 = DensitySketch::<f64>::new(10, 3).unwrap();

    assert_eq!(
        empty_dim2.merge(&empty_dim3).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert!(empty_dim2.is_empty());

    let mut non_empty_dim3 = DensitySketch::<f64>::new(10, 3).unwrap();
    non_empty_dim3.update(&[1.0, 2.0, 3.0]).unwrap();
    assert_eq!(
        empty_dim2.merge(&non_empty_dim3).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert!(empty_dim2.is_empty());

    let mut non_empty_dim2 = DensitySketch::<f64>::new(10, 2).unwrap();
    non_empty_dim2.update(&[1.0, 2.0]).unwrap();
    let before = non_empty_dim2.serialize();

    assert_eq!(
        non_empty_dim2.merge(&empty_dim3).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(non_empty_dim2.serialize(), before);

    assert_eq!(
        non_empty_dim2.merge(&non_empty_dim3).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(non_empty_dim2.serialize(), before);
}

#[test]
fn merge_different_k_and_f32_support() {
    let mut small_k = DensitySketch::<f32>::new(8, 2).unwrap();
    let mut large_k = DensitySketch::<f32>::new(32, 2).unwrap();
    for i in 0..100 {
        small_k
            .update(&[i as f32 * 0.1, -(i as f32 * 0.1)])
            .unwrap();
    }
    for i in 100..250 {
        large_k
            .update(&[i as f32 * 0.1, -(i as f32 * 0.1)])
            .unwrap();
    }

    small_k.merge(&large_k).unwrap();
    assert_eq!(small_k.k(), 8);
    assert_eq!(small_k.total_weight(), 250);
    assert!(small_k.is_estimation_mode());
    assert_eq!(small_k.iter().count(), small_k.num_retained());

    let mut target_large_k = DensitySketch::<f32>::new(32, 2).unwrap();
    target_large_k.update(&[0.0, 0.0]).unwrap();
    target_large_k.merge(&small_k).unwrap();
    assert_eq!(target_large_k.k(), 32);
    assert_eq!(target_large_k.total_weight(), 251);
    assert!(target_large_k.is_estimation_mode());
}

#[test]
fn merge_triggers_cascading_compaction() {
    let mut left = DensitySketch::<f64>::new(8, 2).unwrap();
    let mut right = DensitySketch::<f64>::new(8, 2).unwrap();

    for i in 0..500 {
        let angle = (i as f64) * 0.1;
        left.update(&[angle.cos(), angle.sin()]).unwrap();
        right
            .update(&[angle.cos() + 0.5, angle.sin() - 0.5])
            .unwrap();
    }
    assert!(left.is_estimation_mode());
    assert!(right.is_estimation_mode());

    left.merge(&right).unwrap();
    assert_eq!(left.total_weight(), 1000);
    assert!(left.is_estimation_mode());
    assert!(left.num_retained() < 1000);
    assert_eq!(left.iter().count(), left.num_retained());
    for (pt, weight) in &left {
        assert_eq!(pt.len(), 2);
        assert!(weight.is_power_of_two());
    }

    let estimate = left.estimate(&[0.25, -0.25]).unwrap().unwrap();
    assert!(estimate.is_finite() && estimate > 0.0);

    let roundtrip = DensitySketch::<f64>::deserialize(&left.serialize()).unwrap();
    assert_eq!(roundtrip.total_weight(), 1000);
    assert_eq!(roundtrip.num_retained(), left.num_retained());
    assert_eq!(roundtrip.estimate(&[0.25, -0.25]).unwrap(), Some(estimate));
}

#[test]
fn merge_weight_overflow_returns_invalid_argument_without_mutating() {
    let mut one = DensitySketch::<f64>::new(16, 1).unwrap();
    one.update(&[1.0]).unwrap();

    let mut half = one.clone();
    for _ in 0..63 {
        let snapshot = half.clone();
        half.merge(&snapshot).unwrap();
    }
    assert_eq!(half.total_weight(), 1_u64 << 63);
    let half_before = half.serialize();

    // 2^63 + 2^63 overflows u64::MAX -> InvalidArgument, preserving state.
    assert_eq!(
        half.merge(&half.clone()).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(half.serialize(), half_before);

    // Reach u64::MAX exactly via doubling + 1.
    let mut max_sketch = one.clone();
    for _ in 0..63 {
        let snapshot = max_sketch.clone();
        max_sketch.merge(&snapshot).unwrap();
        max_sketch.update(&[1.0]).unwrap();
    }
    assert_eq!(max_sketch.total_weight(), u64::MAX);
    let max_before = max_sketch.serialize();

    assert_eq!(
        max_sketch.merge(&one).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(max_sketch.serialize(), max_before);

    // Merging an empty sketch of matching dimension succeeds without mutating.
    let empty_same_dim = DensitySketch::<f64>::new(16, 1).unwrap();
    max_sketch.merge(&empty_same_dim).unwrap();
    assert_eq!(max_sketch.serialize(), max_before);

    // Merging an empty sketch of mismatched dimension still fails.
    let empty_other_dim = DensitySketch::<f64>::new(16, 2).unwrap();
    assert_eq!(
        max_sketch.merge(&empty_other_dim).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(max_sketch.serialize(), max_before);
}
