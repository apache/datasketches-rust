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

use datasketches::kll::DEFAULT_K;
use datasketches::kll::KllSketch;
use datasketches::kll::MAX_K;
use datasketches::kll::MIN_K;

const NUMERIC_NOISE_TOLERANCE: f64 = 1e-6;

fn assert_approx_eq(actual: f64, expected: f64, tolerance: f64) {
    let delta = (actual - expected).abs();
    assert!(
        delta <= tolerance,
        "expected {expected} +/- {tolerance}, got {actual}"
    );
}

fn rank_eps(sketch: &KllSketch<f32>) -> f64 {
    sketch.normalized_rank_error(false)
}

#[test]
fn test_k_limits() {
    let _min = KllSketch::<f32>::new(MIN_K);
    let _max = KllSketch::<f32>::new(MAX_K);
}

#[test]
#[should_panic(expected = "k must be in")]
fn test_k_too_small_panics() {
    KllSketch::<f32>::new(MIN_K - 1);
}

#[test]
fn test_empty() {
    let sketch = KllSketch::<f32>::new(DEFAULT_K);
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert!(sketch.min_item().is_none());
    assert!(sketch.max_item().is_none());
    assert!(sketch.rank(&0.0, true).is_none());
    assert!(sketch.quantile(0.5, true).is_none());
    assert!(sketch.pmf(&[0.0f32], true).is_none());
    assert!(sketch.cdf(&[0.0f32], true).is_none());
}

#[test]
#[should_panic(expected = "rank must be in [0.0, 1.0]")]
fn test_quantile_out_of_range_panics() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    sketch.update(0.0);
    sketch.quantile(-1.0, true);
}

#[test]
fn test_one_item() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    sketch.update(1.0);
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 1);
    assert_eq!(sketch.num_retained(), 1);
    assert_eq!(sketch.rank(&1.0, false), Some(0.0));
    assert_eq!(sketch.rank(&1.0, true), Some(1.0));
    assert_eq!(sketch.rank(&2.0, false), Some(1.0));
    assert_eq!(sketch.min_item().cloned(), Some(1.0));
    assert_eq!(sketch.max_item().cloned(), Some(1.0));
    assert_eq!(sketch.quantile(0.5, true), Some(1.0));
}

#[test]
fn test_nan_is_ignored() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    sketch.update(f32::NAN);
    assert!(sketch.is_empty());
    sketch.update(0.0);
    sketch.update(f32::NAN);
    assert_eq!(sketch.n(), 1);
}

#[test]
fn test_many_items_exact_mode() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    let n = DEFAULT_K as usize;
    for i in 1..=n {
        sketch.update(i as f32);
        assert_eq!(sketch.n(), i as u64);
    }
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.num_retained(), n);
    assert_eq!(sketch.min_item().cloned(), Some(1.0));
    assert_eq!(sketch.quantile(0.0, true), Some(1.0));
    assert_eq!(sketch.max_item().cloned(), Some(n as f32));
    assert_eq!(sketch.quantile(1.0, true), Some(n as f32));

    for i in 1..=n {
        let inclusive_rank = i as f64 / n as f64;
        assert_eq!(sketch.rank(&(i as f32), true), Some(inclusive_rank));
        let exclusive_rank = (i - 1) as f64 / n as f64;
        assert_eq!(sketch.rank(&(i as f32), false), Some(exclusive_rank));
    }
}

#[test]
fn test_ten_items_quantiles() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    for i in 1..=10 {
        sketch.update(i as f32);
    }
    assert_eq!(sketch.quantile(0.0, true), Some(1.0));
    assert_eq!(sketch.quantile(0.5, true), Some(5.0));
    assert_eq!(sketch.quantile(0.99, true), Some(10.0));
    assert_eq!(sketch.quantile(1.0, true), Some(10.0));
}

#[test]
fn test_hundred_items_quantiles() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    for i in 0..100 {
        sketch.update(i as f32);
    }
    assert_eq!(sketch.quantile(0.0, true), Some(0.0));
    assert_eq!(sketch.quantile(0.01, true), Some(0.0));
    assert_eq!(sketch.quantile(0.5, true), Some(49.0));
    assert_eq!(sketch.quantile(0.99, true), Some(98.0));
    assert_eq!(sketch.quantile(1.0, true), Some(99.0));
}

#[test]
fn test_many_items_estimation_mode_rank_error() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    let n = 10_000;
    for i in 0..n {
        sketch.update(i as f32);
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.min_item().cloned(), Some(0.0));
    assert_eq!(sketch.max_item().cloned(), Some((n - 1) as f32));

    let rank_eps = rank_eps(&sketch);
    for i in (0..n).step_by(10) {
        let true_rank = i as f64 / n as f64;
        let rank = sketch.rank(&(i as f32), false).unwrap();
        assert_approx_eq(rank, true_rank, rank_eps);
    }

    assert!(sketch.num_retained() > 0);
}

#[test]
fn test_rank_cdf_pmf_consistency() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    let n = 200;
    let mut values = Vec::with_capacity(n);
    for i in 0..n {
        sketch.update(i as f32);
        values.push(i as f32);
    }

    let ranks = sketch.cdf(&values, false).unwrap();
    let pmf = sketch.pmf(&values, false).unwrap();

    let mut subtotal = 0.0;
    for i in 0..n {
        let rank = sketch.rank(&values[i], false).unwrap();
        assert_eq!(rank, ranks[i]);
        subtotal += pmf[i];
        assert!(
            (ranks[i] - subtotal).abs() <= NUMERIC_NOISE_TOLERANCE,
            "cdf vs pmf mismatch at index {i}"
        );
    }

    let ranks = sketch.cdf(&values, true).unwrap();
    let pmf = sketch.pmf(&values, true).unwrap();

    let mut subtotal = 0.0;
    for i in 0..n {
        let rank = sketch.rank(&values[i], true).unwrap();
        assert_eq!(rank, ranks[i]);
        subtotal += pmf[i];
        assert!(
            (ranks[i] - subtotal).abs() <= NUMERIC_NOISE_TOLERANCE,
            "cdf vs pmf mismatch at index {i}"
        );
    }
}

#[test]
#[should_panic(expected = "split_points must be unique and monotonically increasing")]
fn test_out_of_order_split_points_panics() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    sketch.update(0.0);
    let split_points = [1.0, 0.0];
    let _ = sketch.cdf(&split_points, true);
}

#[test]
#[should_panic(expected = "split_points must not contain NaN values")]
fn test_nan_split_point_panics() {
    let mut sketch = KllSketch::<f32>::new(DEFAULT_K);
    sketch.update(0.0);
    let split_points = [f32::NAN];
    let _ = sketch.cdf(&split_points, true);
}

#[test]
fn test_merge() {
    let mut sketch1 = KllSketch::<f32>::new(DEFAULT_K);
    let mut sketch2 = KllSketch::<f32>::new(DEFAULT_K);
    let n = 10_000;
    for i in 0..n {
        sketch1.update(i as f32);
        sketch2.update((2 * n - i - 1) as f32);
    }

    assert_eq!(sketch1.min_item().cloned(), Some(0.0));
    assert_eq!(sketch1.max_item().cloned(), Some((n - 1) as f32));
    assert_eq!(sketch2.min_item().cloned(), Some(n as f32));
    assert_eq!(sketch2.max_item().cloned(), Some((2 * n - 1) as f32));

    sketch1.merge(&sketch2);

    assert!(!sketch1.is_empty());
    assert_eq!(sketch1.n(), (2 * n) as u64);
    assert_eq!(sketch1.min_item().cloned(), Some(0.0));
    assert_eq!(sketch1.max_item().cloned(), Some((2 * n - 1) as f32));
    let median = sketch1.quantile(0.5, true).unwrap();
    let rank_eps = rank_eps(&sketch1);
    assert_approx_eq(median as f64, n as f64, n as f64 * rank_eps);
}

#[test]
fn test_merge_lower_k() {
    let mut sketch1 = KllSketch::<f32>::new(256);
    let mut sketch2 = KllSketch::<f32>::new(128);
    let n = 10_000;
    for i in 0..n {
        sketch1.update(i as f32);
        sketch2.update((2 * n - i - 1) as f32);
    }

    sketch1.merge(&sketch2);

    assert_eq!(sketch1.n(), (2 * n) as u64);
    assert_eq!(sketch1.min_item().cloned(), Some(0.0));
    assert_eq!(sketch1.max_item().cloned(), Some((2 * n - 1) as f32));
    assert_eq!(
        sketch1.normalized_rank_error(false),
        sketch2.normalized_rank_error(false)
    );
    assert_eq!(
        sketch1.normalized_rank_error(true),
        sketch2.normalized_rank_error(true)
    );
    let median = sketch1.quantile(0.5, true).unwrap();
    let rank_eps = rank_eps(&sketch1);
    assert_approx_eq(median as f64, n as f64, n as f64 * rank_eps);
}

#[test]
fn test_merge_exact_mode_lower_k() {
    let mut sketch1 = KllSketch::<f32>::new(256);
    let sketch2 = KllSketch::<f32>::new(128);
    let n = 10_000;
    for i in 0..n {
        sketch1.update(i as f32);
    }

    let err_before = sketch1.normalized_rank_error(true);
    sketch1.merge(&sketch2);
    assert_eq!(sketch1.normalized_rank_error(true), err_before);

    assert_eq!(sketch1.n(), n as u64);
    assert_eq!(sketch1.min_item().cloned(), Some(0.0));
    assert_eq!(sketch1.max_item().cloned(), Some((n - 1) as f32));
    let median = sketch1.quantile(0.5, true).unwrap();
    let rank_eps = rank_eps(&sketch1);
    assert_approx_eq(median as f64, (n / 2) as f64, (n as f64 / 2.0) * rank_eps);
}

#[test]
fn test_merge_min_max_from_other() {
    let mut sketch1 = KllSketch::<f32>::new(DEFAULT_K);
    let mut sketch2 = KllSketch::<f32>::new(DEFAULT_K);
    sketch1.update(1.0);
    sketch2.update(2.0);
    sketch2.merge(&sketch1);
    assert_eq!(sketch2.min_item().cloned(), Some(1.0));
    assert_eq!(sketch2.max_item().cloned(), Some(2.0));
}

#[test]
fn test_merge_min_max_large_other() {
    let mut sketch1 = KllSketch::<f32>::new(DEFAULT_K);
    for i in 0..1_000_000 {
        sketch1.update(i as f32);
    }
    let mut sketch2 = KllSketch::<f32>::new(DEFAULT_K);
    sketch2.merge(&sketch1);
    assert_eq!(sketch2.min_item().cloned(), Some(0.0));
    assert_eq!(sketch2.max_item().cloned(), Some(999_999.0));
}
