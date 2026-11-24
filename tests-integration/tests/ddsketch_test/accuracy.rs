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

use datasketches::ddsketch::DdSketch;
use datasketches::ddsketch::DdSketchBuilder;
use datasketches::ddsketch::MappingKind;
use datasketches::ddsketch::StoreKind;

fn exact_lower_quantile(sorted: &[f64], rank: f64) -> f64 {
    let last = sorted.len() - 1;
    if rank == 0.0 || last == 0 {
        return sorted[0];
    }
    if rank == 1.0 {
        return sorted[last];
    }
    let idx = ((rank * (last as f64)).floor() as usize).min(last);
    sorted[idx]
}

fn assert_within_relative_accuracy(estimate: f64, expected: f64, alpha: f64, ctx: &str) {
    if expected == 0.0 {
        assert_eq!(estimate, 0.0, "{ctx}");
    } else {
        let rel_err = (estimate - expected).abs() / expected.abs();
        assert!(
            rel_err <= alpha + 1e-12,
            "{ctx}: estimate={estimate}, expected={expected}, rel_err={rel_err}, alpha={alpha}"
        );
    }
}

#[test]
fn uncollapsed_mappings_and_stores_satisfy_relative_value_bound() {
    // Log-uniform + heavy-tailed positive dataset spanning 15 orders of magnitude.
    let mut positive: Vec<f64> = (0..2_000)
        .map(|i| {
            let t = f64::from(i) / 1_999.0;
            10.0_f64.powf(-6.0 + 15.0 * t)
        })
        .collect();
    positive.sort_by(f64::total_cmp);

    let mut signed: Vec<f64> = (-1_000_i32..=1_000)
        .map(|i| {
            if i == 0 {
                0.0
            } else {
                let sign = if i < 0 { -1.0 } else { 1.0 };
                let mag = 10.0_f64.powf(f64::from(i.abs()) / 125.0);
                sign * mag
            }
        })
        .collect();
    signed.sort_by(f64::total_cmp);

    let ranks = [
        0.0, 0.001, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 0.999, 1.0,
    ];

    for alpha in [0.005, 0.01, 0.05] {
        for kind in [
            MappingKind::Cubic,
            MappingKind::Linear,
            MappingKind::Logarithmic,
        ] {
            for store in [
                StoreKind::UnboundedDense,
                StoreKind::Sparse,
                StoreKind::CollapsingLowestDense { max_num_bins: 8192 },
                StoreKind::CollapsingHighestDense { max_num_bins: 8192 },
            ] {
                let mut pos_sketch = DdSketchBuilder::new(alpha)
                    .mapping(kind)
                    .store(store)
                    .build()
                    .unwrap();
                for &v in &positive {
                    pos_sketch.update(v);
                }

                let expected_sum: f64 = positive.iter().sum();
                assert_eq!(pos_sketch.sum(), Some(expected_sum));
                assert_eq!(pos_sketch.min_value(), Some(positive[0]));
                assert_eq!(pos_sketch.max_value(), Some(*positive.last().unwrap()));

                let batch = pos_sketch.quantiles(&ranks).unwrap().unwrap();
                for (i, &q) in ranks.iter().enumerate() {
                    let single = pos_sketch.quantile(q).unwrap().unwrap();
                    assert_eq!(batch[i].to_bits(), single.to_bits());
                    assert_within_relative_accuracy(
                        single,
                        exact_lower_quantile(&positive, q),
                        alpha,
                        &format!("pos q={q} {kind:?}/{store:?}/{alpha}"),
                    );
                }

                let mut signed_sketch = DdSketchBuilder::new(alpha)
                    .mapping(kind)
                    .store(store)
                    .build()
                    .unwrap();
                for &v in &signed {
                    signed_sketch.update(v);
                }
                for &q in &ranks {
                    let est = signed_sketch.quantile(q).unwrap().unwrap();
                    assert_within_relative_accuracy(
                        est,
                        exact_lower_quantile(&signed, q),
                        alpha,
                        &format!("signed q={q} {kind:?}/{store:?}/{alpha}"),
                    );
                }
            }
        }
    }
}

#[test]
fn collapsing_lowest_preserves_tail_accuracy_and_collapsing_highest_preserves_near_zero_accuracy() {
    let alpha = 0.01;
    // 512 bins at alpha = 0.01 span ~4.4 orders of magnitude per sign, so a 40-order-of-magnitude
    // input collapses ~35 orders while keeping the outermost ~4 orders uncollapsed.
    let mut lowest = DdSketchBuilder::new(alpha)
        .store(StoreKind::CollapsingLowestDense { max_num_bins: 512 })
        .build()
        .unwrap();
    let mut highest = DdSketchBuilder::new(alpha)
        .store(StoreKind::CollapsingHighestDense { max_num_bins: 512 })
        .build()
        .unwrap();

    let mut values = Vec::new();
    for exp in -20..=20 {
        let v = 10.0_f64.powi(exp);
        values.push(v);
        values.push(-v);
        lowest.update(v);
        lowest.update(-v);
        highest.update(v);
        highest.update(-v);
    }
    values.sort_by(f64::total_cmp);

    assert!(lowest.num_retained() <= 1024);
    assert!(highest.num_retained() <= 1024);

    // CollapsingLowest collapses smallest magnitudes (around 0), preserving extreme negative and
    // positive tails (within the outer 4 orders of magnitude: -10^20..-10^17 and 10^17..10^20).
    for q in [0.0, 0.03, 0.97, 1.0] {
        assert_within_relative_accuracy(
            lowest.quantile(q).unwrap().unwrap(),
            exact_lower_quantile(&values, q),
            alpha,
            &format!("collapsing_lowest tail q={q}"),
        );
    }

    // CollapsingHighest collapses largest magnitudes, preserving values closest to zero (around
    // the median transition).
    for q in [0.49, 0.51] {
        assert_within_relative_accuracy(
            highest.quantile(q).unwrap().unwrap(),
            exact_lower_quantile(&values, q),
            alpha,
            &format!("collapsing_highest near-zero q={q}"),
        );
    }
}

#[test]
fn sparse_and_unbounded_dense_produce_identical_estimates() {
    let mut dense = DdSketchBuilder::new(0.01)
        .store(StoreKind::UnboundedDense)
        .build()
        .unwrap();
    let mut sparse = DdSketchBuilder::new(0.01)
        .store(StoreKind::Sparse)
        .build()
        .unwrap();

    for &v in &[-1e12, -3.5, 0.0, 1e-9, 42.0, 1e15] {
        dense.update_with_weight(v, 7);
        sparse.update_with_weight(v, 7);
    }

    assert_eq!(dense.num_retained(), sparse.num_retained());
    assert_eq!(dense.min_value(), sparse.min_value());
    assert_eq!(dense.max_value(), sparse.max_value());
    assert_eq!(dense.sum(), sparse.sum());
    assert_eq!(
        dense.iter().collect::<Vec<_>>(),
        sparse.iter().collect::<Vec<_>>()
    );

    let scrambled_ranks = [0.99, 0.1, 0.5, 0.0, 1.0, 0.5, 0.25];
    assert_eq!(dense.quantiles(&[]).unwrap(), Some(Vec::new()));
    let q_dense = dense.quantiles(&scrambled_ranks).unwrap().unwrap();
    let q_sparse = sparse.quantiles(&scrambled_ranks).unwrap().unwrap();
    assert_eq!(q_dense, q_sparse);
    for (&r, &val) in scrambled_ranks.iter().zip(&q_dense) {
        assert_eq!(val.to_bits(), dense.quantile(r).unwrap().unwrap().to_bits());
    }
}

#[test]
fn quantile_rank_product_matches_f64_below_2_pow_53_and_stays_exact_above() {
    // With 101 well-separated items (`last = 100`), `0.95 * 100.0 == 95.0` in IEEE 754 `f64`, so
    // `quantile(0.95)` must select 0-based item 95 (`10^9.5`) rather than item 94 (`10^9.4`).
    let mut sketch_101 = DdSketch::new(0.01).unwrap();
    for i in 0..=100 {
        sketch_101.update(10.0_f64.powf(f64::from(i) * 0.1));
    }
    let expected_95 = 10.0_f64.powf(9.5);
    let p95 = sketch_101.quantile(0.95).unwrap().unwrap();
    assert!(
        (p95 - expected_95).abs() <= 0.01 * expected_95,
        "p95={p95}, expected={expected_95}"
    );
    assert_eq!(
        sketch_101.quantiles(&[0.95]).unwrap().unwrap()[0].to_bits(),
        p95.to_bits()
    );

    let mut sketch = DdSketch::new(0.01).unwrap();
    let bucket_weight: u64 = (1_u64 << 52) + 2;
    sketch.update_with_weight(10.0, bucket_weight);
    sketch.update_with_weight(1_000.0, bucket_weight);

    // `last = total_weight - 1 = 2^53 + 3` has 54 significant bits:
    // - exact `floor(0.5 * (2^53 + 3)) = 2^52 + 1 < bucket_weight` selects `10.0`;
    // - naive `f64` rounds `2^53 + 3` to even `2^53 + 4`, giving `2^52 + 2 == bucket_weight` and
    //   selecting `1000.0`.
    let p50 = sketch.quantile(0.5).unwrap().unwrap();
    assert!((p50 - 10.0).abs() <= 0.01 * 10.0, "p50={p50}");

    let batch = sketch.quantiles(&[0.5, 1.0]).unwrap().unwrap();
    assert_eq!(batch[0].to_bits(), p50.to_bits());
    assert!(
        (batch[1] - 1_000.0).abs() <= 0.01 * 1_000.0,
        "at_one={}",
        batch[1]
    );
}
