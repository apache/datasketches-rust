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

//! Property-based density sketch tests.

use datasketches::density::DensitySketch;
use datasketches::error::ErrorKind;
use quickcheck::Gen;
use quickcheck::QuickCheck;
use quickcheck::TestResult;

fn to_points(raw: &[i16], dim: usize, max_points: usize) -> Vec<Vec<f64>> {
    raw.chunks_exact(dim)
        .take(max_points)
        .map(|chunk| chunk.iter().map(|&v| f64::from(v) / 64.0).collect())
        .collect()
}

fn exact_kde(points: &[Vec<f64>], query: &[f64]) -> f64 {
    let sum: f64 = points
        .iter()
        .map(|p| {
            let dist_sq: f64 = p.iter().zip(query).map(|(&x, &y)| (x - y) * (x - y)).sum();
            (-dist_sq).exp()
        })
        .sum();
    sum / (points.len() as f64)
}

#[test]
fn prop_exact_mode_matches_reference_kde() {
    fn property(dim_seed: u8, raw_coords: Vec<i16>, query_raw: Vec<i16>) -> TestResult {
        let dim = usize::from(dim_seed % 4) + 1;
        let k = 16_u16;
        let points = to_points(&raw_coords, dim, usize::from(k));
        if points.is_empty() {
            return TestResult::discard();
        }

        let mut sketch = DensitySketch::<f64>::new(k, dim as u32).unwrap();
        for p in &points {
            sketch.update(p).unwrap();
        }
        assert!(!sketch.is_estimation_mode());
        assert_eq!(sketch.total_weight(), points.len() as u64);
        assert_eq!(
            sketch.iter().map(|(_, w)| w).sum::<u64>(),
            sketch.total_weight()
        );

        let query: Vec<f64> = (0..dim)
            .map(|idx| f64::from(query_raw.get(idx).copied().unwrap_or(0)) / 64.0)
            .collect();
        let actual = sketch.estimate(&query).unwrap().unwrap();
        let expected = exact_kde(&points, &query);
        assert!(
            (actual - expected).abs() <= 1e-12,
            "exact KDE mismatch: actual={actual}, expected={expected}"
        );
        TestResult::passed()
    }

    QuickCheck::new()
        .tests(128)
        .min_tests_passed(128)
        .rng(Gen::new(512))
        .quickcheck(property as fn(u8, Vec<i16>, Vec<i16>) -> TestResult);
}

#[test]
fn prop_partial_aggregation_and_roundtrip_preserve_invariants() {
    fn property(k_seed: u8, dim_seed: u8, raw_coords: Vec<i16>) -> TestResult {
        let k = 2 + u16::from(k_seed % 15);
        let dim = usize::from(dim_seed % 4) + 1;
        let points = to_points(&raw_coords, dim, 128);
        if points.is_empty() {
            return TestResult::discard();
        }

        let mut combined = DensitySketch::<f64>::new(k, dim as u32).unwrap();
        for chunk in points.chunks(17) {
            let mut partial = DensitySketch::<f64>::new(k, dim as u32).unwrap();
            for p in chunk {
                partial.update(p).unwrap();
            }
            let restored = DensitySketch::<f64>::deserialize(&partial.serialize()).unwrap();
            combined.merge(&restored).unwrap();
        }

        let bytes = combined.serialize();
        let roundtrip = DensitySketch::<f64>::deserialize(&bytes).unwrap();
        assert_eq!(roundtrip.serialize(), bytes);
        assert_eq!(roundtrip.k(), k);
        assert_eq!(roundtrip.dim(), dim as u32);
        assert_eq!(roundtrip.total_weight(), points.len() as u64);
        assert!(roundtrip.num_retained() <= points.len());
        assert_eq!(roundtrip.iter().len(), roundtrip.num_retained());
        assert_eq!(roundtrip.iter().count(), roundtrip.num_retained());

        for ((pt_a, w_a), (pt_b, w_b)) in combined.iter().zip(roundtrip.iter()) {
            assert_eq!(pt_a.len(), dim);
            assert!(pt_a.iter().all(|v| v.is_finite()));
            assert!(w_a.is_power_of_two());
            assert_eq!(pt_a, pt_b);
            assert_eq!(w_a, w_b);
        }

        for query in [vec![0.0; dim], points[0].clone()] {
            let est_a = combined.estimate(&query).unwrap().unwrap();
            let est_b = roundtrip.estimate(&query).unwrap().unwrap();
            assert!(est_a.is_finite() && est_a >= 0.0);
            assert_eq!(est_a.to_bits(), est_b.to_bits());
        }

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(128)
        .min_tests_passed(128)
        .rng(Gen::new(512))
        .quickcheck(property as fn(u8, u8, Vec<i16>) -> TestResult);
}

#[test]
fn prop_invalid_points_never_mutate_sketch() {
    fn property(dim_seed: u8, len_seed: u8, non_finite_kind: u8, bad_idx: u8, seed_pts: Vec<i16>) {
        let dim = usize::from(dim_seed % 5) + 1;
        let mut sketch = DensitySketch::<f64>::new(8, dim as u32).unwrap();
        for p in to_points(&seed_pts, dim, 20) {
            sketch.update(&p).unwrap();
        }
        let before = sketch.serialize();

        // Wrong dimension length.
        let wrong_len = {
            let candidate = usize::from(len_seed % 7);
            if candidate == dim { dim + 1 } else { candidate }
        };
        let wrong_point = vec![1.0; wrong_len];
        assert_eq!(
            sketch.update(&wrong_point).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            sketch.estimate(&wrong_point).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(sketch.serialize(), before);

        // Non-finite coordinate at an arbitrary index.
        let mut bad_point = vec![0.5; dim];
        let idx = usize::from(bad_idx) % dim;
        bad_point[idx] = match non_finite_kind % 3 {
            0 => f64::NAN,
            1 => f64::INFINITY,
            _ => f64::NEG_INFINITY,
        };
        assert_eq!(
            sketch.update(&bad_point).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            sketch.estimate(&bad_point).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(sketch.serialize(), before);
    }

    QuickCheck::new()
        .tests(128)
        .rng(Gen::new(256))
        .quickcheck(property as fn(u8, u8, u8, u8, Vec<i16>));
}
