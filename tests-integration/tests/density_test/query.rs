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

use std::f64::consts::PI;

use datasketches::density::DensitySketch;
use datasketches::density::GaussianKernel;
use datasketches::density::Kernel;
use googletest::assert_that;
use googletest::prelude::near;

#[derive(Debug, Clone, Copy)]
struct SphericalKernel {
    radius_squared: f64,
}

impl Kernel<f64> for SphericalKernel {
    fn evaluate(&self, a: &[f64], b: &[f64]) -> f64 {
        assert_eq!(a.len(), b.len(), "point dimension mismatch");
        let dist_sq: f64 = a.iter().zip(b).map(|(&x, &y)| (x - y) * (x - y)).sum();
        if dist_sq <= self.radius_squared {
            1.0
        } else {
            0.0
        }
    }
}

impl Kernel<f32> for SphericalKernel {
    fn evaluate(&self, a: &[f32], b: &[f32]) -> f64 {
        assert_eq!(a.len(), b.len(), "point dimension mismatch");
        let dist_sq: f64 = a
            .iter()
            .zip(b)
            .map(|(&x, &y)| {
                let d = f64::from(x) - f64::from(y);
                d * d
            })
            .sum();
        if dist_sq <= self.radius_squared {
            1.0
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ScaledGaussianKernel {
    bandwidth: f64,
}

impl Kernel<f64> for ScaledGaussianKernel {
    fn evaluate(&self, a: &[f64], b: &[f64]) -> f64 {
        assert_eq!(a.len(), b.len(), "point dimension mismatch");
        let dist_sq: f64 = a.iter().zip(b).map(|(&x, &y)| (x - y) * (x - y)).sum();
        let denom = 2.0 * self.bandwidth * self.bandwidth;
        (-dist_sq / denom).exp()
    }
}

fn exact_gaussian_kde_f64(points: &[Vec<f64>], query: &[f64]) -> f64 {
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
fn exact_kde_f64_and_f32() {
    let points_f64 = vec![
        vec![0.0, 0.0, 0.0],
        vec![1.0, -1.0, 0.5],
        vec![-0.5, 2.0, 1.5],
        vec![0.25, 0.25, -0.25],
        vec![3.0, 1.0, -2.0],
        vec![1.0, -1.0, 0.5],
    ];
    let mut sketch_f64 = DensitySketch::<f64>::new(10, 3).unwrap();
    for p in &points_f64 {
        sketch_f64.update(p).unwrap();
    }
    assert!(!sketch_f64.is_estimation_mode());
    assert_eq!(sketch_f64.total_weight(), 6);
    assert_eq!(sketch_f64.num_retained(), 6);

    for query in [
        [0.0, 0.0, 0.0],
        [1.0, -1.0, 0.5],
        [0.5, 0.25, 0.25],
        [10.0, 10.0, 10.0],
    ] {
        let actual = sketch_f64.estimate(&query).unwrap().unwrap();
        let expected = exact_gaussian_kde_f64(&points_f64, &query);
        assert_that!(actual, near(expected, 1e-12));
    }

    let points_f32: Vec<[f32; 2]> = vec![
        [0.0, 0.0],
        [0.5, -0.5],
        [-1.0, 1.25],
        [2.0, 0.75],
        [0.25, 0.25],
    ];
    let mut sketch_f32 = DensitySketch::<f32>::new(10, 2).unwrap();
    for p in &points_f32 {
        sketch_f32.update(p).unwrap();
    }
    assert!(!sketch_f32.is_estimation_mode());

    let points_f32_as_f64: Vec<Vec<f64>> = points_f32
        .iter()
        .map(|p| vec![f64::from(p[0]), f64::from(p[1])])
        .collect();
    for query in [[0.0_f32, 0.0], [0.5, -0.5], [1.0, 0.5], [-2.0, 2.0]] {
        let actual = sketch_f32.estimate(&query).unwrap().unwrap();
        let expected = exact_gaussian_kde_f64(
            &points_f32_as_f64,
            &[f64::from(query[0]), f64::from(query[1])],
        );
        assert_that!(actual, near(expected, 1e-12));
    }
}

fn deterministic_unit_pair(index: usize) -> (f64, f64) {
    // SplitMix64 hash mixing produces deterministic uniforms in (0, 1).
    let mut x = (index as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    let h1 = x ^ (x >> 31);

    let mut y = (index as u64)
        .wrapping_mul(0x517c_c1b7_2722_0a95)
        .wrapping_add(1);
    y = (y ^ (y >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    y = (y ^ (y >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    let h2 = y ^ (y >> 31);

    let u1 = ((h1 >> 11) as f64 + 0.5) / ((1_u64 << 53) as f64);
    let u2 = ((h2 >> 11) as f64 + 0.5) / ((1_u64 << 53) as f64);
    (u1, u2)
}

#[test]
fn estimation_mode_coreset_accuracy_on_multimodal_stream() {
    const N: usize = 2_000;
    let mut stream = Vec::with_capacity(N);
    let mut sketch = DensitySketch::<f64>::new(100, 2).unwrap();

    for i in 0..N {
        let (u1, u2) = deterministic_unit_pair(i);
        let r = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * PI * u2;
        let z1 = r * theta.cos();
        let z2 = r * theta.sin();

        let (cx, cy) = if i % 2 == 0 { (0.0, 0.0) } else { (2.5, -1.5) };
        let point = vec![cx + 0.8 * z1, cy + 0.8 * z2];
        sketch.update(&point).unwrap();
        stream.push(point);
    }

    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), N as u64);
    assert!(sketch.num_retained() < N);

    let mut max_error = 0.0_f64;
    for gx in 0..7 {
        let x = -1.5 + (gx as f64) * 0.9;
        for gy in 0..7 {
            let y = -3.0 + (gy as f64) * 0.75;
            let query = [x, y];
            let exact = exact_gaussian_kde_f64(&stream, &query);
            let approx = sketch.estimate(&query).unwrap().unwrap();
            let err = (approx - exact).abs();
            max_error = max_error.max(err);
        }
    }

    assert!(
        max_error < 0.05,
        "coreset KDE max grid error {max_error} exceeded 0.05"
    );
}

#[test]
fn custom_kernels_and_delta_zero_tie_break_avoid_collapse() {
    // Exact-mode spherical kernel check.
    let mut sphere_exact = DensitySketch::<f32, _>::with_kernel(
        10,
        3,
        SphericalKernel {
            radius_squared: 0.25,
        },
    )
    .unwrap();
    sphere_exact.update(&[1.0, 1.0, 1.0]).unwrap();
    assert_eq!(
        sphere_exact.estimate(&[1.001, 1.001, 1.001]).unwrap(),
        Some(1.0)
    );
    assert_eq!(sphere_exact.estimate(&[2.0, 2.0, 2.0]).unwrap(), Some(0.0));

    // Identical points with GaussianKernel: delta cancels to 0 on alternating points.
    let mut identical_gaussian = DensitySketch::<f64>::new(10, 3).unwrap();
    for _ in 0..1_000 {
        identical_gaussian.update(&[2.0, -1.0, 0.5]).unwrap();
    }
    assert!(identical_gaussian.is_estimation_mode());
    assert_eq!(identical_gaussian.total_weight(), 1_000);
    assert!(identical_gaussian.num_retained() > 0);
    let max_weight_gaussian = identical_gaussian.iter().map(|(_, w)| w).max().unwrap_or(0);
    assert!(max_weight_gaussian <= 1 << 12);
    let identical_est = identical_gaussian
        .estimate(&[2.0, -1.0, 0.5])
        .unwrap()
        .unwrap();
    assert!(
        (0.5..=1.5).contains(&identical_est),
        "unexpected mass drift on identical points: {identical_est}"
    );

    // Disjoint compact-support points with SphericalKernel: kernel(p_i, p_j) == 0 for all i != j,
    // so every compaction step hits delta == 0.0. Random tie-breaking must prevent mass wipeout
    // and level explosion.
    let mut disjoint_sphere = DensitySketch::<f64, _>::with_kernel(
        10,
        3,
        SphericalKernel {
            radius_squared: 1.0,
        },
    )
    .unwrap();
    for i in 0..1_000 {
        disjoint_sphere
            .update(&[(i as f64) * 10.0, 0.0, 0.0])
            .unwrap();
    }
    assert!(disjoint_sphere.is_estimation_mode());
    assert_eq!(disjoint_sphere.total_weight(), 1_000);
    assert!(disjoint_sphere.num_retained() > 0);
    let sum_weights: u64 = disjoint_sphere.iter().map(|(_, w)| w).sum();
    let max_weight_sphere = disjoint_sphere.iter().map(|(_, w)| w).max().unwrap_or(0);
    assert!(
        sum_weights > 100,
        "compact-support tie-break wiped out mass: sum_weights={sum_weights}"
    );
    assert!(
        max_weight_sphere <= 1 << 16,
        "compact-support tie-break exploded levels: max_weight={max_weight_sphere}"
    );

    // Bandwidth-scaled Gaussian kernel in exact and estimation modes.
    let kernel = ScaledGaussianKernel { bandwidth: 2.0 };
    let mut scaled_exact = DensitySketch::<f64, _>::with_kernel(16, 2, kernel).unwrap();
    let sample_pts = [[0.0, 0.0], [2.0, 0.0], [-1.0, 3.0]];
    for p in &sample_pts {
        scaled_exact.update(p).unwrap();
    }
    let q = [1.0, 1.0];
    let expected_scaled: f64 = sample_pts
        .iter()
        .map(|p| kernel.evaluate(p, &q))
        .sum::<f64>()
        / 3.0;
    assert_that!(
        scaled_exact.estimate(&q).unwrap().unwrap(),
        near(expected_scaled, 1e-12)
    );

    let mut scaled_est = DensitySketch::<f64, _>::with_kernel(16, 2, kernel).unwrap();
    for i in 0..500 {
        let v = (i as f64) * 0.05;
        scaled_est.update(&[v.sin(), v.cos()]).unwrap();
    }
    assert!(scaled_est.is_estimation_mode());
    assert!(scaled_est.num_retained() > 0);
    let est = scaled_est.estimate(&[0.0, 1.0]).unwrap().unwrap();
    assert!(est.is_finite() && est > 0.0);
}

#[test]
fn disjoint_points_at_k2_preserve_unbiased_total_weight() {
    const TRIALS: usize = 200;
    const N: usize = 200;

    fn run_trials<K: Kernel<f64> + Clone>(kernel: K, spacing: f64, label: &str) {
        let mut sum_total_weights = 0_u128;
        for _ in 0..TRIALS {
            let mut sketch = DensitySketch::<f64, _>::with_kernel(2, 1, kernel.clone()).unwrap();
            for i in 0..N {
                sketch.update(&[(i as f64) * spacing]).unwrap();
            }
            assert!(sketch.is_estimation_mode());
            assert_eq!(sketch.total_weight(), N as u64);
            assert!(sketch.num_retained() > 0);

            let retained_weight: u64 = sketch.iter().map(|(_, w)| w).sum();
            sum_total_weights += u128::from(retained_weight);
        }

        let mean_weight = sum_total_weights as f64 / TRIALS as f64;
        let rel_error = (mean_weight - N as f64).abs() / (N as f64);
        assert!(
            rel_error < 0.15,
            "{label}: expected unbiased mean retained weight near {N}, got {mean_weight}"
        );
    }

    run_trials(
        SphericalKernel {
            radius_squared: 1.0,
        },
        10.0,
        "SphericalKernel",
    );
    // Spacing > 30.0 makes exp(-d^2) underflow to 0.0 in f64.
    run_trials(GaussianKernel, 40.0, "GaussianKernel");
}
