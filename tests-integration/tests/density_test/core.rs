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

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::density::DensitySketch;
use datasketches::density::GaussianKernel;
use datasketches::density::Kernel;
use datasketches::error::ErrorKind;

#[derive(Debug, Clone, PartialEq)]
struct TaggedKernel {
    id: u32,
    gamma: f64,
}

impl Kernel<f64> for TaggedKernel {
    fn evaluate(&self, a: &[f64], b: &[f64]) -> f64 {
        assert_eq!(a.len(), b.len(), "point dimension mismatch");
        let dist_sq: f64 = a.iter().zip(b).map(|(&x, &y)| (x - y) * (x - y)).sum();
        (-self.gamma * dist_sq).exp()
    }
}

#[test]
fn parameter_validation() {
    for k in [0_u16, 1] {
        assert_eq!(
            DensitySketch::<f64>::new(k, 3).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            DensitySketch::<f32>::new(k, 3).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            DensitySketch::<f64, _>::with_kernel(k, 3, TaggedKernel { id: 1, gamma: 0.5 })
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }

    assert_eq!(
        DensitySketch::<f64>::new(10, 0).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        DensitySketch::<f32>::new(10, 0).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        DensitySketch::<f64, _>::with_kernel(10, 0, TaggedKernel { id: 1, gamma: 0.5 })
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );

    let min_sketch = DensitySketch::<f64>::new(2, 1).unwrap();
    assert_eq!(min_sketch.k(), 2);
    assert_eq!(min_sketch.dim(), 1);
    assert!(min_sketch.is_empty());

    let max_k_sketch = DensitySketch::<f32>::new(u16::MAX, 4).unwrap();
    assert_eq!(max_k_sketch.k(), u16::MAX);
    assert_eq!(max_k_sketch.dim(), 4);
}

#[test]
fn empty_and_reset_retain_configuration() {
    let kernel = TaggedKernel {
        id: 42,
        gamma: 0.25,
    };
    let mut sketch = DensitySketch::<f64, _>::with_kernel(8, 3, kernel.clone()).unwrap();

    assert_eq!(sketch.k(), 8);
    assert_eq!(sketch.dim(), 3);
    assert_eq!(sketch.kernel(), &kernel);
    assert_eq!(sketch.total_weight(), 0);
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.num_retained(), 0);
    assert!(sketch.estimated_size() > 0);
    assert_eq!(sketch.estimate(&[0.0, 0.0, 0.0]).unwrap(), None);
    assert_eq!(sketch.iter().len(), 0);
    assert!(sketch.iter().next().is_none());

    for i in 0..100 {
        let v = i as f64;
        sketch.update(&[v, v * 0.5, -v]).unwrap();
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), 100);
    assert!(sketch.num_retained() > 0);
    assert!(sketch.num_retained() < 100);
    assert!(sketch.estimated_size() >= sketch.num_retained() * 3 * size_of::<f64>());
    assert!(sketch.estimate(&[50.0, 25.0, -50.0]).unwrap().is_some());

    sketch.reset();
    assert_eq!(sketch.k(), 8);
    assert_eq!(sketch.dim(), 3);
    assert_eq!(sketch.kernel(), &kernel);
    assert_eq!(sketch.total_weight(), 0);
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.estimate(&[50.0, 25.0, -50.0]).unwrap(), None);
    assert_eq!(sketch.iter().len(), 0);
    assert!(sketch.iter().next().is_none());

    // Sketch remains usable after reset.
    sketch.update(&[1.0, 2.0, 3.0]).unwrap();
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), 1);
    assert_eq!(sketch.num_retained(), 1);
    assert_eq!(sketch.estimate(&[1.0, 2.0, 3.0]).unwrap(), Some(1.0));
}

#[test]
fn update_and_estimate_reject_invalid_inputs_without_mutating() {
    let mut empty_f64 = DensitySketch::<f64>::new(10, 3).unwrap();
    let mut exact_f64 = DensitySketch::<f64>::new(10, 3).unwrap();
    exact_f64.update(&[1.0, 2.0, 3.0]).unwrap();
    let mut est_f64 = DensitySketch::<f64>::new(10, 3).unwrap();
    for i in 0..30 {
        est_f64.update(&[i as f64, 0.0, -(i as f64)]).unwrap();
    }

    let invalid_f64: [&[f64]; 8] = [
        &[],
        &[1.0, 2.0],
        &[1.0, 2.0, 3.0, 4.0],
        &[f64::NAN, 0.0, 0.0],
        &[0.0, f64::INFINITY, 0.0],
        &[0.0, 0.0, f64::NEG_INFINITY],
        &[f64::from_bits(0x7ff8_0000_0000_0001), 1.0, 2.0],
        &[1.0, f64::NAN, f64::INFINITY],
    ];

    for sketch in [&mut empty_f64, &mut exact_f64, &mut est_f64] {
        let before = sketch.serialize();
        for &bad in &invalid_f64 {
            assert_eq!(
                sketch.update(bad).unwrap_err().kind(),
                ErrorKind::InvalidArgument
            );
            assert_eq!(
                sketch.estimate(bad).unwrap_err().kind(),
                ErrorKind::InvalidArgument
            );
            assert_eq!(sketch.serialize(), before);
        }
    }

    let mut sketch_f32 = DensitySketch::<f32>::new(10, 2).unwrap();
    let invalid_f32: [&[f32]; 6] = [
        &[],
        &[1.0],
        &[1.0, 2.0, 3.0],
        &[f32::NAN, 0.0],
        &[0.0, f32::INFINITY],
        &[f32::NEG_INFINITY, 0.0],
    ];
    for state in 0..2 {
        if state == 1 {
            sketch_f32.update(&[1.0, 2.0]).unwrap();
        }
        let before = sketch_f32.serialize();
        for &bad in &invalid_f32 {
            assert_eq!(
                sketch_f32.update(bad).unwrap_err().kind(),
                ErrorKind::InvalidArgument
            );
            assert_eq!(
                sketch_f32.estimate(bad).unwrap_err().kind(),
                ErrorKind::InvalidArgument
            );
            assert_eq!(sketch_f32.serialize(), before);
        }
    }
}

#[test]
fn iterator_contracts_and_weights() {
    // Exact mode: sum of weights equals total_weight().
    let mut exact = DensitySketch::<f64>::new(16, 2).unwrap();
    for i in 0..16 {
        exact.update(&[i as f64, -(i as f64)]).unwrap();
    }
    assert!(!exact.is_estimation_mode());
    let exact_sum: u64 = exact.iter().map(|(_, weight)| weight).sum();
    assert_eq!(exact_sum, exact.total_weight());

    // Estimation mode: ExactSizeIterator, FusedIterator, Clone, IntoIterator.
    let mut sketch = DensitySketch::<f64>::new(8, 3).unwrap();
    for i in 0..200 {
        let v = i as f64;
        sketch.update(&[v, v.sqrt(), -v]).unwrap();
    }
    assert!(sketch.is_estimation_mode());

    let mut iter = sketch.iter();
    assert_eq!(iter.len(), sketch.num_retained());
    assert_eq!(
        iter.size_hint(),
        (sketch.num_retained(), Some(sketch.num_retained()))
    );

    let mut collected = Vec::new();
    if let Some((pt, weight)) = iter.next() {
        collected.push((pt.to_vec(), weight));
    }
    assert_eq!(iter.len(), sketch.num_retained() - 1);

    // Clone mid-iteration yields identical remaining elements independently.
    let cloned_remaining: Vec<_> = iter
        .clone()
        .map(|(pt, weight)| (pt.to_vec(), weight))
        .collect();
    let orig_remaining: Vec<_> = (&mut iter)
        .map(|(pt, weight)| (pt.to_vec(), weight))
        .collect();
    assert_eq!(cloned_remaining, orig_remaining);

    // FusedIterator keeps returning None with length 0.
    assert_eq!(iter.len(), 0);
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.len(), 0);

    collected.extend(orig_remaining);
    assert_eq!(collected.len(), sketch.num_retained());

    // IntoIterator for &DensitySketch matches sketch.iter().
    let via_into_iter: Vec<_> = (&sketch)
        .into_iter()
        .map(|(pt, weight)| (pt.to_vec(), weight))
        .collect();
    assert_eq!(via_into_iter, collected);

    for (pt, weight) in &collected {
        assert_eq!(pt.len(), 3);
        assert!(weight.is_power_of_two());
    }

    // Kernel::default sanity check.
    let k = GaussianKernel;
    assert_eq!(k.evaluate(&[1.0_f64, 2.0], &[1.0, 2.0]), 1.0);
    assert!(
        catch_unwind(|| {
            let _ = k.evaluate(&[1.0_f64], &[1.0, 2.0]);
        })
        .is_err()
    );
}

#[test]
fn update_weight_overflow_panics_without_mutating() {
    let mut one = DensitySketch::<f64>::new(16, 1).unwrap();
    one.update(&[1.0]).unwrap();
    let mut sketch = one.clone();
    for _ in 0..63 {
        let snapshot = sketch.clone();
        sketch.merge(&snapshot).unwrap();
        sketch.update(&[1.0]).unwrap();
    }
    assert_eq!(sketch.total_weight(), u64::MAX);
    let before = sketch.serialize();

    // Invalid arguments still fail cleanly before the overflow check.
    assert_eq!(
        sketch.update(&[1.0, 2.0]).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        sketch.update(&[f64::NAN]).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(sketch.serialize(), before);

    // A valid update that would overflow u64::MAX panics and leaves the sketch unchanged.
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _ = sketch.update(&[2.0]);
        }))
        .is_err()
    );
    assert_eq!(sketch.total_weight(), u64::MAX);
    assert_eq!(sketch.serialize(), before);
}
