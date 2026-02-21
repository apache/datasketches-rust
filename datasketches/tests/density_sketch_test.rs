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

use datasketches::density::DensityKernel;
use datasketches::density::DensitySketch;
use datasketches::density::DensityValue;

#[test]
#[should_panic(expected = "operation is undefined for an empty sketch")]
fn test_empty() {
    let sketch: DensitySketch<f32> = DensitySketch::new(10, 3);
    assert!(sketch.is_empty());
    let _ = sketch.estimate(&[0.0, 0.0, 0.0]);
}

#[test]
#[should_panic(expected = "dimension mismatch")]
fn test_dimension_mismatch() {
    let mut sketch: DensitySketch<f32> = DensitySketch::new(10, 3);
    sketch.update(vec![0.0, 0.0]);
}

#[test]
#[should_panic(expected = "dimension mismatch")]
fn test_estimate_dimension_mismatch() {
    let mut sketch: DensitySketch<f32> = DensitySketch::new(10, 3);
    sketch.update(vec![0.0, 0.0, 0.0]);
    let _ = sketch.estimate(&[0.0, 0.0]);
}

#[test]
fn test_one_item() {
    let mut sketch: DensitySketch<f32> = DensitySketch::new(10, 3);

    sketch.update(vec![0.0, 0.0, 0.0]);
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.estimate(&[0.0, 0.0, 0.0]), 1.0);
    assert!(sketch.estimate(&[0.01, 0.01, 0.01]) > 0.95);
    assert!(sketch.estimate(&[1.0, 1.0, 1.0]) < 0.05);
}

#[test]
fn test_merge() {
    let mut sketch1: DensitySketch<f32> = DensitySketch::new(10, 4);
    sketch1.update(vec![0.0, 0.0, 0.0, 0.0]);
    sketch1.update(vec![1.0, 2.0, 3.0, 4.0]);

    let mut sketch2: DensitySketch<f32> = DensitySketch::new(10, 4);
    sketch2.update(vec![5.0, 6.0, 7.0, 8.0]);

    sketch1.merge(&sketch2);
    assert_eq!(sketch1.n(), 3);
    assert_eq!(sketch1.num_retained(), 3);
}

#[test]
fn test_iterator() {
    let mut sketch: DensitySketch<f32> = DensitySketch::new(10, 3);
    let n = 1000;
    for i in 1..=n {
        sketch.update(vec![i as f32, i as f32, i as f32]);
    }
    assert_eq!(sketch.n(), n as u64);
    assert!(sketch.is_estimation_mode());

    let mut count = 0;
    for item in &sketch {
        count += 1;
        assert_eq!(item.point().len(), sketch.dim() as usize);
    }
    assert_eq!(count as u32, sketch.num_retained());
}

#[derive(Clone, Copy)]
struct SphericalKernel {
    radius_squared: f32,
}

impl DensityKernel for SphericalKernel {
    fn evaluate<T: DensityValue>(&self, left: &[T], right: &[T]) -> T {
        let mut sum = 0.0f64;
        for (a, b) in left.iter().zip(right.iter()) {
            let diff = a.as_f64() - b.as_f64();
            sum += diff * diff;
        }
        if sum <= self.radius_squared as f64 {
            T::from_f64(1.0)
        } else {
            T::from_f64(0.0)
        }
    }
}

#[test]
fn test_custom_kernel() {
    let kernel = SphericalKernel {
        radius_squared: 0.25,
    };
    let mut sketch: DensitySketch<f32, SphericalKernel> = DensitySketch::with_kernel(10, 3, kernel);

    sketch.update(vec![1.0, 1.0, 1.0]);
    assert_eq!(sketch.estimate(&[1.001, 1.001, 1.001]), 1.0);
    assert_eq!(sketch.estimate(&[2.0, 2.0, 2.0]), 0.0);

    let n = 1000;
    for i in 2..=n {
        sketch.update(vec![i as f32, i as f32, i as f32]);
    }
    assert_eq!(sketch.n(), n as u64);
    assert!(sketch.is_estimation_mode());
    let mut count = 0;
    for item in &sketch {
        count += 1;
        assert_eq!(item.point().len(), sketch.dim() as usize);
    }
    assert_eq!(count as u32, sketch.num_retained());
}

#[test]
fn test_serialize_empty() {
    let sketch: DensitySketch<f64> = DensitySketch::new(10, 2);
    let bytes = sketch.serialize();
    let decoded = DensitySketch::<f64>::deserialize(&bytes).unwrap();
    assert!(decoded.is_empty());
    assert!(!decoded.is_estimation_mode());
    assert_eq!(sketch.k(), decoded.k());
    assert_eq!(sketch.dim(), decoded.dim());
    assert_eq!(sketch.n(), decoded.n());
    assert_eq!(sketch.num_retained(), decoded.num_retained());
}

#[test]
fn test_serialize_bytes() {
    let k = 10;
    let dim = 3;
    let mut sketch: DensitySketch<f64> = DensitySketch::new(k, dim);

    for i in 0..k {
        let value = i as f64;
        sketch.update(vec![value, value.sqrt(), -value]);
    }
    assert!(!sketch.is_estimation_mode());

    let bytes = sketch.serialize();
    let decoded = DensitySketch::<f64>::deserialize(&bytes).unwrap();
    assert!(!decoded.is_empty());
    assert!(!decoded.is_estimation_mode());
    assert_eq!(sketch.k(), decoded.k());
    assert_eq!(sketch.dim(), decoded.dim());
    assert_eq!(sketch.n(), decoded.n());
    assert_eq!(sketch.num_retained(), decoded.num_retained());
    let mut iter_left = sketch.iter();
    let mut iter_right = decoded.iter();
    while let (Some(left), Some(right)) = (iter_left.next(), iter_right.next()) {
        assert_eq!(left.point()[0], right.point()[0]);
        assert_eq!(left.weight(), right.weight());
    }

    let n = 1031;
    for i in k..n {
        let value = i as f64;
        sketch.update(vec![value, value.sqrt(), -value]);
    }
    assert!(sketch.is_estimation_mode());

    let bytes = sketch.serialize();
    let decoded = DensitySketch::<f64>::deserialize(&bytes).unwrap();
    assert!(!decoded.is_empty());
    assert!(decoded.is_estimation_mode());
    assert_eq!(sketch.k(), decoded.k());
    assert_eq!(sketch.dim(), decoded.dim());
    assert_eq!(sketch.n(), decoded.n());
    assert_eq!(sketch.num_retained(), decoded.num_retained());
    let mut iter_left = sketch.iter();
    let mut iter_right = decoded.iter();
    while let (Some(left), Some(right)) = (iter_left.next(), iter_right.next()) {
        assert_eq!(left.point()[0], right.point()[0]);
        assert_eq!(left.weight(), right.weight());
    }
}

#[test]
fn test_serialize_f32() {
    let k = 10;
    let dim = 3;
    let mut sketch: DensitySketch<f32> = DensitySketch::new(k, dim);

    for i in 0..k {
        let value = i as f32;
        sketch.update(vec![value, value.sin(), value.cos()]);
    }
    assert!(!sketch.is_estimation_mode());

    let bytes = sketch.serialize();
    let decoded = DensitySketch::<f32>::deserialize(&bytes).unwrap();
    assert!(!decoded.is_empty());
    assert!(!decoded.is_estimation_mode());
    assert_eq!(sketch.k(), decoded.k());
    assert_eq!(sketch.dim(), decoded.dim());
    assert_eq!(sketch.n(), decoded.n());
    assert_eq!(sketch.num_retained(), decoded.num_retained());
    let mut iter_left = sketch.iter();
    let mut iter_right = decoded.iter();
    while let (Some(left), Some(right)) = (iter_left.next(), iter_right.next()) {
        assert_eq!(left.point()[0], right.point()[0]);
        assert_eq!(left.weight(), right.weight());
        assert_eq!(left.point()[1], right.point()[1]);
        assert_eq!(left.point()[2], right.point()[2]);
    }

    let n = 1031;
    for i in k..n {
        let value = i as f32;
        sketch.update(vec![value, value.sqrt(), -value]);
    }
    assert!(sketch.is_estimation_mode());

    let bytes = sketch.serialize();
    let decoded = DensitySketch::<f32>::deserialize(&bytes).unwrap();
    assert!(!decoded.is_empty());
    assert!(decoded.is_estimation_mode());
    assert_eq!(sketch.k(), decoded.k());
    assert_eq!(sketch.dim(), decoded.dim());
    assert_eq!(sketch.n(), decoded.n());
    assert_eq!(sketch.num_retained(), decoded.num_retained());
    let mut iter_left = sketch.iter();
    let mut iter_right = decoded.iter();
    while let (Some(left), Some(right)) = (iter_left.next(), iter_right.next()) {
        assert_eq!(left.point()[0], right.point()[0]);
        assert_eq!(left.weight(), right.weight());
        assert_eq!(left.point()[1], right.point()[1]);
        assert_eq!(left.point()[2], right.point()[2]);
    }
}
