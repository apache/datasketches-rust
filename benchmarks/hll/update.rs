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

use std::hint::black_box;

use benchmarks::Bencher;
use benchmarks::Runner;
use datasketches::hash::value::raw_bytes;
use datasketches::hll::Coupon;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;

use crate::hash_inputs::ITEMS;
use crate::hash_inputs::bytes_32_values;

fn u64(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
        for value in 0..ITEMS as u64 {
            sketch.update(black_box(value));
        }
        black_box(sketch)
    });
}

fn bytes_32(bencher: Bencher) {
    let values = bytes_32_values();

    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
        for value in &values {
            sketch.update(raw_bytes::from_slice(black_box(value)));
        }
        black_box(sketch)
    });
}

fn dense_byte_batches(bencher: Bencher, hash: impl Fn(&[u8; 8]) -> Coupon) {
    let keys: Vec<_> = (0..400_000_u64).map(u64::to_le_bytes).collect();
    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(14, HllType::Hll8).unwrap();
        let mut coupons = Vec::new();
        for batch in black_box(&keys).chunks(8_192) {
            coupons.clear();
            coupons.extend(batch.iter().map(&hash));
            for &coupon in &coupons {
                sketch.update_with_coupon(coupon);
            }
        }
        black_box(sketch.composite_estimate())
    });
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "hll::update::dense_eight_byte_batches/8192",
        200,
        &[
            ("streaming", &|b| {
                dense_byte_batches(b, |key| Coupon::from_value(raw_bytes::from_slice(key)))
            }),
            ("one_shot", &|b| {
                dense_byte_batches(b, |key| Coupon::from_bytes(key))
            }),
        ],
    );
    runner.case("hll::update::u64", 6_000, u64);
    runner.case("hll::update::bytes_32", 2_000, bytes_32);
}
