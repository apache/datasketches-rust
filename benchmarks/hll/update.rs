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
use crate::hash_inputs::bytes_64_values;
use crate::hash_inputs::u64_pair;
use crate::hash_inputs::variable_byte_slices;

fn precomputed_coupons(
    bencher: Bencher,
    hll_type: HllType,
    update: impl Fn(&mut HllSketch, &[Coupon]),
) {
    let coupons: Vec<_> = (0..400_000_u64).map(Coupon::from_value).collect();
    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(14, hll_type).unwrap();
        for batch in black_box(&coupons).chunks(8_192) {
            update(&mut sketch, batch);
        }
        black_box(sketch.estimate())
    });
}

fn u64(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
        for value in 0..ITEMS as u64 {
            sketch.update(black_box(value));
        }
        black_box(sketch)
    });
}

fn u64_tuple(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
        for value in 0..ITEMS as u64 {
            sketch.update(black_box(u64_pair(value)));
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

fn variable_bytes(bencher: Bencher) {
    let values = bytes_64_values();
    let keys = variable_byte_slices(&values);

    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
        for &key in &keys {
            sketch.update(raw_bytes::from_slice(black_box(key)));
        }
        black_box(sketch)
    });
}

fn prefixed_bytes(bencher: Bencher) {
    let values = bytes_64_values();
    let keys = variable_byte_slices(&values);

    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
        for &key in &keys {
            sketch.update(black_box(key));
        }
        black_box(sketch)
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("hll::update::u64", 6_000, u64);
    runner.case("hll::update::u64_pair", 5_500, u64_tuple);
    runner.case("hll::update::bytes_32", 2_000, bytes_32);
    runner.case("hll::update::variable_bytes", 2_000, variable_bytes);
    runner.case("hll::update::prefixed_bytes", 1_600, prefixed_bytes);
    for (label, hll_type, rounds) in [
        ("hll4", HllType::Hll4, 200),
        ("hll6", HllType::Hll6, 400),
        ("hll8", HllType::Hll8, 600),
    ] {
        runner.compare(
            &format!("hll::update::precomputed_coupons/{label}/8192"),
            rounds,
            &[
                ("scalar", &|b| {
                    precomputed_coupons(b, hll_type, |sketch, batch| {
                        for &coupon in batch {
                            sketch.update_with_coupon(coupon);
                        }
                    })
                }),
                ("batch", &|b| {
                    precomputed_coupons(b, hll_type, |sketch, batch| {
                        sketch.update_with_coupons(batch.iter().copied());
                    })
                }),
            ],
        );
    }
}
