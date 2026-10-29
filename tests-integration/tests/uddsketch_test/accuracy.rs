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

use super::build;
use super::shuffle;

#[test]
fn value_error_certificate_across_distributions() {
    let mut values = (0..6000)
        .map(|i| (f64::from(i) / 4.0 - 750.0).exp())
        .filter(|x| x.is_finite())
        .collect::<Vec<_>>();
    values.extend(values.clone().into_iter().map(|x| -x));
    values.extend([0.0, -0.0, f64::MAX, -f64::MAX]);
    for bits in 1..256 {
        values.extend([f64::from_bits(bits), -f64::from_bits(bits)]);
    }
    values.extend(std::iter::repeat_n(1.0, 500));
    shuffle(&mut values, 41);
    for limit in [3, 32, 512, 4096] {
        let sketch = build(&values, limit, 0.01);
        assert!(sketch.num_buckets() <= limit as usize);
        let bound = sketch.relative_error();
        assert!(bound.is_finite());
        let mut sorted = values.clone();
        sorted.sort_by(f64::total_cmp);
        // Power-of-two ranks have exact products here, keeping the reference definition explicit.
        for i in 0..=1024 {
            let q = f64::from(i) / 1024.0;
            let truth = sorted[(q * (sorted.len() - 1) as f64).floor() as usize];
            let estimate = sketch.quantile(q).unwrap().unwrap();
            assert!(estimate.is_finite());
            if truth == 0.0 {
                assert_eq!(estimate, 0.0);
            } else {
                assert!(
                    ((estimate - truth) / truth).abs() <= bound,
                    "q={q}, truth={truth}, estimate={estimate}, bound={bound}"
                );
            }
        }
    }
}

#[test]
fn global_collapse_can_exceed_initial_target() {
    let values = (1..=10_000)
        .map(|i| f64::from(i).powi(3))
        .collect::<Vec<_>>();
    let sketch = build(&values, 8, 0.001);
    assert!(sketch.compaction_level() > 0);
    assert!(sketch.relative_error() > sketch.initial_relative_accuracy());
    assert_eq!(sketch.quantile(0.0).unwrap(), Some(1.0));
    assert_eq!(sketch.quantile(1.0).unwrap(), Some(1e12));
}
