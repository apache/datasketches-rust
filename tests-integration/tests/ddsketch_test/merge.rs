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

use datasketches::ddsketch::CollapseDirection;
use datasketches::ddsketch::DdSketch;
use datasketches::error::ErrorKind;

use super::next_random;

#[test]
fn partitioning_order_and_merge_trees_preserve_histogram() {
    let mut state = 1337;
    let values: Vec<_> = (0..4096)
        .map(|i| {
            let magnitude = 1.7f64.powi((next_random(&mut state) % 1200) as i32 - 600);
            let value = match i % 3 {
                0 => -magnitude,
                1 => 0.0,
                _ => magnitude,
            };
            (value, next_random(&mut state) % 100 + 1)
        })
        .collect();
    for limit in [1, 2, 16, 2048] {
        for direction in [CollapseDirection::Lowest, CollapseDirection::Highest] {
            let empty = DdSketch::with_collapse_direction(limit, 0.01, direction).unwrap();
            let mut direct = empty.clone();
            let mut reversed = empty.clone();
            let mut partitions = vec![empty.clone(); 16];
            for (i, &(value, weight)) in values.iter().enumerate() {
                direct.update_weighted(value, weight).unwrap();
                partitions[i % 16].update_weighted(value, weight).unwrap();
            }
            for &(value, weight) in values.iter().rev() {
                reversed.update_weighted(value, weight).unwrap();
            }
            assert_eq!(direct.serialize(), reversed.serialize());
            let mut sequential = empty.clone();
            for partition in &partitions {
                let before = partition.serialize();
                sequential.merge(partition).unwrap();
                assert_eq!(partition.serialize(), before);
            }
            assert_eq!(sequential.serialize(), direct.serialize());
            while partitions.len() > 1 {
                partitions = partitions
                    .chunks(2)
                    .map(|pair| {
                        let mut merged = DdSketch::deserialize(&pair[0].serialize()).unwrap();
                        merged.merge(&pair[1]).unwrap();
                        merged
                    })
                    .collect();
            }
            assert_eq!(partitions[0].serialize(), direct.serialize());
            let mut adopted = empty.clone();
            adopted.merge(&direct).unwrap();
            adopted.merge(&empty).unwrap();
            assert_eq!(adopted.serialize(), direct.serialize());
        }
    }
}

#[test]
fn collapsed_boundary_is_preserved_when_merging_new_values() {
    for direction in [CollapseDirection::Lowest, CollapseDirection::Highest] {
        let mut left = DdSketch::with_collapse_direction(5, 0.01, direction).unwrap();
        let mut right = left.clone();
        let mut direct = left.clone();
        for value in [-1000.0, -1.0, 1.0, 1000.0] {
            left.update(value).unwrap();
            direct.update(value).unwrap();
        }
        for value in [-5.0, -2.0, 0.0, 2.0, 5.0] {
            right.update(value).unwrap();
            direct.update(value).unwrap();
        }
        let mut reverse = right.clone();
        reverse.merge(&left).unwrap();
        left.merge(&right).unwrap();
        assert_eq!(left.serialize(), direct.serialize());
        assert_eq!(reverse.serialize(), direct.serialize());
        for i in 0..=32 {
            let q = i as f64 / 32.0;
            assert_eq!(
                left.quantile_is_collapsed(q).unwrap(),
                direct.quantile_is_collapsed(q).unwrap()
            );
        }
    }
}

#[test]
fn incompatible_and_overflowing_merges_are_atomic() {
    let mut sketch = DdSketch::default();
    sketch.update_weighted(2.0, u64::MAX).unwrap();
    let before = sketch.serialize();
    let mut overflowing = DdSketch::default();
    overflowing.update(-100.0).unwrap();
    for other in [
        DdSketch::new(128, 0.01).unwrap(),
        DdSketch::new(2048, 0.02).unwrap(),
        DdSketch::with_collapse_direction(2048, 0.01, CollapseDirection::Highest).unwrap(),
        overflowing,
    ] {
        assert_eq!(
            sketch.merge(&other).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(sketch.serialize(), before);
    }
    let mut empty = DdSketch::default();
    assert!(empty.merge(&DdSketch::new(1, 0.01).unwrap()).is_err());
}
