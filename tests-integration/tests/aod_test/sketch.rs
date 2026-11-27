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

use datasketches::aod::ArrayOfDoublesSketchBuilder;
use datasketches::aod::CompactArrayOfDoublesSketch;
use datasketches::common::NumStdDev;
use datasketches::error::ErrorKind;
use tests_integration::MAX_THETA;
use tests_integration::ZERO_HASH_SEED;

use crate::sketch_with_range;

#[test]
fn builder_validates_configuration_at_build() {
    for error in [
        ArrayOfDoublesSketchBuilder::default()
            .num_values(0)
            .build()
            .unwrap_err(),
        ArrayOfDoublesSketchBuilder::default()
            .lg_k(4)
            .build()
            .unwrap_err(),
        ArrayOfDoublesSketchBuilder::default()
            .sampling_probability(f32::INFINITY)
            .build()
            .unwrap_err(),
        ArrayOfDoublesSketchBuilder::default()
            .seed(ZERO_HASH_SEED)
            .build()
            .unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}

#[test]
fn updates_accumulate_values_and_validate_length_atomically() {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    sketch.update("shared", [1.0, 2.0]).unwrap();
    sketch.update("shared", vec![3.0, 4.0]).unwrap();
    sketch.update("other", &[5.0, 6.0][..]).unwrap();

    assert_eq!(sketch.num_values(), 2);
    assert_eq!(sketch.num_retained(), 2);
    assert_eq!(sketch.estimate(), 2.0);
    let mut values: Vec<_> = sketch.iter().map(|entry| entry.values().to_vec()).collect();
    values.sort_by(|a, b| a[0].total_cmp(&b[0]));
    assert_eq!(values, [vec![4.0, 6.0], vec![5.0, 6.0]]);

    let before: Vec<_> = sketch
        .iter()
        .map(|entry| (entry.hash(), entry.values().to_vec()))
        .collect();
    let error = sketch.update("new", [1.0]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert!(error.message().contains("expected 2, got 1"));
    assert_eq!(sketch.num_retained(), 2);
    let after: Vec<_> = sketch
        .iter()
        .map(|entry| (entry.hash(), entry.values().to_vec()))
        .collect();
    assert_eq!(after, before);
}

#[test]
fn invalid_first_update_leaves_the_sketch_empty() {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    let error = sketch.update("key", [1.0]).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert!(sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.theta64(), MAX_THETA);
}

#[test]
fn trim_reset_and_bounds_follow_tuple_semantics() {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .lg_k(5)
        .build()
        .unwrap();
    for key in 0..1_000 {
        sketch.update(key, [key as f64]).unwrap();
    }

    assert!(sketch.is_estimation_mode());
    assert!(sketch.lower_bound(NumStdDev::Three) <= sketch.estimate());
    assert!(sketch.upper_bound(NumStdDev::Three) >= sketch.estimate());
    sketch.trim();
    assert_eq!(sketch.num_retained(), 32);

    sketch.reset();
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.theta64(), MAX_THETA);
    assert!(!sketch.is_estimation_mode());
}

#[test]
fn compact_and_view_preserve_entries_and_state() {
    let sketch = sketch_with_range(0, 1_000, 3);
    for ordered in [false, true] {
        let compact = sketch.compact(ordered);
        assert_eq!(compact.is_ordered(), ordered);
        assert_eq!(compact.num_values(), 3);
        assert_eq!(compact.num_retained(), sketch.num_retained());
        assert_eq!(compact.theta64(), sketch.theta64());
        assert_eq!(compact.seed_hash(), sketch.seed_hash());
        assert_eq!(compact.estimate(), sketch.estimate());

        let view = compact.as_view();
        assert_eq!(view.num_values(), 3);
        assert_eq!(view.num_retained(), compact.num_retained());
        assert_eq!(view.estimate(), compact.estimate());
        let mut expected: Vec<_> = sketch
            .iter()
            .map(|entry| (entry.hash(), entry.values().to_vec()))
            .collect();
        expected.sort_unstable_by_key(|entry| entry.0);
        let mut actual: Vec<_> = view
            .iter()
            .map(|entry| (entry.hash(), entry.values().to_vec()))
            .collect();
        actual.sort_unstable_by_key(|entry| entry.0);
        assert_eq!(actual, expected);
    }
}

#[test]
fn compact_round_trip_preserves_values() {
    let sketch = sketch_with_range(0, 100, 3);
    for ordered in [false, true] {
        let bytes = sketch.compact(ordered).serialize();
        let restored = CompactArrayOfDoublesSketch::deserialize(&bytes).unwrap();
        assert_eq!(restored.is_ordered(), ordered);
        assert_eq!(restored.serialize(), bytes);

        let mut expected: Vec<_> = sketch
            .iter()
            .map(|entry| (entry.hash(), entry.values().to_vec()))
            .collect();
        expected.sort_unstable_by_key(|entry| entry.0);
        let mut actual: Vec<_> = restored
            .iter()
            .map(|entry| (entry.hash(), entry.values().to_vec()))
            .collect();
        actual.sort_unstable_by_key(|entry| entry.0);
        assert_eq!(actual, expected);
    }
}

#[test]
fn sampled_non_empty_sketch_without_entries_is_not_empty() {
    let screened_key = (0_u64..)
        .find(|key| {
            let mut sketch = ArrayOfDoublesSketchBuilder::default()
                .sampling_probability(0.01)
                .build()
                .unwrap();
            sketch.update(*key, [1.0]).unwrap();
            !sketch.is_empty() && sketch.num_retained() == 0
        })
        .unwrap();
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .sampling_probability(0.01)
        .build()
        .unwrap();
    sketch.update(screened_key, [1.0]).unwrap();

    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
    assert!(sketch.is_estimation_mode());
    let compact = sketch.compact(true);
    assert!(!compact.is_empty());
    assert_eq!(compact.num_retained(), 0);
    assert_eq!(compact.theta64(), sketch.theta64());

    let restored = CompactArrayOfDoublesSketch::deserialize(&compact.serialize()).unwrap();
    assert!(!restored.is_empty());
    assert_eq!(restored.theta64(), compact.theta64());
    assert_eq!(restored.num_retained(), 0);
}

#[test]
fn estimated_size_accounts_for_heap_buffers_only() {
    let empty_mutable = ArrayOfDoublesSketchBuilder::default()
        .num_values(3)
        .build()
        .unwrap();
    assert!(empty_mutable.estimated_size() > 0);

    let empty_compact = empty_mutable.compact(true);
    assert_eq!(empty_compact.estimated_size(), 0);

    let populated = sketch_with_range(0, 500, 3).compact(true);
    assert_eq!(
        populated.estimated_size(),
        populated.num_retained() * (size_of::<u64>() + 3 * size_of::<f64>())
    );
}
