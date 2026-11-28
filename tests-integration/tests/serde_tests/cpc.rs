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

use std::fs;
use std::path::Path;

use datasketches::cpc::CpcSketch;
use datasketches::cpc::CpcUnion;
use datasketches::cpc::CpcWrapper;
use datasketches::error::ErrorKind;
use datasketches::hash::value::sign_extend;
use googletest::assert_that;
use googletest::prelude::near;

use crate::serialization_test_data;

#[test]
fn test_union_serialize_all_flavors_and_subsequent_merges() {
    const SEED: u64 = 0x5eed_cafe;

    // Covers Empty, Sparse, Hybrid, Pinned (without and with post-window surprises), and Sliding
    // across multiple window offsets and permutation phases.
    let cases: &[(u8, &[u64])] = &[
        (4, &[0, 1, 3, 12, 40, 120, 500]),
        (8, &[0, 10, 60, 180, 1_500, 3_000, 12_000, 50_000]),
        // A Hybrid sketch with table pairs interleaved among rows containing window bits.
        (14, &[4_096]),
    ];

    let mut saw_window_only = false;
    let mut saw_window_and_table = false;
    for &(lg_k, counts) in cases {
        for &num_values in counts {
            let mut union = CpcUnion::with_seed(lg_k, SEED).unwrap();
            if num_values > 0 {
                let split = num_values / 2;
                let mut left = CpcSketch::with_seed(lg_k, SEED).unwrap();
                for value in 0..split {
                    left.update(value);
                }
                let mut right = CpcSketch::with_seed(lg_k, SEED).unwrap();
                for value in split..num_values {
                    right.update(value);
                }
                union.update(&left).unwrap();
                union.update(&right).unwrap();
            }

            let bytes = union.serialize();
            let expected_sketch = union.to_sketch();
            assert_eq!(
                bytes,
                expected_sketch.serialize(),
                "mismatch at lg_k={lg_k}, n={num_values}"
            );

            let flags = bytes[5];
            let has_table = flags & (1 << 3) != 0;
            let has_window = flags & (1 << 4) != 0;
            if has_window && !has_table {
                saw_window_only = true;
            }
            if has_window && has_table {
                saw_window_and_table = true;
            }

            assert_eq!(
                CpcSketch::deserialize(&bytes).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
            let decoded = CpcSketch::deserialize_with_seed(&bytes, SEED).unwrap();
            assert_eq!(decoded.lg_k(), expected_sketch.lg_k());
            assert_eq!(decoded.is_empty(), expected_sketch.is_empty());
            assert_eq!(decoded.estimate(), expected_sketch.estimate());

            // Decoded union image must merge cleanly into downstream unions.
            let mut extra = CpcSketch::with_seed(lg_k, SEED).unwrap();
            for value in num_values..(num_values + 64) {
                extra.update(value);
            }

            let mut downstream_from_bytes = CpcUnion::with_seed(lg_k, SEED).unwrap();
            downstream_from_bytes.update(&decoded).unwrap();
            downstream_from_bytes.update(&extra).unwrap();

            let mut downstream_from_sketch = CpcUnion::with_seed(lg_k, SEED).unwrap();
            downstream_from_sketch.update(&expected_sketch).unwrap();
            downstream_from_sketch.update(&extra).unwrap();

            assert_eq!(
                downstream_from_bytes.serialize(),
                downstream_from_sketch.to_sketch().serialize()
            );
        }
    }

    assert!(
        saw_window_only,
        "expected a pinned bit-matrix case without surprise table"
    );
    assert!(
        saw_window_and_table,
        "expected windowed bit-matrix cases with surprise table"
    );
}

#[test]
fn test_union_serialize_mixed_lg_k_downsampling_across_bit_matrix_flavors() {
    let mut union = CpcUnion::new(11).unwrap();

    // Progress through Sparse -> Hybrid -> Pinned -> Sliding while mixing wider and narrower lg_k
    // inputs so both source folding and bit-matrix downsampling are exercised before serializing.
    for (lg_k, range) in [
        (12_u8, 0_u64..80),
        (10, 40..450),
        (11, 300..900),
        (9, 500..2_200),
        (10, 1_800..4_500),
        (8, 2_000..15_000),
    ] {
        let mut sketch = CpcSketch::new(lg_k).unwrap();
        for value in range {
            sketch.update(value);
        }
        union.update(&sketch).unwrap();

        let bytes = union.serialize();
        let expected = union.to_sketch();
        assert_eq!(bytes, expected.serialize());

        let decoded = CpcSketch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.lg_k(), union.lg_k());
        assert_eq!(decoded.estimate(), expected.estimate());
    }
}

#[test]
fn test_union_serialization_after_updates_and_downsampling() {
    let mut union = CpcUnion::new(12).unwrap();
    // Adopt a sparse input, accumulate overlapping inputs, downsample, fold a larger input,
    // then promote to a bit matrix. Serialize between updates to check continued use.
    for (lg_k, values) in [
        (12, 0..40),
        (12, 20..60),
        (11, 50..100),
        (12, 90..140),
        (10, 100..2_000),
    ] {
        let mut sketch = CpcSketch::new(lg_k).unwrap();
        for value in values.clone() {
            sketch.update(value);
        }
        union.update(&sketch).unwrap();

        // The same coupons must produce the same merged result regardless of stream order.
        let mut reversed = CpcSketch::new(union.lg_k()).unwrap();
        for value in (0..values.end).rev() {
            reversed.update(value);
        }
        let mut reference = CpcUnion::new(union.lg_k()).unwrap();
        reference.update(&reversed).unwrap();

        let bytes = union.serialize();
        assert_eq!(bytes, reference.to_sketch().serialize());
        let decoded = CpcSketch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.lg_k(), union.lg_k());
        assert_eq!(decoded.estimate(), union.to_sketch().estimate());
    }
}

fn test_sketch_file(path: &Path, expected_cardinality: usize) -> CpcSketch {
    let expected = expected_cardinality as f64;

    let bytes = fs::read(path).unwrap();
    let sketch = CpcSketch::deserialize(&bytes).unwrap();
    assert_that!(sketch.estimate(), near(expected, expected * 0.02));

    let serialized_bytes = sketch.serialize();
    let round_trip = CpcSketch::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    assert_eq!(
        bytes,
        serialized_bytes,
        "Serialized bytes differ after round-trip for {}",
        path.display()
    );

    assert_eq!(
        sketch.estimate(),
        round_trip.estimate(),
        "Estimates differ after round-trip for {}",
        path.display()
    );

    sketch
}

fn test_sketch_replay(path: &Path, sketch: CpcSketch, inputs: impl Iterator<Item = usize>) {
    let initial_estimate = sketch.estimate();

    let mut sketch = sketch;
    for value in inputs {
        sketch.update(value);
    }
    assert_eq!(
        initial_estimate,
        sketch.estimate(),
        "Estimate changed after replaying input for {}",
        path.display()
    );
}

fn assert_negative_one_file(dir: &str, filename: &str) -> Vec<u8> {
    let path = serialization_test_data(dir, filename);
    let bytes = fs::read(&path).unwrap();
    let mut sketch = CpcSketch::deserialize(&bytes).unwrap();
    assert_eq!(sketch.lg_k(), 11, "{filename}");
    assert!(!sketch.is_empty(), "{filename}");
    let initial_estimate = sketch.estimate();
    assert_that!(initial_estimate, near(1.0, 0.02));
    assert_eq!(sketch.serialize(), bytes, "{filename}");

    let wrapper = CpcWrapper::new(&bytes).unwrap();
    assert_eq!(wrapper.estimate(), initial_estimate, "{filename}");

    sketch.update(sign_extend::from_i8(-1));
    sketch.update(sign_extend::from_i16(-1));
    sketch.update(sign_extend::from_i32(-1));
    sketch.update(-1_i64);
    assert_eq!(sketch.estimate(), initial_estimate, "{filename}");
    bytes
}

#[test]
fn test_java_compatibility() {
    let test_cases = [0, 100, 200, 2000, 20000];

    for n in test_cases {
        let filename = format!("cpc_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        let sketch = test_sketch_file(&path, n);
        test_sketch_replay(&path, sketch, 0..n);
    }

    let java_neg = assert_negative_one_file("java_generated_files", "cpc_negative_one_java.sk");
    let rust_neg = fs::read(serialization_test_data(
        "rust_generated_files",
        "cpc_negative_one_rust.sk",
    ))
    .unwrap();
    assert_eq!(java_neg, rust_neg);
}

#[test]
fn test_cpp_compatibility() {
    let test_cases = [0, 100, 200, 2000, 20000];

    for n in test_cases {
        let filename = format!("cpc_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        let sketch = test_sketch_file(&path, n);
        test_sketch_replay(&path, sketch, 1..=n);
    }
}

#[test]
fn test_go_compatibility() {
    let test_cases = [0, 100, 200, 2000, 20000];

    for n in test_cases {
        let filename = format!("cpc_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        let sketch = test_sketch_file(&path, n);
        test_sketch_replay(&path, sketch, 0..n);
    }
}

#[test]
fn test_rust_compatibility() {
    for n in [0, 100, 200, 2000, 20000] {
        let filename = format!("cpc_n{n}_rust.sk");
        let path = serialization_test_data("rust_generated_files", &filename);
        let sketch = test_sketch_file(&path, n);
        let wrapper = CpcWrapper::new(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(wrapper.lg_k(), sketch.lg_k());
        assert_eq!(wrapper.is_empty(), sketch.is_empty());
        assert_eq!(wrapper.estimate(), sketch.estimate());
        test_sketch_replay(&path, sketch, 0..n);
    }

    assert_negative_one_file("rust_generated_files", "cpc_negative_one_rust.sk");
}
