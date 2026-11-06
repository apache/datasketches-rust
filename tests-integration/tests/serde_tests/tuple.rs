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

use std::fmt::Debug;
use std::fs;
use std::io;
use std::path::PathBuf;

use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::error::Error;
use datasketches::error::ErrorKind;
use datasketches::tuple::CompactTupleSketch;
use datasketches::tuple::DefaultUnionPolicy;
use datasketches::tuple::DefaultUpdatePolicy;
use datasketches::tuple::SummaryCombinePolicy;
use datasketches::tuple::SummaryPolicy;
use datasketches::tuple::SummaryUpdatePolicy;
use datasketches::tuple::TupleSketchBuilder;
use datasketches::tuple::TupleSummaryValue;
use datasketches::tuple::TupleUnionBuilder;
use googletest::assert_that;
use googletest::prelude::each;
use googletest::prelude::eq;
use googletest::prelude::near;
use tests_integration::ZERO_HASH_SEED;

use crate::serialization_test_data;

fn test_sketch_file(path: PathBuf, expected_cardinality: usize) {
    let expected = expected_cardinality as f64;

    let bytes = fs::read(&path).unwrap();
    let sketch1 = CompactTupleSketch::<i32>::deserialize(&bytes)
        .unwrap_or_else(|err| panic!("Deserialization failed for {}: {}", path.display(), err));

    assert_eq!(
        sketch1.is_empty(),
        expected_cardinality == 0,
        "Unexpected is_empty for {}",
        path.display()
    );

    let estimate1 = sketch1.estimate();
    assert_that!(estimate1, near(expected, expected * 0.03));

    // Snapshots from other implementations are not required to match byte-for-byte output from this
    // implementation. Verify our own serialization is stable across a round-trip instead.
    let serialized_bytes = sketch1.serialize();
    let sketch2 = CompactTupleSketch::<i32>::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    let serialized_bytes2 = sketch2.serialize();
    assert_eq!(
        serialized_bytes,
        serialized_bytes2,
        "Serialized bytes are unstable after round-trip for {}",
        path.display()
    );

    let estimate2 = sketch2.estimate();
    assert_eq!(
        estimate1,
        estimate2,
        "Estimates differ after round-trip for {}",
        path.display()
    );
}

#[test]
fn test_java_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in test_cases {
        let filename = format!("tuple_int_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n);
    }
}

#[test]
fn test_cpp_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in test_cases {
        let filename = format!("tuple_int_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n);
    }
}

#[test]
fn test_go_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in test_cases {
        let filename = format!("tuple_int_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_sketch_file(path, n);
    }
}

#[test]
fn empty_sketch_serialization() {
    let expected = fs::read(serialization_test_data(
        "cpp_generated_files",
        "tuple_int_n0_cpp.sk",
    ))
    .unwrap();
    let sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<i32>::default())
        .seed(123)
        .sampling_probability(0.5)
        .build()
        .unwrap()
        .compact(false);
    assert_eq!(sketch.serialize(), expected);

    let other_seed_hash = TupleSketchBuilder::new(DefaultUpdatePolicy::<i32>::default())
        .build()
        .unwrap()
        .seed_hash();
    for stored_seed_hash in [0, other_seed_hash] {
        let mut bytes = expected.clone();
        bytes[6..8].copy_from_slice(&stored_seed_hash.to_le_bytes());
        let restored = CompactTupleSketch::<i32>::deserialize_with_seed(&bytes, 123).unwrap();
        assert!(restored.is_empty());
        assert_eq!(restored.seed_hash(), sketch.seed_hash());
    }
}

#[test]
fn non_empty_images_without_entries_preserve_seed_hash() {
    let mut sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<i32>::default())
        .seed(123)
        .sampling_probability(1e-12)
        .build()
        .unwrap();
    sketch.update("apple", 1);
    let compact = sketch.compact(true);
    assert!(!compact.is_empty());
    assert_eq!(compact.num_retained(), 0);

    let bytes = compact.serialize();
    let restored = CompactTupleSketch::<i32>::deserialize_with_seed(&bytes, 123).unwrap();
    assert!(!restored.is_empty());
    assert_eq!(restored.theta64(), compact.theta64());
    let err = CompactTupleSketch::<i32>::deserialize(&bytes).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn round_trip_preserves_summaries() {
    let mut sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .build()
        .unwrap();
    for value in 0..50 {
        sketch.update(value, 1);
        sketch.update(value, 2);
    }

    let restored =
        CompactTupleSketch::<u64>::deserialize(&sketch.compact(true).serialize()).unwrap();

    assert_eq!(restored.num_retained(), 50);
    let summaries: Vec<_> = restored.iter().map(|entry| *entry.summary()).collect();
    assert_that!(summaries, each(eq(&3)));
}

#[test]
fn malformed_input_is_rejected() {
    let mut sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .build()
        .unwrap();
    for value in 0..100 {
        sketch.update(value, 1);
    }
    let bytes = sketch.compact(true).serialize();

    let truncated = &bytes[..bytes.len() - 4];
    let err = CompactTupleSketch::<u64>::deserialize(truncated).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let err = CompactTupleSketch::<u64>::deserialize_with_seed(&bytes, 8).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let err = CompactTupleSketch::<u64>::deserialize_with_seed(&bytes, ZERO_HASH_SEED).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let mut wrong_family = bytes;
    wrong_family[2] = 0;
    let err = CompactTupleSketch::<u64>::deserialize(&wrong_family).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn declared_entry_payload_is_checked_before_allocating() {
    let mut sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .build()
        .unwrap();
    for value in 0..100 {
        sketch.update(value, 1);
    }
    let mut bytes = sketch.compact(true).serialize();
    assert!(bytes[0] > 1);
    bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());

    assert!(CompactTupleSketch::<u64>::deserialize(&bytes).is_err());
}

/// Asserts that two serialized compact Tuple sketches hold the same result. Unordered images may
/// list the same entries in a different order.
fn assert_same_compact_result<S>(actual: &[u8], expected: &[u8])
where
    S: TupleSummaryValue + PartialEq + Debug,
{
    let actual = CompactTupleSketch::<S>::deserialize(actual).unwrap();
    let expected = CompactTupleSketch::<S>::deserialize(expected).unwrap();
    assert_eq!(actual.is_empty(), expected.is_empty());
    assert_eq!(actual.is_ordered(), expected.is_ordered());
    assert_eq!(actual.theta64(), expected.theta64());
    assert_eq!(actual.seed_hash(), expected.seed_hash());

    fn sorted_entries<S>(sketch: &CompactTupleSketch<S>) -> Vec<(u64, &S)> {
        let mut entries: Vec<_> = sketch
            .iter()
            .map(|entry| (entry.hash(), entry.summary()))
            .collect();
        entries.sort_unstable_by_key(|(hash, _)| *hash);
        entries
    }
    assert_eq!(sorted_entries(&actual), sorted_entries(&expected));
}

#[test]
fn union_serialization_matches_compact_result() {
    let union_builder = || TupleUnionBuilder::new(DefaultUnionPolicy::<u64>::default());
    let empty = union_builder().build().unwrap();
    for ordered in [true, false] {
        assert_eq!(
            empty.serialize(ordered),
            empty.to_sketch(ordered).serialize()
        );
    }

    // Covers exact mode with zero, one and many entries, trimming to nominal capacity, a sampled
    // union, and a non-empty result whose entries were all screened by theta.
    for (lg_k, sampling_probability, count) in [
        (5, 1.0, 0),
        (5, 1.0, 1),
        (5, 1.0, 20),
        (5, 1.0, 10_000),
        (8, 0.5, 10_000),
        (5, 1e-12, 10),
    ] {
        let mut a = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
            .build()
            .unwrap();
        let mut b = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
            .build()
            .unwrap();
        for value in 0..count {
            a.update(value, 1u64);
            b.update(value + count / 2, 2u64);
        }
        let mut union = union_builder()
            .lg_k(lg_k)
            .sampling_probability(sampling_probability)
            .build()
            .unwrap();
        union.update(&a).unwrap();
        union.update(&b).unwrap();

        for ordered in [true, false] {
            let bytes = union.serialize(ordered);
            let expected = union.to_sketch(ordered).serialize();
            if ordered {
                assert_eq!(bytes, expected);
            }
            assert_same_compact_result::<u64>(&bytes, &expected);
        }
    }
}

/// Variable-size, heap-backed summary: the tags recorded for a key.
#[derive(Debug, Default, Clone, PartialEq)]
struct Tags(Vec<String>);

impl TupleSummaryValue for Tags {
    fn serialize_size(&self) -> usize {
        4 + self.0.iter().map(|tag| 4 + tag.len()).sum::<usize>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_u32_le(self.0.len() as u32);
        for tag in &self.0 {
            bytes.write_u32_le(tag.len() as u32);
            bytes.write(tag.as_bytes());
        }
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let malformed = |_: io::Error| Error::new(ErrorKind::InvalidData, "malformed tags");
        let count = cursor.read_u32_le().map_err(malformed)?;
        let mut tags = Vec::new();
        for _ in 0..count {
            let mut tag = vec![0; cursor.read_u32_le().map_err(malformed)? as usize];
            cursor.read_exact(&mut tag).map_err(malformed)?;
            tags.push(
                String::from_utf8(tag)
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "tag is not UTF-8"))?,
            );
        }
        Ok(Tags(tags))
    }
}

#[derive(Debug, Clone, Copy)]
struct TagsPolicy;

impl SummaryPolicy for TagsPolicy {
    type Summary = Tags;

    fn create(&self) -> Self::Summary {
        Tags::default()
    }
}

impl SummaryUpdatePolicy<&str> for TagsPolicy {
    fn update(&self, summary: &mut Self::Summary, value: &str) {
        summary.0.push(value.to_string());
    }
}

impl SummaryCombinePolicy for TagsPolicy {
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
        summary.0.extend(other.0.iter().cloned());
    }
}

#[test]
fn union_serialization_with_heap_backed_summaries_supports_later_merges() {
    let sketch = |values: std::ops::Range<u64>, tag: &str| {
        let mut sketch = TupleSketchBuilder::new(TagsPolicy).lg_k(5).build().unwrap();
        for value in values {
            sketch.update(value, tag);
        }
        sketch
    };
    let union_builder = || TupleUnionBuilder::new(TagsPolicy).lg_k(5);

    let mut union = union_builder().build().unwrap();
    union.update(&sketch(0..1_000, "first")).unwrap();
    union
        .update(&sketch(500..1_500, "a longer second tag"))
        .unwrap();
    let later = sketch(1_000..2_000, "third");

    for ordered in [true, false] {
        let bytes = union.serialize(ordered);
        let expected = union.to_sketch(ordered).serialize();
        assert_same_compact_result::<Tags>(&bytes, &expected);

        let restored = CompactTupleSketch::<Tags>::deserialize(&bytes).unwrap();
        let mut from_bytes = union_builder().build().unwrap();
        from_bytes.update(&restored).unwrap();
        from_bytes.update(&later).unwrap();

        let mut from_sketch = union_builder().build().unwrap();
        from_sketch.update(&union.to_sketch(ordered)).unwrap();
        from_sketch.update(&later).unwrap();

        assert_eq!(from_bytes.serialize(true), from_sketch.serialize(true));
    }
}
