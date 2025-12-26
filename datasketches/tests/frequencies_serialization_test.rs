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
use std::path::PathBuf;

use datasketches::frequencies::FrequentItemsSketch;
use datasketches::frequencies::FrequentLongsSketch;
use datasketches::frequencies::StringSerde;

fn maybe_java_data(name: &str) -> Option<PathBuf> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let local = base
        .join("tests/serialization_test_data/java_generated_files")
        .join(name);
    if local.exists() {
        return Some(local);
    }
    let tmp = base
        .parent()
        .unwrap_or(&base)
        .join("tmp_datasketches_java/serialization_test_data/java_generated_files")
        .join(name);
    if tmp.exists() {
        return Some(tmp);
    }
    None
}

#[test]
fn longs_round_trip() {
    let mut sketch = FrequentLongsSketch::new(32);
    for i in 1..=100 {
        sketch.update_with_count(i, i as i64);
    }
    let bytes = sketch.serialize();
    let restored = FrequentLongsSketch::deserialize(&bytes).unwrap();
    assert_eq!(restored.get_total_weight(), sketch.get_total_weight());
    assert_eq!(restored.get_estimate(42), sketch.get_estimate(42));
    assert_eq!(restored.get_maximum_error(), sketch.get_maximum_error());
}

#[test]
fn items_round_trip() {
    let mut sketch = FrequentItemsSketch::new(32);
    sketch.update_with_count("alpha".to_string(), 3);
    sketch.update_with_count("beta".to_string(), 5);
    sketch.update_with_count("gamma".to_string(), 7);

    let serde = StringSerde;
    let bytes = sketch.serialize_with(&serde);
    let restored = FrequentItemsSketch::deserialize_with(&bytes, &serde).unwrap();
    assert_eq!(restored.get_total_weight(), sketch.get_total_weight());
    assert_eq!(restored.get_estimate(&"beta".to_string()), 5);
    assert_eq!(restored.get_maximum_error(), sketch.get_maximum_error());
}

#[test]
fn java_frequent_longs_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];
    for n in test_cases {
        let filename = format!("frequent_long_n{}_java.sk", n);
        let Some(path) = maybe_java_data(&filename) else {
            continue;
        };
        let bytes = fs::read(&path).unwrap();
        let sketch = FrequentLongsSketch::deserialize(&bytes).unwrap();
        assert_eq!(sketch.is_empty(), n == 0);
        if n > 10 {
            assert!(sketch.get_maximum_error() > 0);
        } else {
            assert_eq!(sketch.get_maximum_error(), 0);
        }
        assert_eq!(sketch.get_total_weight(), n as i64);
    }
}

#[test]
fn java_frequent_strings_ascii() {
    let Some(path) = maybe_java_data("frequent_string_ascii_java.sk") else {
        return;
    };
    let bytes = fs::read(&path).unwrap();
    let serde = StringSerde;
    let sketch = FrequentItemsSketch::deserialize_with(&bytes, &serde).unwrap();
    assert!(!sketch.is_empty());
    assert_eq!(sketch.get_maximum_error(), 0);
    assert_eq!(sketch.get_total_weight(), 10);
    assert_eq!(
        sketch.get_estimate(&"aaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
        1
    );
    assert_eq!(
        sketch.get_estimate(&"bbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
        2
    );
    assert_eq!(
        sketch.get_estimate(&"ccccccccccccccccccccccccccccc".to_string()),
        3
    );
    assert_eq!(
        sketch.get_estimate(&"ddddddddddddddddddddddddddddd".to_string()),
        4
    );
}

#[test]
fn java_frequent_strings_utf8() {
    let Some(path) = maybe_java_data("frequent_string_utf8_java.sk") else {
        return;
    };
    let bytes = fs::read(&path).unwrap();
    let serde = StringSerde;
    let sketch = FrequentItemsSketch::deserialize_with(&bytes, &serde).unwrap();
    assert!(!sketch.is_empty());
    assert_eq!(sketch.get_maximum_error(), 0);
    assert_eq!(sketch.get_total_weight(), 28);
    assert_eq!(sketch.get_estimate(&"абвгд".to_string()), 1);
    assert_eq!(sketch.get_estimate(&"еёжзи".to_string()), 2);
    assert_eq!(sketch.get_estimate(&"йклмн".to_string()), 3);
    assert_eq!(sketch.get_estimate(&"опрст".to_string()), 4);
    assert_eq!(sketch.get_estimate(&"уфхцч".to_string()), 5);
    assert_eq!(sketch.get_estimate(&"шщъыь".to_string()), 6);
    assert_eq!(sketch.get_estimate(&"эюя".to_string()), 7);
}
