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

use datasketches::frequencies::ErrorType;
use datasketches::frequencies::FrequentItemsSketch;
use datasketches::frequencies::FrequentLongsSketch;

#[test]
fn longs_purge_keeps_heavy_hitters() {
    let mut sketch = FrequentLongsSketch::new(8);
    sketch.update_with_count(1, 10);
    for item in 2..=7 {
        sketch.update(item);
    }

    assert_eq!(sketch.get_total_weight(), 16);
    assert_eq!(sketch.get_maximum_error(), 1);
    assert_eq!(sketch.get_estimate(1), 10);
    assert_eq!(sketch.get_lower_bound(1), 9);

    let rows = sketch.get_frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 1);
    assert_eq!(*rows[0].item(), 1);
    assert_eq!(rows[0].estimate(), 10);
}

#[test]
fn items_purge_keeps_heavy_hitters() {
    let mut sketch = FrequentItemsSketch::new(8);
    sketch.update_with_count("a".to_string(), 10);
    for item in ["b", "c", "d", "e", "f", "g"] {
        sketch.update(item.to_string());
    }

    assert_eq!(sketch.get_total_weight(), 16);
    assert_eq!(sketch.get_maximum_error(), 1);
    assert_eq!(sketch.get_estimate(&"a".to_string()), 10);
    assert_eq!(sketch.get_lower_bound(&"a".to_string()), 9);

    let rows = sketch.get_frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item(), "a");
    assert_eq!(rows[0].estimate(), 10);
}

#[test]
#[should_panic(expected = "count may not be negative")]
fn longs_negative_count_panics() {
    let mut sketch = FrequentLongsSketch::new(8);
    sketch.update_with_count(1, -1);
}
