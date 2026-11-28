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

use datasketches::aod::ArrayOfNumbersCombiner;
use datasketches::aod::ArrayOfNumbersSketchBuilder;
use datasketches::aod::CompactArrayOfNumbersSketch;

#[derive(Debug, Clone, Copy)]
struct SumF32;

impl ArrayOfNumbersCombiner<f32> for SumF32 {
    fn combine(&self, existing: &mut [f32], incoming: &[f32]) {
        for (existing, incoming) in existing.iter_mut().zip(incoming) {
            *existing += incoming;
        }
    }
}

#[test]
fn f32_update_compact_and_round_trip() {
    let mut sketch = ArrayOfNumbersSketchBuilder::<f32>::default()
        .num_values(2)
        .build()
        .unwrap();
    sketch.update("shared", [1.0, 2.0]).unwrap();
    sketch.update("shared", [3.0, 4.0]).unwrap();
    sketch.update("other", [5.0, 6.0]).unwrap();

    assert_eq!(sketch.num_retained(), 2);
    assert_eq!(sketch.estimate(), 2.0);
    assert!(sketch.iter().any(|entry| entry.values() == [4.0, 6.0]));

    for ordered in [false, true] {
        let compact = sketch.compact(ordered);
        assert_eq!(compact.num_values(), 2);
        assert_eq!(compact.num_retained(), 2);
        let bytes = compact.serialize();
        let restored = CompactArrayOfNumbersSketch::<f32>::deserialize(&bytes).unwrap();
        assert_eq!(restored.serialize(), bytes);
        assert_eq!(restored.num_retained(), 2);
        assert!(restored.iter().any(|entry| entry.values() == [4.0, 6.0]));
    }
}

#[test]
fn f32_set_operations_preserve_numeric_summaries() {
    let mut a = ArrayOfNumbersSketchBuilder::<f32>::default()
        .num_values(2)
        .build()
        .unwrap();
    a.update("shared", [1.0, 2.0]).unwrap();
    a.update("only_a", [3.0, 4.0]).unwrap();
    let mut b = ArrayOfNumbersSketchBuilder::<f32>::default()
        .num_values(2)
        .build()
        .unwrap();
    b.update("shared", [10.0, 20.0]).unwrap();
    b.update("only_b", [30.0, 40.0]).unwrap();

    let mut union = datasketches::aod::ArrayOfNumbersUnionBuilder::<f32>::default()
        .num_values(2)
        .build()
        .unwrap();
    union.update(&a).unwrap();
    union.update(&b.compact(true)).unwrap();
    let union_result = union.to_sketch(true);
    assert_eq!(union_result.num_retained(), 3);
    assert!(
        union_result
            .iter()
            .any(|entry| entry.values() == [11.0, 22.0])
    );
    assert_eq!(union.serialize(true), union_result.serialize());

    let mut intersection = datasketches::aod::ArrayOfNumbersIntersection::new(2, SumF32).unwrap();
    intersection.update(&a).unwrap();
    intersection.update(&b).unwrap();
    let intersection_result = intersection.to_sketch(true).unwrap();
    assert_eq!(intersection_result.num_retained(), 1);
    assert_eq!(
        intersection_result.iter().next().unwrap().values(),
        &[11.0, 22.0]
    );

    let difference = datasketches::aod::ArrayOfNumbersANotB::default()
        .compute(&a, &b, true)
        .unwrap();
    assert_eq!(difference.num_retained(), 1);
    assert_eq!(difference.iter().next().unwrap().values(), &[3.0, 4.0]);
}
