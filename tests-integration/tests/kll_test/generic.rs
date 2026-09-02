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

use std::cmp::Ordering;

use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::common::SearchCriteria;
use datasketches::error::Error;
use datasketches::kll::KllSketch;
use datasketches::kll::KllValue;

#[derive(Debug, Clone, PartialEq, Eq)]
struct NumericString(String);

impl Ord for NumericString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .parse::<u64>()
            .unwrap()
            .cmp(&other.0.parse::<u64>().unwrap())
    }
}

impl PartialOrd for NumericString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl KllValue for NumericString {
    const MIN_SERIALIZED_SIZE: usize = String::MIN_SERIALIZED_SIZE;

    fn serialized_size(value: &Self) -> usize {
        String::serialized_size(&value.0)
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        String::serialize(&value.0, bytes);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        String::deserialize(input).map(Self)
    }
}

#[test]
fn custom_item_order_controls_queries_and_survives_roundtrip() {
    let mut sketch = KllSketch::<NumericString>::new(200).unwrap();
    for item in ["2", "10", "1"] {
        sketch.update(NumericString(item.to_owned()));
    }

    assert_eq!(sketch.min_item().map(|item| item.0.as_str()), Some("1"));
    assert_eq!(sketch.max_item().map(|item| item.0.as_str()), Some("10"));
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap().0,
        "2"
    );

    let decoded = KllSketch::<NumericString>::deserialize(&sketch.serialize()).unwrap();
    assert_eq!(decoded.n(), sketch.n());
    assert_eq!(decoded.num_retained(), sketch.num_retained());
    assert_eq!(decoded.min_item().map(|item| item.0.as_str()), Some("1"));
    assert_eq!(decoded.max_item().map(|item| item.0.as_str()), Some("10"));
    assert_eq!(
        decoded.quantile(0.5, SearchCriteria::Inclusive).unwrap().0,
        "2"
    );
}
