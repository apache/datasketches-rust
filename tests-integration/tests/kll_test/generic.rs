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

use datasketches::common::SearchCriteria;
use datasketches::kll::KllComparator;
use datasketches::kll::KllSketch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NumericStringOrder;

impl KllComparator<String> for NumericStringOrder {
    fn compare(&self, left: &String, right: &String) -> Ordering {
        left.parse::<u64>()
            .unwrap()
            .cmp(&right.parse::<u64>().unwrap())
    }

    fn is_compatible(&self, _other: &Self) -> bool {
        true
    }
}

#[test]
fn custom_comparator_controls_queries_and_survives_roundtrip() {
    let mut sketch =
        KllSketch::<String, NumericStringOrder>::new_with_comparator(200, NumericStringOrder)
            .unwrap();
    for item in ["2", "10", "1"] {
        sketch.update(item.to_owned());
    }

    assert_eq!(sketch.min_item().map(String::as_str), Some("1"));
    assert_eq!(sketch.max_item().map(String::as_str), Some("10"));
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
        "2"
    );

    let decoded = KllSketch::<String, NumericStringOrder>::deserialize_with_comparator(
        &sketch.serialize(),
        NumericStringOrder,
    )
    .unwrap();
    assert_eq!(decoded.n(), sketch.n());
    assert_eq!(decoded.num_retained(), sketch.num_retained());
    assert_eq!(decoded.min_item().map(String::as_str), Some("1"));
    assert_eq!(decoded.max_item().map(String::as_str), Some("10"));
    assert_eq!(
        decoded.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
        "2"
    );
}
