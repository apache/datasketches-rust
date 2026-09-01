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

use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Reading(i32);

#[test]
fn custom_items_do_not_need_serialization() {
    let mut sketch = ReqSketch::default();
    sketch.update(Reading(30));
    sketch.update(Reading(10));
    sketch.update(Reading(20));

    assert_eq!(sketch.min_item(), Some(&Reading(10)));
    assert_eq!(sketch.max_item(), Some(&Reading(30)));
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
        Reading(20)
    );
}
