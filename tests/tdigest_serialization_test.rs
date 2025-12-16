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

use datasketches::tdigest::TDigest;

#[test]
fn test_empty() {
    let mut td = TDigest::new(100);
    assert!(td.is_empty());

    let bytes = td.serialize();
    assert_eq!(bytes.len(), 8);

    let deserialized_td = TDigest::deserialize(&bytes, false).unwrap();
    assert_eq!(td.k(), deserialized_td.k());
    assert_eq!(td.total_weight(), deserialized_td.total_weight());
    assert!(td.is_empty());
    assert!(deserialized_td.is_empty());
}

#[test]
fn test_single_value() {
    let mut td = TDigest::default();
    td.update(123.0);

    let bytes = td.serialize();
    assert_eq!(bytes.len(), 16);

    let deserialized_td = TDigest::deserialize(&bytes, false).unwrap();
    assert_eq!(deserialized_td.k(), 200);
    assert_eq!(deserialized_td.total_weight(), 1);
    assert!(!deserialized_td.is_empty());
    assert_eq!(deserialized_td.min_value(), Some(123.0));
    assert_eq!(deserialized_td.max_value(), Some(123.0));
}

#[test]
fn test_many_values() {
    let mut td = TDigest::new(100);
    for i in 0..1000 {
        td.update(i as f64);
    }

    let bytes = td.serialize();
    assert_eq!(bytes.len(), 1584);

    let mut deserialized_td = TDigest::deserialize(&bytes, false).unwrap();
    assert_eq!(td.k(), deserialized_td.k());
    assert_eq!(td.total_weight(), deserialized_td.total_weight());
    assert_eq!(td.is_empty(), deserialized_td.is_empty());
    assert_eq!(td.min_value(), deserialized_td.min_value());
    assert_eq!(td.max_value(), deserialized_td.max_value());
    assert_eq!(td.rank(500.0), deserialized_td.rank(500.0));
    assert_eq!(td.quantile(0.5), deserialized_td.quantile(0.5));
}
