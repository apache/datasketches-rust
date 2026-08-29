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
use datasketches::error::Error;
use datasketches::error::ErrorKind;
use datasketches::req::ReqItemCodec;
use datasketches::req::ReqOrder;
use datasketches::req::ReqSketch;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Reading(i32);

struct ReadingOrder;

impl ReqOrder<Reading> for ReadingOrder {
    fn compare(left: &Reading, right: &Reading) -> Ordering {
        left.0.cmp(&right.0)
    }
}

struct ReadingCodec;

impl ReqItemCodec<Reading> for ReadingCodec {
    fn serialized_size(&self, _item: &Reading) -> usize {
        size_of::<i32>()
    }

    fn serialize(&self, item: &Reading, bytes: &mut SketchBytes) {
        bytes.write_i32_le(item.0);
    }

    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<Reading, Error> {
        cursor
            .read_i32_le()
            .map(Reading)
            .map_err(|error| Error::new(ErrorKind::InvalidData, error.to_string()))
    }
}

#[test]
fn custom_item_round_trip() {
    let mut sketch = ReqSketch::<Reading, ReadingOrder>::with_order();
    for value in 0..100 {
        sketch.update(Reading(value));
    }

    let bytes = sketch.serialize_with(&ReadingCodec);
    let restored =
        ReqSketch::<Reading, ReadingOrder>::deserialize_with(&bytes, &ReadingCodec).unwrap();

    assert_eq!(restored.n(), sketch.n());
    assert_eq!(restored.min_item(), sketch.min_item());
    assert_eq!(restored.max_item(), sketch.max_item());
    assert_eq!(restored.serialize_with(&ReadingCodec), bytes);
}
