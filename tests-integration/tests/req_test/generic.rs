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
use std::num::NonZeroU32;

use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::error::Error;
use datasketches::error::ErrorKind;
use datasketches::req::ReqItemCodec;
use datasketches::req::ReqOrder;
use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Reading {
    sequence: u32,
    value: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ByValue;

impl ReqOrder<Reading> for ByValue {
    fn compare(&self, left: &Reading, right: &Reading) -> Ordering {
        left.value.cmp(&right.value)
    }
}

#[test]
fn custom_items_do_not_need_partial_ord_or_a_codec() {
    let mut sketch = ReqSketch::with_order(ByValue);
    sketch.update(Reading {
        sequence: 1,
        value: 30,
    });
    sketch.update(Reading {
        sequence: 2,
        value: 10,
    });
    sketch.update(Reading {
        sequence: 3,
        value: 20,
    });

    assert_eq!(sketch.min_item().unwrap().sequence, 2);
    assert_eq!(sketch.max_item().unwrap().sequence, 1);
    assert_eq!(
        sketch
            .quantile(0.5, SearchCriteria::Inclusive)
            .unwrap()
            .sequence,
        3
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NumericOrder {
    descending: bool,
}

impl ReqOrder<NonZeroU32> for NumericOrder {
    fn compare(&self, left: &NonZeroU32, right: &NonZeroU32) -> Ordering {
        if self.descending {
            right.cmp(left)
        } else {
            left.cmp(right)
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct NonZeroCodec;

impl ReqItemCodec<NonZeroU32> for NonZeroCodec {
    fn serialized_size(&self, _item: &NonZeroU32) -> usize {
        size_of::<u32>()
    }

    fn serialize(&self, item: &NonZeroU32, bytes: &mut SketchBytes) {
        bytes.write_u32_le(item.get());
    }

    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<NonZeroU32, Error> {
        let value = cursor
            .read_u32_le()
            .map_err(|error| Error::new(ErrorKind::InvalidData, error.to_string()))?;
        NonZeroU32::new(value)
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "REQ item must be non-zero"))
    }
}

#[test]
fn custom_codec_round_trips_a_foreign_item_type() {
    let order = NumericOrder { descending: false };
    let codec = NonZeroCodec;
    let mut sketch = ReqSketch::new_with_order(12, Default::default(), order).unwrap();
    for value in 1..=1_000 {
        sketch.update(NonZeroU32::new(value).unwrap());
    }

    let bytes = sketch.serialize_with(&codec);
    let restored = ReqSketch::deserialize_with(&bytes, order, &codec).unwrap();

    assert_eq!(restored.n(), sketch.n());
    assert_eq!(restored.min_item(), sketch.min_item());
    assert_eq!(restored.max_item(), sketch.max_item());
    assert_eq!(restored.serialize_with(&codec), bytes);
}

#[test]
fn merge_rejects_different_stateful_orderings() {
    let mut ascending = ReqSketch::with_order(NumericOrder { descending: false });
    ascending.update(NonZeroU32::new(1).unwrap());

    let mut descending = ReqSketch::with_order(NumericOrder { descending: true });
    descending.update(NonZeroU32::new(2).unwrap());

    let error = ascending.merge(&descending).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}
