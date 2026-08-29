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

//! Item codecs for REQ sketch serialization.

use std::mem::size_of;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// Encodes and decodes REQ sketch items.
///
/// A codec is supplied only at serialization boundaries, so an item type may use
/// different codecs without changing its in-memory sketch type. The REQ wire format does not
/// identify the codec; deserialization must use the codec that wrote the image.
pub trait ReqItemCodec<T> {
    /// Returns the number of bytes required to serialize `item`.
    fn serialized_size(&self, item: &T) -> usize;

    /// Serializes `item` into `bytes`.
    fn serialize(&self, item: &T, bytes: &mut SketchBytes);

    /// Deserializes one item from `cursor`.
    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<T, Error>;
}

/// Default little-endian codec for the built-in REQ numeric item types.
///
/// The `f32` encoding is compatible with the Java REQ format. All implementations match the
/// arithmetic item encoding used by the C++ REQ format.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultReqItemCodec;

impl ReqItemCodec<i32> for DefaultReqItemCodec {
    fn serialized_size(&self, _item: &i32) -> usize {
        size_of::<i32>()
    }

    fn serialize(&self, item: &i32, bytes: &mut SketchBytes) {
        bytes.write_i32_le(*item);
    }

    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<i32, Error> {
        cursor
            .read_i32_le()
            .map_err(insufficient_data("failed to read i32 from REQ sketch"))
    }
}

impl ReqItemCodec<i64> for DefaultReqItemCodec {
    fn serialized_size(&self, _item: &i64) -> usize {
        size_of::<i64>()
    }

    fn serialize(&self, item: &i64, bytes: &mut SketchBytes) {
        bytes.write_i64_le(*item);
    }

    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<i64, Error> {
        cursor
            .read_i64_le()
            .map_err(insufficient_data("failed to read i64 from REQ sketch"))
    }
}

impl ReqItemCodec<u32> for DefaultReqItemCodec {
    fn serialized_size(&self, _item: &u32) -> usize {
        size_of::<u32>()
    }

    fn serialize(&self, item: &u32, bytes: &mut SketchBytes) {
        bytes.write_u32_le(*item);
    }

    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<u32, Error> {
        cursor
            .read_u32_le()
            .map_err(insufficient_data("failed to read u32 from REQ sketch"))
    }
}

impl ReqItemCodec<u64> for DefaultReqItemCodec {
    fn serialized_size(&self, _item: &u64) -> usize {
        size_of::<u64>()
    }

    fn serialize(&self, item: &u64, bytes: &mut SketchBytes) {
        bytes.write_u64_le(*item);
    }

    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<u64, Error> {
        cursor
            .read_u64_le()
            .map_err(insufficient_data("failed to read u64 from REQ sketch"))
    }
}

impl ReqItemCodec<f32> for DefaultReqItemCodec {
    fn serialized_size(&self, _item: &f32) -> usize {
        size_of::<f32>()
    }

    fn serialize(&self, item: &f32, bytes: &mut SketchBytes) {
        bytes.write_f32_le(*item);
    }

    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<f32, Error> {
        cursor
            .read_f32_le()
            .map_err(insufficient_data("failed to read f32 from REQ sketch"))
    }
}

impl ReqItemCodec<f64> for DefaultReqItemCodec {
    fn serialized_size(&self, _item: &f64) -> usize {
        size_of::<f64>()
    }

    fn serialize(&self, item: &f64, bytes: &mut SketchBytes) {
        bytes.write_f64_le(*item);
    }

    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<f64, Error> {
        cursor
            .read_f64_le()
            .map_err(insufficient_data("failed to read f64 from REQ sketch"))
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::*;

    fn round_trip<T>(item: T)
    where
        T: Debug + PartialEq,
        DefaultReqItemCodec: ReqItemCodec<T>,
    {
        let codec = DefaultReqItemCodec;
        let mut bytes = SketchBytes::with_capacity(codec.serialized_size(&item));
        codec.serialize(&item, &mut bytes);
        let raw = bytes.into_bytes();
        assert_eq!(raw.len(), codec.serialized_size(&item));
        let decoded = codec.deserialize(&mut SketchSlice::new(&raw)).unwrap();
        assert_eq!(decoded, item);
    }

    #[test]
    fn round_trips_built_in_items() {
        round_trip(i32::MIN);
        round_trip(i64::MAX);
        round_trip(u32::MAX);
        round_trip(u64::MAX);
        round_trip(f32::NEG_INFINITY);
        round_trip(f64::INFINITY);
    }
}
