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

//! REQ item serialization.

use std::mem::size_of;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// Serialization support for values stored in a REQ sketch.
pub trait ReqValue: Sized {
    /// Returns the serialized size of `item` in bytes.
    fn serialize_size(item: &Self) -> usize;

    /// Serializes this value into `bytes`.
    fn serialize_value(&self, bytes: &mut SketchBytes);

    /// Deserializes one value from `cursor`.
    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

impl ReqValue for i32 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_i32_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_i32_le()
            .map_err(insufficient_data("failed to read i32 from REQ sketch"))
    }
}

impl ReqValue for i64 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_i64_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_i64_le()
            .map_err(insufficient_data("failed to read i64 from REQ sketch"))
    }
}

impl ReqValue for u32 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_u32_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_u32_le()
            .map_err(insufficient_data("failed to read u32 from REQ sketch"))
    }
}

impl ReqValue for u64 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_u64_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_u64_le()
            .map_err(insufficient_data("failed to read u64 from REQ sketch"))
    }
}

impl ReqValue for f32 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_f32_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_f32_le()
            .map_err(insufficient_data("failed to read f32 from REQ sketch"))
    }
}

impl ReqValue for f64 {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<Self>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(*self);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor
            .read_f64_le()
            .map_err(insufficient_data("failed to read f64 from REQ sketch"))
    }
}
