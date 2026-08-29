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

//! REQ item ordering and serialization policies.

use std::cmp::Ordering;
use std::mem::size_of;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// Defines the ordered domain of items stored in a REQ sketch.
pub trait ReqOrder<T> {
    /// Compares two accepted items.
    fn compare(left: &T, right: &T) -> Ordering;

    /// Returns whether an item belongs to this ordering's domain.
    #[inline(always)]
    fn accepts(_item: &T) -> bool {
        true
    }
}

/// Default REQ ordering for the built-in numeric item types.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultReqOrder;

macro_rules! impl_integer_order {
    ($($type:ty),+ $(,)?) => {$(
        impl ReqOrder<$type> for DefaultReqOrder {
            #[inline(always)]
            fn compare(left: &$type, right: &$type) -> Ordering {
                left.cmp(right)
            }
        }
    )+};
}

impl_integer_order!(i32, i64, u32, u64);

macro_rules! impl_float_order {
    ($($type:ty),+ $(,)?) => {$(
        impl ReqOrder<$type> for DefaultReqOrder {
            #[inline(always)]
            fn compare(left: &$type, right: &$type) -> Ordering {
                left.partial_cmp(right).unwrap()
            }

            #[inline(always)]
            fn accepts(item: &$type) -> bool {
                !item.is_nan()
            }
        }
    )+};
}

impl_float_order!(f32, f64);

/// Encodes and decodes REQ sketch items.
pub trait ReqItemCodec<T> {
    /// Returns the number of bytes required to serialize `item`.
    fn serialized_size(&self, item: &T) -> usize;

    /// Serializes `item` into `bytes`.
    fn serialize(&self, item: &T, bytes: &mut SketchBytes);

    /// Deserializes one item from `cursor`.
    fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<T, Error>;
}

/// Default little-endian codec for the built-in REQ numeric item types.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultReqItemCodec;

macro_rules! impl_item_codec {
    ($type:ty, $read:ident, $write:ident) => {
        impl ReqItemCodec<$type> for DefaultReqItemCodec {
            fn serialized_size(&self, _item: &$type) -> usize {
                size_of::<$type>()
            }

            fn serialize(&self, item: &$type, bytes: &mut SketchBytes) {
                bytes.$write(*item);
            }

            fn deserialize(&self, cursor: &mut SketchSlice<'_>) -> Result<$type, Error> {
                cursor.$read().map_err(insufficient_data(concat!(
                    "failed to read ",
                    stringify!($type),
                    " from REQ sketch"
                )))
            }
        }
    };
}

impl_item_codec!(i32, read_i32_le, write_i32_le);
impl_item_codec!(i64, read_i64_le, write_i64_le);
impl_item_codec!(u32, read_u32_le, write_u32_le);
impl_item_codec!(u64, read_u64_le, write_u64_le);
impl_item_codec!(f32, read_f32_le, write_f32_le);
impl_item_codec!(f64, read_f64_le, write_f64_le);
