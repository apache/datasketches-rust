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

//! Serialization helpers for frequent items sketches.

use std::str;

use crate::error::Error;
use crate::frequencies::serialization::read_i64_le;
use crate::frequencies::serialization::read_u32_le;

/// Built-in serializers for frequency sketch items.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemsSerde {
    /// i64 items compatible with ArrayOfLongsSerDe in Java.
    Int64,
    /// UTF-8 strings compatible with ArrayOfStringsSerDe in Java.
    String,
}

pub(crate) fn serialize_string_items(items: &[String]) -> Vec<u8> {
    let total_len: usize = items.iter().map(|item| 4 + item.len()).sum();
    let mut out = Vec::with_capacity(total_len);
    for item in items {
        let bytes = item.as_bytes();
        let len = bytes.len() as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

pub(crate) fn deserialize_string_items(
    bytes: &[u8],
    num_items: usize,
) -> Result<(Vec<String>, usize), Error> {
    if num_items == 0 {
        return Ok((Vec::new(), 0));
    }
    let mut items = Vec::with_capacity(num_items);
    let mut offset = 0usize;
    for _ in 0..num_items {
        if offset + 4 > bytes.len() {
            return Err(Error::insufficient_data(
                "not enough bytes for string length",
            ));
        }
        let len = read_u32_le(bytes, offset) as usize;
        offset += 4;
        if offset + len > bytes.len() {
            return Err(Error::insufficient_data(
                "not enough bytes for string payload",
            ));
        }
        let slice = &bytes[offset..offset + len];
        let value = match str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(_) => {
                return Err(Error::deserial("invalid UTF-8 string payload"));
            }
        };
        items.push(value);
        offset += len;
    }
    Ok((items, offset))
}

pub(crate) fn serialize_i64_items(items: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(items.len() * 8);
    for item in items {
        out.extend_from_slice(&item.to_le_bytes());
    }
    out
}

pub(crate) fn deserialize_i64_items(
    bytes: &[u8],
    num_items: usize,
) -> Result<(Vec<i64>, usize), Error> {
    let needed = num_items
        .checked_mul(8)
        .ok_or_else(|| Error::deserial("items size overflow"))?;
    if bytes.len() < needed {
        return Err(Error::insufficient_data("not enough bytes for i64 items"));
    }
    let mut items = Vec::with_capacity(num_items);
    for i in 0..num_items {
        let offset = i * 8;
        let value = read_i64_le(bytes, offset);
        items.push(value);
    }
    Ok((items, needed))
}
