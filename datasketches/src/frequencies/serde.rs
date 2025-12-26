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

use crate::error::SerdeError;

/// Serializer/deserializer for items stored in a frequency sketch.
pub trait ItemsSerde<T> {
    /// Serializes a slice of items to a byte buffer.
    fn serialize_items(&self, items: &[T]) -> Vec<u8>;

    /// Deserializes `num_items` from bytes, returning items and bytes consumed.
    fn deserialize_items(&self, bytes: &[u8], num_items: usize) -> Result<(Vec<T>, usize), SerdeError>;
}

/// Serializer for UTF-8 strings compatible with ArrayOfStringsSerDe in Java.
#[derive(Debug, Default, Clone, Copy)]
pub struct StringSerde;

impl ItemsSerde<String> for StringSerde {
    fn serialize_items(&self, items: &[String]) -> Vec<u8> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for item in items {
            let bytes = item.as_bytes();
            let len = bytes.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        out
    }

    fn deserialize_items(&self, bytes: &[u8], num_items: usize) -> Result<(Vec<String>, usize), SerdeError> {
        if num_items == 0 {
            return Ok((Vec::new(), 0));
        }
        let mut items = Vec::with_capacity(num_items);
        let mut offset = 0usize;
        for _ in 0..num_items {
            if offset + 4 > bytes.len() {
                return Err(SerdeError::InsufficientData(
                    "not enough bytes for string length".to_string(),
                ));
            }
            let len = u32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;
            if offset + len > bytes.len() {
                return Err(SerdeError::InsufficientData(
                    "not enough bytes for string payload".to_string(),
                ));
            }
            let slice = &bytes[offset..offset + len];
            let value = match str::from_utf8(slice) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    return Err(SerdeError::MalformedData(
                        "invalid UTF-8 string payload".to_string(),
                    ))
                }
            };
            items.push(value);
            offset += len;
        }
        Ok((items, offset))
    }
}

/// Serializer for i64 items compatible with ArrayOfLongsSerDe in Java.
#[derive(Debug, Default, Clone, Copy)]
pub struct I64Serde;

impl ItemsSerde<i64> for I64Serde {
    fn serialize_items(&self, items: &[i64]) -> Vec<u8> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(items.len() * 8);
        for item in items {
            out.extend_from_slice(&item.to_le_bytes());
        }
        out
    }

    fn deserialize_items(&self, bytes: &[u8], num_items: usize) -> Result<(Vec<i64>, usize), SerdeError> {
        let needed = num_items
            .checked_mul(8)
            .ok_or_else(|| SerdeError::MalformedData("items size overflow".to_string()))?;
        if bytes.len() < needed {
            return Err(SerdeError::InsufficientData(
                "not enough bytes for i64 items".to_string(),
            ));
        }
        let mut items = Vec::with_capacity(num_items);
        for i in 0..num_items {
            let offset = i * 8;
            let value = i64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);
            items.push(value);
        }
        Ok((items, needed))
    }
}
