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

use byteorder::{ByteOrder, LE, ReadBytesExt};
use std::io::Cursor;

use crate::error::SerdeError;
use crate::tdigest::{Centroid, TDigest};

const PREAMBLE_LONGS_EMPTY_OR_SINGLE: u8 = 1;
const PREAMBLE_LONGS_MULTIPLE: u8 = 2;
const SERIAL_VERSION: u8 = 1;
const TDIGEST_FAMILY_ID: u8 = 20;
const FLAGS_IS_EMPTY: u8 = 1 << 0;
const FLAGS_IS_SINGLE_VALUE: u8 = 1 << 1;
const FLAGS_REVERSE_MERGE: u8 = 1 << 2;

impl TDigest {
    /// Serializes this TDigest to bytes.
    pub fn serialize(&mut self) -> Vec<u8> {
        self.compress();

        let mut bytes = vec![];
        bytes.push(match self.total_weight() {
            0 => PREAMBLE_LONGS_EMPTY_OR_SINGLE,
            1 => PREAMBLE_LONGS_EMPTY_OR_SINGLE,
            _ => PREAMBLE_LONGS_MULTIPLE,
        });
        bytes.push(SERIAL_VERSION);
        bytes.push(TDIGEST_FAMILY_ID);
        LE::write_u16(&mut bytes, self.k);
        bytes.push({
            let mut flags = 0;
            if self.is_empty() {
                flags |= FLAGS_IS_EMPTY;
            }
            if self.is_single_value() {
                flags |= FLAGS_IS_SINGLE_VALUE;
            }
            if self.reverse_merge {
                flags |= FLAGS_REVERSE_MERGE;
            }
            flags
        });
        LE::write_u16(&mut bytes, 0); // unused
        if self.is_empty() {
            return bytes;
        }
        if self.is_single_value() {
            LE::write_f64(&mut bytes, self.min);
            return bytes;
        }
        LE::write_u32(&mut bytes, self.centroids.len() as u32);
        LE::write_u32(&mut bytes, 0); // unused
        LE::write_f64(&mut bytes, self.min);
        LE::write_f64(&mut bytes, self.max);
        for centroid in &self.centroids {
            LE::write_f64(&mut bytes, centroid.mean);
            LE::write_u64(&mut bytes, centroid.weight);
        }
        bytes
    }

    /// Deserializes a TDigest from bytes.
    ///
    /// Supports reading compact format with (float, int) centroids as opposed to (double, long) to
    /// represent (mean, weight). [^1]
    ///
    /// [^1]: This is to support reading the `tdigest<float>` format from the C++ implementation.
    pub fn deserialize(bytes: &[u8], is_float: bool) -> Result<Self, SerdeError> {
        let make_error = |tag: &'static str| move |_| SerdeError::InsufficientData(tag.to_string());
        let mut cursor = Cursor::new(bytes);

        let preamble_longs = cursor.read_u8().map_err(make_error("preamble_longs"))?;
        let serial_version = cursor.read_u8().map_err(make_error("serial_version"))?;
        let family_id = cursor.read_u8().map_err(make_error("family_id"))?;
        if family_id != TDIGEST_FAMILY_ID {
            // TODO: Support reading format of the reference implementation
            return Err(SerdeError::InvalidFamily(format!(
                "expected {} (TDigest), got {}",
                TDIGEST_FAMILY_ID, family_id
            )));
        }
        if serial_version != SERIAL_VERSION {
            return Err(SerdeError::UnsupportedVersion(format!(
                "expected {}, got {}",
                SERIAL_VERSION, serial_version
            )));
        }
        let k = cursor.read_u16::<LE>().map_err(make_error("k"))?;
        let flags = cursor.read_u8().map_err(make_error("flags"))?;
        let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
        let is_single_value = (flags & FLAGS_IS_SINGLE_VALUE) != 0;
        let expected_preamble_longs = if is_empty || is_single_value {
            PREAMBLE_LONGS_EMPTY_OR_SINGLE
        } else {
            PREAMBLE_LONGS_MULTIPLE
        };
        if preamble_longs != expected_preamble_longs {
            return Err(SerdeError::MalformedData(format!(
                "expected preamble_longs to be {}, got {}",
                expected_preamble_longs, preamble_longs
            )));
        }
        cursor.read_u16::<LE>().map_err(make_error("<unused>"))?; // unused
        if is_empty {
            return Ok(TDigest::new(k));
        }

        let reverse_merge = (flags & FLAGS_REVERSE_MERGE) != 0;
        if is_single_value {
            let value = if is_float {
                cursor
                    .read_f32::<LE>()
                    .map_err(make_error("single_value"))? as f64
            } else {
                cursor
                    .read_f64::<LE>()
                    .map_err(make_error("single_value"))?
            };
            return Ok(TDigest::make(
                k,
                reverse_merge,
                value,
                value,
                vec![Centroid {
                    mean: value,
                    weight: 1,
                }],
                1,
                vec![],
            ));
        }
        let num_centroids = cursor
            .read_u32::<LE>()
            .map_err(make_error("num_centroids"))? as usize;
        let num_buffered = cursor
            .read_u32::<LE>()
            .map_err(make_error("num_buffered"))? as usize;
        let (min, max) = if is_float {
            (
                cursor.read_f32::<LE>().map_err(make_error("min"))? as f64,
                cursor.read_f32::<LE>().map_err(make_error("max"))? as f64,
            )
        } else {
            (
                cursor.read_f64::<LE>().map_err(make_error("min"))?,
                cursor.read_f64::<LE>().map_err(make_error("max"))?,
            )
        };
        let mut centroids = Vec::with_capacity(num_centroids);
        let mut centroids_weight = 0;
        for _ in 0..num_centroids {
            let (mean, weight) = if is_float {
                (
                    cursor.read_f32::<LE>().map_err(make_error("mean"))? as f64,
                    cursor.read_u32::<LE>().map_err(make_error("weight"))? as u64,
                )
            } else {
                (
                    cursor.read_f64::<LE>().map_err(make_error("mean"))?,
                    cursor.read_u64::<LE>().map_err(make_error("weight"))?,
                )
            };
            centroids_weight += weight;
            centroids.push(Centroid { mean, weight });
        }
        let mut buffer = Vec::with_capacity(num_buffered);
        for _ in 0..num_buffered {
            buffer.push(if is_float {
                cursor
                    .read_f32::<LE>()
                    .map_err(make_error("buffered_value"))? as f64
            } else {
                cursor
                    .read_f64::<LE>()
                    .map_err(make_error("buffered_value"))?
            })
        }
        Ok(TDigest::make(
            k,
            reverse_merge,
            min,
            max,
            centroids,
            centroids_weight,
            buffer,
        ))
    }

    fn is_single_value(&self) -> bool {
        self.total_weight() == 1
    }
}
