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

use byteorder::{ByteOrder, LE};

use crate::error::SerdeError;
use crate::tdigest::TDigest;

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
    pub fn deserialize(_bytes: &[u8]) -> Result<Self, SerdeError> {
        unimplemented!("Deserialization is not yet implemented");
    }

    fn is_single_value(&self) -> bool {
        self.total_weight() == 1
    }
}
