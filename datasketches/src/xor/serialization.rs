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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in_range;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::xor::filter::Fingerprints;
use crate::xor::filter::XorFilter;
use crate::xor::filter::XorFilterType;

const SERIAL_VERSION: u8 = 1;
const NUM_HASHES: u8 = 3;
const PREAMBLE_BYTES: usize = 3 * size_of::<u64>();

impl XorFilter {
    /// Serializes the filter to a byte vector.
    ///
    /// The format uses Apache DataSketches family ID `22` and is compatible with the corresponding
    /// xor-filter format in other DataSketches implementations.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = SketchBytes::with_capacity(self.serialized_size());

        bytes.write_u8(Family::XORFILTER.min_pre_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::XORFILTER.id);
        bytes.write_u8(0); // flags
        bytes.write_u8(self.bits_per_fingerprint());
        bytes.write_u8(NUM_HASHES);
        bytes.write_u16_le(0); // unused
        bytes.write_u64_le(self.seed);
        bytes.write_i32_le(self.segment_length as i32);
        bytes.write_i32_le(self.num_items as i32);

        match &self.fingerprints {
            Fingerprints::Xor8(fingerprints) => bytes.write(fingerprints),
            Fingerprints::Xor16(fingerprints) => {
                for &fingerprint in fingerprints.iter() {
                    bytes.write_u16_le(fingerprint);
                }
            }
        }

        bytes.into_bytes()
    }

    /// Deserializes an owned filter from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the image is truncated, belongs to another sketch family or version, or
    /// contains metadata that could make a membership query index outside the fingerprint payload.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        let mut cursor = SketchSlice::new(bytes);

        let preamble_longs = cursor
            .read_u8()
            .map_err(insufficient_data("preamble_longs"))?;
        let serial_version = cursor
            .read_u8()
            .map_err(insufficient_data("serial_version"))?;
        let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
        let _flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
        let bits_per_fingerprint = cursor
            .read_u8()
            .map_err(insufficient_data("bits_per_fingerprint"))?;
        let num_hashes = cursor.read_u8().map_err(insufficient_data("num_hashes"))?;
        let _unused = cursor.read_u16_le().map_err(insufficient_data("unused"))?;

        Family::XORFILTER.validate_id(family_id)?;
        ensure_serial_version_is(SERIAL_VERSION, serial_version)?;
        ensure_preamble_longs_in_range(
            Family::XORFILTER.min_pre_longs..=Family::XORFILTER.max_pre_longs,
            preamble_longs,
        )?;
        let filter_type = XorFilterType::from_bits(bits_per_fingerprint)?;
        if num_hashes != NUM_HASHES {
            return Err(Error::deserial(format!(
                "invalid number of hashes: expected {NUM_HASHES}, got {num_hashes}"
            )));
        }

        let seed = cursor.read_u64_le().map_err(insufficient_data("seed"))?;
        let segment_length = cursor
            .read_i32_le()
            .map_err(insufficient_data("segment_length"))?;
        if segment_length <= 0 {
            return Err(Error::deserial(format!(
                "invalid segment length: expected a positive value, got {segment_length}"
            )));
        }
        let segment_length = segment_length as usize;

        let num_items = cursor
            .read_i32_le()
            .map_err(insufficient_data("num_items"))?;
        if num_items < 0 {
            return Err(Error::deserial(format!(
                "invalid item count: expected a non-negative value, got {num_items}"
            )));
        }
        let num_items = num_items as usize;

        let capacity = segment_length
            .checked_mul(usize::from(NUM_HASHES))
            .ok_or_else(|| Error::deserial("xor filter capacity overflow"))?;
        if capacity > i32::MAX as usize {
            return Err(Error::deserial(format!(
                "invalid xor filter capacity: maximum is {}, got {capacity}",
                i32::MAX
            )));
        }
        if num_items > capacity {
            return Err(Error::deserial(format!(
                "invalid item count: capacity is {capacity}, got {num_items}"
            )));
        }

        let fingerprint_bytes = capacity
            .checked_mul(filter_type.bytes_per_fingerprint())
            .ok_or_else(|| Error::deserial("xor filter fingerprint size overflow"))?;
        if cursor.remaining().len() < fingerprint_bytes {
            return Err(Error::insufficient_data_of(
                "fingerprints",
                format!(
                    "expected {fingerprint_bytes} bytes, got {}",
                    cursor.remaining().len()
                ),
            ));
        }

        let fingerprints = match filter_type {
            XorFilterType::Xor8 => {
                let mut values = vec![0_u8; capacity].into_boxed_slice();
                cursor
                    .read_exact(&mut values)
                    .map_err(insufficient_data("fingerprints"))?;
                Fingerprints::Xor8(values)
            }
            XorFilterType::Xor16 => {
                let mut values = Vec::with_capacity(capacity);
                for _ in 0..capacity {
                    values.push(
                        cursor
                            .read_u16_le()
                            .map_err(insufficient_data("fingerprints"))?,
                    );
                }
                Fingerprints::Xor16(values.into_boxed_slice())
            }
        };

        Ok(Self {
            filter_type,
            segment_length,
            num_items,
            seed,
            fingerprints,
        })
    }

    /// Returns the serialized size of the filter in bytes.
    pub fn serialized_size(&self) -> usize {
        PREAMBLE_BYTES + self.fingerprints.byte_len()
    }
}
