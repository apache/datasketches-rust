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
use crate::xor::filter::XorFilter;
use crate::xor::filter::XorFilterType;
use crate::xor::filter::XorFingerprints;

const SERIAL_VERSION: u8 = 1;
const NUM_HASHES: u8 = 3;
const PREAMBLE_BYTES: usize = 3 * size_of::<u64>();

impl XorFilter {
    /// Serializes the filter using DataSketches family `22`, version `1`.
    ///
    /// The format is compatible with Java for 8- and 16-bit fingerprints when input hashing
    /// agrees. Reserved fields are written as zero; integers and fingerprints are little-endian.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) for 32-bit
    /// fingerprints, an unknown item count, or an item count or segment length above `i32::MAX`.
    /// The total number of slots must also fit `i32::MAX` so that Java can index all three
    /// segments; this is an additional interoperability limit, not a header field. Construction
    /// and raw parts allow larger capacities, so successful construction does not guarantee
    /// standard serialization. Use raw parts for other storage formats.
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let size = self.serialized_size()?;
        let num_items = self
            .num_items
            .expect("serialized_size checked the item count");
        let mut bytes = SketchBytes::with_capacity(size);

        bytes.write_u8(Family::XORFILTER.min_pre_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::XORFILTER.id);
        bytes.write_u8(0); // flags
        bytes.write_u8(self.bits_per_fingerprint());
        bytes.write_u8(NUM_HASHES);
        bytes.write_u16_le(0); // unused
        bytes.write_u64_le(self.seed);
        bytes.write_i32_le(self.segment_length as i32);
        bytes.write_i32_le(num_items as i32);

        match &self.fingerprints {
            XorFingerprints::Xor8(fingerprints) => bytes.write(fingerprints),
            XorFingerprints::Xor16(fingerprints) => {
                for &fingerprint in fingerprints.iter() {
                    bytes.write_u16_le(fingerprint);
                }
            }
            XorFingerprints::Xor32(_) => {
                unreachable!("serialized_size rejected 32-bit fingerprints")
            }
        }

        Ok(bytes.into_bytes())
    }

    /// Deserializes an owned filter from bytes.
    ///
    /// Accepts 8- and 16-bit images within the limits of [`serialize`](Self::serialize). Reserved
    /// fields and trailing bytes are ignored.
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
        let filter_type = match bits_per_fingerprint {
            8 => XorFilterType::Xor8,
            16 => XorFilterType::Xor16,
            _ => {
                return Err(Error::deserial(
                    "standard xor format requires 8- or 16-bit fingerprints",
                ));
            }
        };
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
                XorFingerprints::Xor8(values)
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
                XorFingerprints::Xor16(values.into_boxed_slice())
            }
            XorFilterType::Xor32 => unreachable!("header validation rejected 32-bit fingerprints"),
        };

        Ok(Self {
            segment_length,
            num_items: Some(num_items),
            seed,
            fingerprints,
        })
    }

    /// Returns the standard serialized size in bytes.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`serialize`](Self::serialize).
    pub fn serialized_size(&self) -> Result<usize, Error> {
        if self.filter_type() == XorFilterType::Xor32 {
            return Err(Error::invalid_argument(
                "standard xor format does not support 32-bit fingerprints",
            ));
        }
        let count = self
            .num_items
            .ok_or_else(|| Error::invalid_argument("standard xor format requires an item count"))?;
        if count > i32::MAX as usize
            || self.segment_length > i32::MAX as usize
            || self.capacity() > i32::MAX as usize
        {
            return Err(Error::invalid_argument(
                "xor filter exceeds standard format limits",
            ));
        }
        PREAMBLE_BYTES
            .checked_add(self.fingerprints.byte_len())
            .ok_or_else(|| Error::invalid_argument("xor filter serialized size overflow"))
    }
}
