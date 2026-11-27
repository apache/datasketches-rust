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

use std::iter;

use super::QuantilesSketch;
use super::QuantilesValue;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;

const SERIAL_VERSION: u8 = 3;
const EMPTY_PREAMBLE_LONGS: u8 = Family::QUANTILES.min_pre_longs;
const NONEMPTY_PREAMBLE_LONGS: u8 = Family::QUANTILES.max_pre_longs;
const FLAG_READ_ONLY: u8 = 1 << 1;
const FLAG_EMPTY: u8 = 1 << 2;
const FLAG_COMPACT: u8 = 1 << 3;
const FLAG_ORDERED: u8 = 1 << 4;

fn deserialize_item<T: QuantilesValue>(
    input: &mut SketchSlice<'_>,
    compact: bool,
) -> Result<T, Error> {
    if compact {
        return T::deserialize(input);
    }

    let mut slot = SketchSlice::new(&input.remaining()[..size_of::<f64>()]);
    let item = T::deserialize(&mut slot)?;
    if !slot.remaining().is_empty() {
        return Err(Error::deserial(
            "a noncompact item must consume one double slot",
        ));
    }
    input.advance(size_of::<f64>());
    Ok(item)
}

impl<T: QuantilesValue + Ord> QuantilesSketch<T> {
    /// Serializes the sketch in ordered compact version `3` format.
    ///
    /// # Panics
    ///
    /// Panics if a `String` item exceeds `u32::MAX` bytes.
    ///
    /// # Cross-language compatibility
    ///
    /// The serialized data can be read by other DataSketches implementations when the
    /// [item encoding and ordering](QuantilesValue) match.
    /// DataSketches Java requires the total weight to fit in an `i64`.
    pub fn serialize(&self) -> Vec<u8> {
        let (preamble_longs, flags) = if self.is_empty() {
            (
                EMPTY_PREAMBLE_LONGS,
                FLAG_COMPACT | FLAG_ORDERED | FLAG_EMPTY,
            )
        } else {
            (NONEMPTY_PREAMBLE_LONGS, FLAG_COMPACT | FLAG_ORDERED)
        };
        let item_bytes = self
            .min_item()
            .into_iter()
            .chain(self.max_item())
            .chain(self.levels.iter().flatten())
            .map(T::serialized_size)
            .sum::<usize>();
        let mut bytes = SketchBytes::with_capacity(usize::from(preamble_longs) * 8 + item_bytes);
        bytes.write_u8(preamble_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::QUANTILES.id);
        bytes.write_u8(flags);
        bytes.write_u16_le(self.k());
        bytes.write_u16_le(0);

        if let Some((min, max)) = self.min_item().zip(self.max_item()) {
            bytes.write_u64_le(self.total_weight());
            T::serialize(min, &mut bytes);
            T::serialize(max, &mut bytes);

            let mut base: Vec<_> = self.levels[0].iter().collect();
            base.sort_unstable();
            for item in base.into_iter().chain(self.levels[1..].iter().flatten()) {
                T::serialize(item, &mut bytes);
            }
        }
        bytes.into_bytes()
    }

    /// Deserializes a version `3` Classic Quantiles sketch.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the image is malformed or has trailing bytes.
    ///
    /// # Cross-language compatibility
    ///
    /// Also accepts data serialized by other DataSketches implementations when the
    /// [item encoding and ordering](QuantilesValue) match.
    /// Use [`QuantilesFloat<f64>`](super::QuantilesFloat) to read noncompact doubles written by DataSketches Java's `toByteArray()`.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        let mut input = SketchSlice::new(bytes);
        let preamble_longs = input
            .read_u8()
            .map_err(insufficient_data("preamble longs"))?;
        let version = input
            .read_u8()
            .map_err(insufficient_data("serial version"))?;
        let family = input.read_u8().map_err(insufficient_data("family"))?;
        let flags = input.read_u8().map_err(insufficient_data("flags"))?;
        let k = input.read_u16_le().map_err(insufficient_data("k"))?;
        input.read_u16_le().map_err(insufficient_data("unused"))?;

        ensure_serial_version_is(SERIAL_VERSION, version)?;
        Family::QUANTILES.validate_id(family)?;
        if flags & !(FLAG_READ_ONLY | FLAG_EMPTY | FLAG_COMPACT | FLAG_ORDERED) != 0 {
            return Err(Error::deserial(format!(
                "invalid Classic Quantiles flags: {flags:#x}"
            )));
        }
        let compact = flags & FLAG_COMPACT != 0;
        let mut sketch = Self::new(k).map_err(|error| Error::deserial(error.message()))?;
        if flags & FLAG_EMPTY != 0 {
            // Java direct compact can declare two longs for an eight-byte empty sketch.
            ensure_preamble_longs_in(
                &[EMPTY_PREAMBLE_LONGS, NONEMPTY_PREAMBLE_LONGS],
                preamble_longs,
            )?;
        } else {
            ensure_preamble_longs_in(&[NONEMPTY_PREAMBLE_LONGS], preamble_longs)?;
            let total_weight = input
                .read_u64_le()
                .map_err(insufficient_data("total weight"))?;
            if total_weight == 0 {
                return Err(Error::deserial(
                    "a nonempty sketch must have positive total weight",
                ));
            }

            let k = usize::from(k);
            let base_capacity = 2 * k;
            let base_count = (total_weight % base_capacity as u64) as usize;
            let occupied = total_weight / base_capacity as u64;
            let num_upper_levels = (u64::BITS - occupied.leading_zeros()) as usize;
            let payload_bytes = input.remaining().len();
            let base_slots = if compact {
                let num_retained = base_count + occupied.count_ones() as usize * k;
                let min_bytes = (num_retained + 2)
                    .checked_mul(T::MIN_SERIALIZED_SIZE)
                    .ok_or_else(|| Error::deserial("minimum item payload size overflows usize"))?;
                if payload_bytes < min_bytes {
                    return Err(Error::insufficient_data_of(
                        "Classic Quantiles item payload",
                        format_args!("expected at least {min_bytes} bytes, got {payload_bytes}"),
                    ));
                }
                base_count
            } else {
                if total_weight > i64::MAX as u64 {
                    return Err(Error::deserial(
                        "noncompact total weight must fit in an i64",
                    ));
                }
                // Java's base starts at four slots and grows to twice k.
                let slots = if total_weight <= k as u64 {
                    base_count.next_power_of_two().max(4)
                } else {
                    base_capacity
                };
                let expected_bytes = (2 + slots + num_upper_levels * k) * size_of::<f64>();
                if payload_bytes != expected_bytes {
                    return Err(Error::deserial(format!(
                        "invalid noncompact payload size: expected {expected_bytes} bytes, got {payload_bytes}"
                    )));
                }
                slots
            };

            let min = deserialize_item::<T>(&mut input, compact)?;
            let max = deserialize_item::<T>(&mut input, compact)?;
            if min > max {
                return Err(Error::deserial("sketch minimum exceeds its maximum"));
            }

            // Bit i marks a level of k items of weight 2^(i + 1).
            let upper_sizes =
                (0..num_upper_levels).map(|bit| if occupied & (1 << bit) != 0 { k } else { 0 });
            let mut levels = Vec::with_capacity(1 + num_upper_levels);
            for (level, size) in iter::once(base_count).chain(upper_sizes).enumerate() {
                let mut items = Vec::with_capacity(size);
                for index in 0..size {
                    let item = deserialize_item::<T>(&mut input, compact).map_err(|error| {
                        error
                            .with_context("Classic Quantiles level", level)
                            .with_context("item index", index)
                    })?;
                    if item < min || item > max {
                        return Err(Error::deserial(format!(
                            "item outside sketch extrema in level {level}"
                        )));
                    }
                    items.push(item);
                }
                if !compact {
                    let slots = if level == 0 { base_slots } else { k };
                    input.advance((slots - size) * size_of::<f64>());
                }
                let ordered = level > 0 || flags & FLAG_ORDERED != 0;
                if ordered && !items.is_sorted() {
                    return Err(Error::deserial(format!(
                        "items in level {level} must be sorted"
                    )));
                }
                levels.push(items);
            }
            if occupied == 0 && !(levels[0].contains(&min) && levels[0].contains(&max)) {
                return Err(Error::deserial(
                    "an exact sketch must retain both stream extrema",
                ));
            }
            sketch.total_weight = total_weight;
            sketch.min_item = Some(min);
            sketch.max_item = Some(max);
            sketch.levels = levels;
        }
        let trailing = input.remaining().len();
        if trailing != 0 {
            return Err(Error::deserial(format!(
                "expected end of Classic Quantiles image, found {trailing} trailing bytes"
            )));
        }
        Ok(sketch)
    }
}
