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

use super::CollapseDirection;
use super::DdSketch;
use super::Store;
use crate::codec::SketchSlice;
use crate::error::Error;

const HEADER_SIZE: usize = 76;

impl DdSketch {
    /// Serializes the sketch using the Rust-specific version 1 format.
    ///
    /// The little-endian header contains `DDS1`, the bucket limit (`u32`), accuracy (`f64`),
    /// direction (`u8`, lowest = `0`, highest = `1`), positive and negative collapse flags
    /// (`u8` each), a reserved zero byte, total and zero weights (`u64` each), and extrema
    /// (`f64` each). Negative and positive stores each have an offset (`i64`) and length (`u32`).
    /// The payload contains their dense counts (`u64`), negative store first, in index order.
    /// Empty extrema are positive and negative infinity. There is no cross-language format
    /// compatibility contract.
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.serialize_into(&mut out);
        out
    }

    /// Replaces `out` with the version 1 image, reusing its allocated capacity.
    ///
    /// Produces the same bytes as [`Self::serialize`].
    pub fn serialize_into(&self, out: &mut Vec<u8>) {
        out.clear();
        out.reserve(HEADER_SIZE + 8 * (self.negative.counts.len() + self.positive.counts.len()));
        out.extend_from_slice(b"DDS1");
        out.extend_from_slice(&self.max_buckets.to_le_bytes());
        out.extend_from_slice(&self.accuracy.to_le_bytes());
        out.push(match self.direction {
            CollapseDirection::Lowest => 0,
            CollapseDirection::Highest => 1,
        });
        out.push(u8::from(self.positive.collapsed));
        out.push(u8::from(self.negative.collapsed));
        out.push(0);
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.zero_count.to_le_bytes());
        out.extend_from_slice(&self.min.to_le_bytes());
        out.extend_from_slice(&self.max.to_le_bytes());
        for store in [&self.negative, &self.positive] {
            out.extend_from_slice(&store.offset.to_le_bytes());
            out.extend_from_slice(&(store.counts.len() as u32).to_le_bytes());
        }
        for store in [&self.negative, &self.positive] {
            for count in &store.counts {
                out.extend_from_slice(&count.to_le_bytes());
            }
        }
    }

    /// Deserializes a Rust-specific version 1 image.
    ///
    /// Validates histogram structure, counts, extrema, and collapse boundaries, allowing for
    /// floating-point rounding at logarithmic bucket boundaries. The image does
    /// not authenticate the original observations or prove that collapse flags reflect their
    /// history. Unmodified roundtrips preserve collapse state and subsequent update behavior.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` for unsupported versions, truncation, trailing data, invalid
    /// parameters, invalid stores or extrema, and inconsistent or overflowing counts.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        let bad = || Error::deserial("invalid DDSketch image");
        if bytes.len() < HEADER_SIZE || &bytes[..4] != b"DDS1" {
            return Err(bad());
        }
        let mut input = SketchSlice::new(&bytes[4..]);
        let limit = input.read_u32_le().map_err(|_| bad())?;
        let accuracy = input.read_f64_le().map_err(|_| bad())?;
        let direction = match input.read_u8().map_err(|_| bad())? {
            0 => CollapseDirection::Lowest,
            1 => CollapseDirection::Highest,
            _ => return Err(bad()),
        };
        let mut sketch =
            Self::with_collapse_direction(limit, accuracy, direction).map_err(|_| bad())?;
        let positive_collapsed = input.read_u8().map_err(|_| bad())?;
        let negative_collapsed = input.read_u8().map_err(|_| bad())?;
        if positive_collapsed > 1
            || negative_collapsed > 1
            || input.read_u8().map_err(|_| bad())? != 0
        {
            return Err(bad());
        }
        sketch.positive.collapsed = positive_collapsed != 0;
        sketch.negative.collapsed = negative_collapsed != 0;
        sketch.count = input.read_u64_le().map_err(|_| bad())?;
        sketch.zero_count = input.read_u64_le().map_err(|_| bad())?;
        sketch.min = input.read_f64_le().map_err(|_| bad())?;
        sketch.max = input.read_f64_le().map_err(|_| bad())?;
        sketch.negative.offset = input.read_i64_le().map_err(|_| bad())?;
        let negative_len = input.read_u32_le().map_err(|_| bad())?;
        sketch.positive.offset = input.read_i64_le().map_err(|_| bad())?;
        let positive_len = input.read_u32_le().map_err(|_| bad())?;
        // Check payload length before allocating from untrusted bucket counts.
        if negative_len > limit
            || positive_len > limit
            || input.remaining().len() as u64
                != 8 * (u64::from(negative_len) + u64::from(positive_len))
        {
            return Err(bad());
        }
        let mut count = sketch.zero_count;
        for (store, len) in [
            (&mut sketch.negative, negative_len),
            (&mut sketch.positive, positive_len),
        ] {
            for _ in 0..len {
                let weight = input.read_u64_le().map_err(|_| bad())?;
                count = count.checked_add(weight).ok_or_else(bad)?;
                store.counts.push_back(weight);
            }
        }
        if count != sketch.count {
            return Err(bad());
        }
        sketch.validate_image()?;
        Ok(sketch)
    }

    fn validate_image(&self) -> Result<(), Error> {
        let bad = || Error::deserial("inconsistent DDSketch histogram");
        let low = self.mapping.index_bounds(self.zero_threshold()).0;
        let high = self.mapping.index_bounds(f64::MAX).1;
        for (store, negative) in [(&self.negative, true), (&self.positive, false)] {
            if store.counts.is_empty() {
                if store.offset != 0 || store.collapsed {
                    return Err(bad());
                }
                continue;
            }
            let (low, high) = if negative { (-high, -low) } else { (low, high) };
            let last = store.offset.checked_add(store.counts.len() as i64 - 1);
            if store.offset < low
                || last.is_none_or(|last| last > high)
                || store.counts.front() == Some(&0)
                || store.counts.back() == Some(&0)
                || (store.collapsed && store.counts.len() != self.max_buckets as usize)
            {
                return Err(bad());
            }
        }
        if self.is_empty() {
            if self.num_buckets() != 0 || self.min != f64::INFINITY || self.max != f64::NEG_INFINITY
            {
                return Err(bad());
            }
            return Ok(());
        }
        if !self.min.is_finite()
            || !self.max.is_finite()
            || self.min > self.max
            || (self.count == 1 && self.min != self.max)
            || self.min.to_bits() == (-0.0f64).to_bits()
            || self.max.to_bits() == (-0.0f64).to_bits()
        {
            return Err(bad());
        }

        let category = |value: f64| {
            if value <= -self.zero_threshold() {
                -1
            } else if value >= self.zero_threshold() {
                1
            } else {
                0
            }
        };
        let first = if !self.negative.counts.is_empty() {
            -1
        } else if self.zero_count != 0 {
            0
        } else {
            1
        };
        let last = if !self.positive.counts.is_empty() {
            1
        } else if self.zero_count != 0 {
            0
        } else {
            -1
        };
        if category(self.min) != first || category(self.max) != last {
            return Err(bad());
        }
        for (value, minimum) in [(self.min, true), (self.max, false)] {
            if category(value) == 0 {
                continue;
            }
            let (low, high) = self.mapping.index_bounds(value.abs());
            let (store, low, high): (&Store, i64, i64) = if value < 0.0 {
                (&self.negative, -high, -low)
            } else {
                (&self.positive, low, high)
            };
            let expected = if minimum {
                store.offset
            } else {
                store.last_index()
            };
            let retained = |index: i64| {
                if store.collapsed {
                    match self.direction {
                        CollapseDirection::Lowest => index.max(store.offset),
                        CollapseDirection::Highest => index.min(store.last_index()),
                    }
                } else {
                    index
                }
            };
            if !(retained(low)..=retained(high)).contains(&expected) {
                return Err(bad());
            }
        }
        Ok(())
    }
}
