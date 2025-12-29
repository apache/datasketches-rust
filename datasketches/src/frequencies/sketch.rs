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

//! Frequent items sketch implementations.

use std::hash::Hash;

use crate::error::SerdeError;
use crate::frequencies::reverse_purge_item_hash_map::ReversePurgeItemHashMap;
use crate::frequencies::serde::ItemsSerde;
use crate::frequencies::serde::deserialize_i64_items;
use crate::frequencies::serde::deserialize_string_items;
use crate::frequencies::serde::serialize_i64_items;
use crate::frequencies::serde::serialize_string_items;
use crate::frequencies::serialization::*;

type DeserializeItems<T> = fn(&[u8], usize) -> Result<(Vec<T>, usize), SerdeError>;

const LG_MIN_MAP_SIZE: u8 = 3;
const SAMPLE_SIZE: usize = 1024;
const EPSILON_FACTOR: f64 = 3.5;
const LOAD_FACTOR_NUMERATOR: usize = 3;
const LOAD_FACTOR_DENOMINATOR: usize = 4;

/// Error guarantees for frequent item queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    /// Include items if upper bound exceeds threshold (no false negatives).
    NoFalseNegatives,
    /// Include items if lower bound exceeds threshold (no false positives).
    NoFalsePositives,
}

/// Result row for frequent item queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row<T> {
    item: T,
    estimate: i64,
    upper_bound: i64,
    lower_bound: i64,
}

impl<T> Row<T> {
    /// Returns the item value.
    pub fn item(&self) -> &T {
        &self.item
    }

    /// Returns the estimated frequency.
    pub fn estimate(&self) -> i64 {
        self.estimate
    }

    /// Returns the upper bound for the frequency.
    pub fn upper_bound(&self) -> i64 {
        self.upper_bound
    }

    /// Returns the lower bound for the frequency.
    pub fn lower_bound(&self) -> i64 {
        self.lower_bound
    }
}

/// Frequent items sketch for generic item types.
///
/// See [`crate::frequencies`] for an overview and error guarantees.
#[derive(Debug, Clone)]
pub struct FrequentItemsSketch<T> {
    lg_max_map_size: u8,
    cur_map_cap: usize,
    offset: i64,
    stream_weight: i64,
    sample_size: usize,
    hash_map: ReversePurgeItemHashMap<T>,
}

impl<T: Eq + Hash> FrequentItemsSketch<T> {
    /// Creates a new sketch with the given maximum map size (power of two).
    ///
    /// # Panics
    ///
    /// Panics if `max_map_size` is not a power of two.
    pub fn new(max_map_size: usize) -> Self {
        let lg_max_map_size = exact_log2(max_map_size);
        Self::with_lg_map_sizes(lg_max_map_size, LG_MIN_MAP_SIZE)
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.hash_map.get_num_active() == 0
    }

    /// Returns the number of active items being tracked.
    pub fn num_active_items(&self) -> usize {
        self.hash_map.get_num_active()
    }

    /// Returns the total weight of the stream.
    pub fn total_weight(&self) -> i64 {
        self.stream_weight
    }

    /// Returns the estimated frequency for an item.
    pub fn estimate(&self, item: &T) -> i64 {
        let value = self.hash_map.get(item);
        if value > 0 { value + self.offset } else { 0 }
    }

    /// Returns the lower bound for an item's frequency.
    pub fn lower_bound(&self, item: &T) -> i64 {
        self.hash_map.get(item)
    }

    /// Returns the upper bound for an item's frequency.
    pub fn upper_bound(&self, item: &T) -> i64 {
        self.hash_map.get(item) + self.offset
    }

    /// Returns an upper bound on the maximum error of [`FrequentItemsSketch::estimate`]
    /// for any item.
    ///
    /// This is equivalent to the maximum distance between the upper bound and the lower bound
    /// for any item.
    pub fn maximum_error(&self) -> i64 {
        self.offset
    }

    /// Returns epsilon for this sketch.
    pub fn epsilon(&self) -> f64 {
        Self::epsilon_for_lg(self.lg_max_map_size)
    }

    /// Returns epsilon for a sketch configured with `lg_max_map_size`.
    pub fn epsilon_for_lg(lg_max_map_size: u8) -> f64 {
        EPSILON_FACTOR / (1u64 << lg_max_map_size) as f64
    }

    /// Returns the a priori error estimate.
    pub fn apriori_error(lg_max_map_size: u8, estimated_total_weight: i64) -> f64 {
        Self::epsilon_for_lg(lg_max_map_size) * estimated_total_weight as f64
    }

    /// Returns the maximum map capacity for this sketch.
    pub fn maximum_map_capacity(&self) -> usize {
        (1usize << self.lg_max_map_size) * LOAD_FACTOR_NUMERATOR / LOAD_FACTOR_DENOMINATOR
    }

    /// Returns the current map capacity.
    pub fn current_map_capacity(&self) -> usize {
        self.cur_map_cap
    }

    /// Returns the configured lg_max_map_size.
    pub fn lg_max_map_size(&self) -> u8 {
        self.lg_max_map_size
    }

    /// Returns the current map size in log2.
    pub fn lg_cur_map_size(&self) -> u8 {
        self.hash_map.get_lg_length()
    }

    /// Updates the sketch with a count of one.
    pub fn update(&mut self, item: T) {
        self.update_with_count(item, 1);
    }

    /// Updates the sketch with an item and count.
    ///
    /// # Panics
    ///
    /// Panics if `count` is negative.
    pub fn update_with_count(&mut self, item: T, count: i64) {
        if count == 0 {
            return;
        }
        assert!(count > 0, "count may not be negative");
        self.stream_weight += count;
        self.hash_map.adjust_or_put_value(item, count);
        self.maybe_resize_or_purge();
    }

    /// Merges another sketch into this one.
    pub fn merge(&mut self, other: &Self)
    where
        T: Clone,
    {
        if other.is_empty() {
            return;
        }
        let merged_total = self.stream_weight + other.stream_weight;
        for (item, count) in other.hash_map.iter() {
            self.update_with_count(item.clone(), count);
        }
        self.offset += other.offset;
        self.stream_weight = merged_total;
    }

    /// Resets the sketch to an empty state.
    pub fn reset(&mut self) {
        *self = Self::with_lg_map_sizes(self.lg_max_map_size, LG_MIN_MAP_SIZE);
    }

    /// Returns frequent items using the sketch maximum error as threshold.
    pub fn frequent_items(&self, error_type: ErrorType) -> Vec<Row<T>>
    where
        T: Clone,
    {
        self.frequent_items_with_threshold(error_type, self.offset)
    }

    /// Returns frequent items using a custom threshold.
    pub fn frequent_items_with_threshold(
        &self,
        error_type: ErrorType,
        threshold: i64,
    ) -> Vec<Row<T>>
    where
        T: Clone,
    {
        let threshold = threshold.max(self.offset);
        let mut rows = Vec::new();
        for (item, count) in self.hash_map.iter() {
            let lower = count;
            let upper = count + self.offset;
            let include = match error_type {
                ErrorType::NoFalseNegatives => upper > threshold,
                ErrorType::NoFalsePositives => lower > threshold,
            };
            if include {
                rows.push(Row {
                    item: item.clone(),
                    estimate: upper,
                    upper_bound: upper,
                    lower_bound: lower,
                });
            }
        }
        rows.sort_by(|a, b| b.estimate.cmp(&a.estimate));
        rows
    }

    fn maybe_resize_or_purge(&mut self) {
        if self.hash_map.get_num_active() > self.cur_map_cap {
            if self.hash_map.get_lg_length() < self.lg_max_map_size {
                self.hash_map.resize(self.hash_map.get_length() * 2);
                self.cur_map_cap = self.hash_map.get_capacity();
            } else {
                let delta = self.hash_map.purge(self.sample_size);
                self.offset += delta;
                if self.hash_map.get_num_active() > self.maximum_map_capacity() {
                    panic!("purge did not reduce number of active items");
                }
            }
        }
    }

    fn with_lg_map_sizes(lg_max_map_size: u8, lg_cur_map_size: u8) -> Self {
        let lg_max = lg_max_map_size.max(LG_MIN_MAP_SIZE);
        let lg_cur = lg_cur_map_size.max(LG_MIN_MAP_SIZE);
        assert!(
            lg_cur <= lg_max,
            "lg_cur_map_size must not exceed lg_max_map_size"
        );
        let map = ReversePurgeItemHashMap::new(1usize << lg_cur);
        let cur_map_cap = map.get_capacity();
        let max_map_cap = (1usize << lg_max) * LOAD_FACTOR_NUMERATOR / LOAD_FACTOR_DENOMINATOR;
        let sample_size = SAMPLE_SIZE.min(max_map_cap);
        Self {
            lg_max_map_size: lg_max,
            cur_map_cap,
            offset: 0,
            stream_weight: 0,
            sample_size,
            hash_map: map,
        }
    }

    fn serialize_inner(&self, serialize_items: fn(&[T]) -> Vec<u8>) -> Vec<u8>
    where
        T: Clone,
    {
        if self.is_empty() {
            let mut out = vec![0u8; 8];
            out[PREAMBLE_LONGS_BYTE] = PREAMBLE_LONGS_EMPTY;
            out[SER_VER_BYTE] = SER_VER;
            out[FAMILY_BYTE] = FAMILY_ID;
            out[LG_MAX_MAP_SIZE_BYTE] = self.lg_max_map_size;
            out[LG_CUR_MAP_SIZE_BYTE] = self.hash_map.get_lg_length();
            out[FLAGS_BYTE] = EMPTY_FLAG_MASK;
            return out;
        }
        let active_items = self.num_active_items();
        let values = self.hash_map.get_active_values();
        let keys = self.hash_map.get_active_keys();
        let items_bytes = serialize_items(&keys);
        let total_bytes =
            PREAMBLE_LONGS_NONEMPTY as usize * 8 + (active_items * 8) + items_bytes.len();
        let mut out = vec![0u8; total_bytes];
        out[PREAMBLE_LONGS_BYTE] = PREAMBLE_LONGS_NONEMPTY;
        out[SER_VER_BYTE] = SER_VER;
        out[FAMILY_BYTE] = FAMILY_ID;
        out[LG_MAX_MAP_SIZE_BYTE] = self.lg_max_map_size;
        out[LG_CUR_MAP_SIZE_BYTE] = self.hash_map.get_lg_length();
        out[FLAGS_BYTE] = 0;
        write_u32_le(&mut out, ACTIVE_ITEMS_INT, active_items as u32);
        write_i64_le(&mut out, STREAM_WEIGHT_LONG, self.stream_weight);
        write_i64_le(&mut out, OFFSET_LONG, self.offset);

        let mut offset = PREAMBLE_LONGS_NONEMPTY as usize * 8;
        for value in values {
            write_i64_le(&mut out, offset, value);
            offset += 8;
        }
        out[offset..offset + items_bytes.len()].copy_from_slice(&items_bytes);
        out
    }

    fn deserialize_inner(
        bytes: &[u8],
        deserialize_items: DeserializeItems<T>,
    ) -> Result<Self, SerdeError> {
        if bytes.len() < 8 {
            return Err(SerdeError::InsufficientData(
                "insufficient data for preamble".to_string(),
            ));
        }
        let pre_longs = bytes[PREAMBLE_LONGS_BYTE] & 0x3f;
        let ser_ver = bytes[SER_VER_BYTE];
        let family = bytes[FAMILY_BYTE];
        let lg_max = bytes[LG_MAX_MAP_SIZE_BYTE];
        let lg_cur = bytes[LG_CUR_MAP_SIZE_BYTE];
        let flags = bytes[FLAGS_BYTE];
        let is_empty = (flags & EMPTY_FLAG_MASK) != 0;
        if ser_ver != SER_VER {
            return Err(SerdeError::UnsupportedVersion(format!(
                "unsupported ser_ver {}",
                ser_ver
            )));
        }
        if family != FAMILY_ID {
            return Err(SerdeError::InvalidFamily(format!(
                "expected family {}, got {}",
                FAMILY_ID, family
            )));
        }
        if lg_cur > lg_max {
            return Err(SerdeError::InvalidParameter(
                "lg_cur_map_size exceeds lg_max_map_size".to_string(),
            ));
        }
        if is_empty {
            if pre_longs != PREAMBLE_LONGS_EMPTY {
                return Err(SerdeError::MalformedData(
                    "empty sketch with invalid preamble size".to_string(),
                ));
            }
            return Ok(Self::with_lg_map_sizes(lg_max, lg_cur));
        }
        if pre_longs != PREAMBLE_LONGS_NONEMPTY {
            return Err(SerdeError::MalformedData(
                "non-empty sketch with invalid preamble size".to_string(),
            ));
        }
        if bytes.len() < PREAMBLE_LONGS_NONEMPTY as usize * 8 {
            return Err(SerdeError::InsufficientData(
                "insufficient data for full preamble".to_string(),
            ));
        }
        let active_items = read_u32_le(bytes, ACTIVE_ITEMS_INT) as usize;
        let stream_weight = read_i64_le(bytes, STREAM_WEIGHT_LONG);
        let offset_val = read_i64_le(bytes, OFFSET_LONG);
        let values_offset = PREAMBLE_LONGS_NONEMPTY as usize * 8;
        let values_bytes = active_items
            .checked_mul(8)
            .ok_or_else(|| SerdeError::MalformedData("values size overflow".to_string()))?;
        let items_offset = values_offset + values_bytes;
        if bytes.len() < items_offset {
            return Err(SerdeError::InsufficientData(
                "insufficient data for values".to_string(),
            ));
        }
        let mut values = Vec::with_capacity(active_items);
        for i in 0..active_items {
            values.push(read_i64_le(bytes, values_offset + i * 8));
        }
        let (items, consumed) = deserialize_items(&bytes[items_offset..], active_items)?;
        if items.len() != active_items {
            return Err(SerdeError::MalformedData(
                "item count mismatch during deserialization".to_string(),
            ));
        }
        if consumed > bytes.len() - items_offset {
            return Err(SerdeError::InsufficientData(
                "insufficient data for items".to_string(),
            ));
        }
        let mut sketch = Self::with_lg_map_sizes(lg_max, lg_cur);
        for (item, value) in items.into_iter().zip(values) {
            sketch.update_with_count(item, value);
        }
        sketch.stream_weight = stream_weight;
        sketch.offset = offset_val;
        Ok(sketch)
    }
}

impl FrequentItemsSketch<i64> {
    /// Serializes this sketch into a byte vector.
    pub fn serialize(&self) -> Vec<u8> {
        self.serialize_inner(serialize_i64_items)
    }

    /// Deserializes a sketch from bytes using the selected serializer.
    ///
    /// Returns an error if `serde` does not match the sketch item type.
    pub fn deserialize(bytes: &[u8], serde: ItemsSerde) -> Result<Self, SerdeError> {
        match serde {
            ItemsSerde::Int64 => Self::deserialize_inner(bytes, deserialize_i64_items),
            ItemsSerde::String => Err(SerdeError::InvalidParameter(
                "ItemsSerde::String cannot deserialize i64 items".to_string(),
            )),
        }
    }
}

impl FrequentItemsSketch<String> {
    /// Serializes this sketch into a byte vector.
    pub fn serialize(&self) -> Vec<u8> {
        self.serialize_inner(serialize_string_items)
    }

    /// Deserializes a sketch from bytes using the selected serializer.
    ///
    /// Returns an error if `serde` does not match the sketch item type.
    pub fn deserialize(bytes: &[u8], serde: ItemsSerde) -> Result<Self, SerdeError> {
        match serde {
            ItemsSerde::String => Self::deserialize_inner(bytes, deserialize_string_items),
            ItemsSerde::Int64 => Err(SerdeError::InvalidParameter(
                "ItemsSerde::Int64 cannot deserialize String items".to_string(),
            )),
        }
    }
}

fn exact_log2(value: usize) -> u8 {
    assert!(value.is_power_of_two(), "value must be power of 2");
    value.trailing_zeros() as u8
}
