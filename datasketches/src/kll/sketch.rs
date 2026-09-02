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

use std::cmp::Ordering;

use super::DEFAULT_K;
use super::DEFAULT_M;
use super::MAX_K;
use super::MIN_K;
use super::capacity::level_capacity;
use super::capacity::total_capacity;
use super::serialization::DATA_START;
use super::serialization::DATA_START_SINGLE_ITEM;
use super::serialization::EMPTY_SIZE_BYTES;
use super::serialization::FLAG_EMPTY;
use super::serialization::FLAG_LEVEL_ZERO_SORTED;
use super::serialization::FLAG_SINGLE_ITEM;
use super::serialization::MAX_NUM_LEVELS;
use super::serialization::PREAMBLE_INTS_FULL;
use super::serialization::PREAMBLE_INTS_SHORT;
use super::serialization::SERIAL_VERSION_1;
use super::serialization::SERIAL_VERSION_2;
use super::sorted_view::SortedView;
use super::sorted_view::build_sorted_view;
use super::value::KllValue;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::common::SearchCriteria;
use crate::error::Error;

/// KLL sketch for estimating quantiles and ranks.
///
/// See the [kll module level documentation](crate::kll) for more.
#[derive(Debug, Clone, PartialEq)]
pub struct KllSketch<T> {
    k: u16,
    m: u8,
    min_k: u16,
    n: u64,
    num_retained: usize,
    capacity: usize,
    is_level_zero_sorted: bool,
    levels: Vec<Vec<T>>,
    min_item: Option<T>,
    max_item: Option<T>,
}

impl<T: Clone + Ord> Default for KllSketch<T> {
    fn default() -> Self {
        Self::make(DEFAULT_K, DEFAULT_K, 0, vec![Vec::new()], None, None, false)
    }
}

impl<T: Clone + Ord> KllSketch<T> {
    /// Creates a new sketch with the given value of k.
    ///
    /// # Errors
    ///
    /// Returns an error if `k` is outside `8..=65535`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::kll::KllSketch;
    /// let sketch = KllSketch::<i64>::new(200).unwrap();
    /// assert_eq!(sketch.k(), 200);
    /// ```
    pub fn new(k: u16) -> Result<Self, Error> {
        if !(MIN_K..=MAX_K).contains(&k) {
            return Err(Error::invalid_argument(format!(
                "k must be in [{MIN_K}, {MAX_K}], got {k}"
            )));
        }
        Ok(Self::make(k, k, 0, vec![Vec::new()], None, None, false))
    }

    /// Returns parameter k used to configure this sketch.
    pub fn k(&self) -> u16 {
        self.k
    }

    /// Returns the minimum k used when merging sketches.
    pub fn min_k(&self) -> u16 {
        self.min_k
    }

    /// Returns total weight of the stream.
    pub fn n(&self) -> u64 {
        self.n
    }

    /// Returns true if the sketch has not seen any data.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Returns the number of retained items.
    pub fn num_retained(&self) -> usize {
        self.num_retained
    }

    /// Returns true if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        self.levels.len() > 1
    }

    /// Returns the minimum item seen by the sketch.
    pub fn min_item(&self) -> Option<&T> {
        self.min_item.as_ref()
    }

    /// Returns the maximum item seen by the sketch.
    pub fn max_item(&self) -> Option<&T> {
        self.max_item.as_ref()
    }

    /// Updates the sketch with a new item.
    ///
    /// # Panics
    ///
    /// Panics if the stream weight would exceed [`u64::MAX`].
    pub fn update(&mut self, item: T) {
        self.update_min_max(&item);
        self.internal_update(item);
    }

    /// Resets this sketch to its empty state while retaining its configuration.
    pub fn reset(&mut self) {
        self.min_k = self.k;
        self.n = 0;
        self.num_retained = 0;
        self.capacity = total_capacity(self.k, self.m, 1) as usize;
        self.is_level_zero_sorted = false;
        self.levels.clear();
        self.levels.push(Vec::new());
        self.min_item = None;
        self.max_item = None;
    }

    /// Merges another sketch into this one.
    ///
    /// # Errors
    ///
    /// Returns an error if the combined stream weight exceeds [`u64::MAX`].
    pub fn merge(&mut self, other: &KllSketch<T>) -> Result<(), Error> {
        if other.is_empty() {
            return Ok(());
        }

        if self.m != other.m {
            return Err(Error::invalid_argument(format!(
                "cannot merge sketches with different m values: {} and {}",
                self.m, other.m
            )));
        }
        let final_n = self.n.checked_add(other.n).ok_or_else(|| {
            Error::invalid_argument(format!(
                "combined stream weight exceeds {}: left {}, right {}",
                u64::MAX,
                self.n,
                other.n
            ))
        })?;

        self.update_min_max_from_other(other);

        for item in &other.levels[0] {
            self.internal_update(item.clone());
        }

        if other.levels.len() >= 2 {
            self.merge_higher_levels(other);
        }

        self.n = final_n;
        if other.is_estimation_mode() {
            self.min_k = self.min_k.min(other.min_k);
        }

        debug_assert_eq!(self.total_weight(), self.n, "total weight does not match n");
        Ok(())
    }

    /// Returns the normalized rank of the given item.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty.
    pub fn rank(&self, item: &T, criteria: SearchCriteria) -> Result<f64, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("cannot query an empty sketch"));
        }
        let inclusive = criteria == SearchCriteria::Inclusive;
        let mut weight = 0u64;
        for (level, items) in self.levels.iter().enumerate() {
            let count = items
                .iter()
                .filter(|retained| match (*retained).cmp(item) {
                    Ordering::Less => true,
                    Ordering::Equal => inclusive,
                    Ordering::Greater => false,
                })
                .count() as u64;
            weight += count << level;
        }
        Ok(weight as f64 / self.n as f64)
    }

    /// Returns the quantile for the given normalized rank.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty or `rank` is outside `[0.0, 1.0]`.
    pub fn quantile(&self, rank: f64, criteria: SearchCriteria) -> Result<T, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("cannot query an empty sketch"));
        }
        if !(0.0..=1.0).contains(&rank) {
            return Err(Error::invalid_argument(format!(
                "rank must be in [0.0, 1.0], got {rank}"
            )));
        }
        self.sorted_view().quantile(rank, criteria)
    }

    /// Returns approximate quantiles for the given normalized ranks.
    ///
    /// The sorted view is built once for the whole batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty or any rank is outside `[0.0, 1.0]`.
    pub fn quantiles(&self, ranks: &[f64], criteria: SearchCriteria) -> Result<Vec<T>, Error> {
        self.sorted_view().quantiles(ranks, criteria)
    }

    /// Returns the approximate CDF for the given split points.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty or the split points are not unique and strictly
    /// increasing.
    pub fn cdf(&self, split_points: &[T], criteria: SearchCriteria) -> Result<Vec<f64>, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("cannot query an empty sketch"));
        }
        self.sorted_view().cdf(split_points, criteria)
    }

    /// Returns the approximate PMF for the given split points.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty or the split points are not unique and strictly
    /// increasing.
    pub fn pmf(&self, split_points: &[T], criteria: SearchCriteria) -> Result<Vec<f64>, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("cannot query an empty sketch"));
        }
        self.sorted_view().pmf(split_points, criteria)
    }

    /// Returns an owned, sorted snapshot of the current sketch state.
    ///
    /// The view can be reused for repeated queries while this sketch continues to receive updates.
    pub fn sorted_view(&self) -> SortedView<T> {
        build_sorted_view(&self.levels, self.is_level_zero_sorted)
    }

    /// Returns the normalized single-sided rank error for the configured k.
    pub fn normalized_rank_error(&self) -> f64 {
        normalized_rank_error(self.min_k, false)
    }

    /// Returns the normalized double-sided rank error for PMF queries for the configured k.
    pub fn normalized_pmf_error(&self) -> f64 {
        normalized_rank_error(self.min_k, true)
    }
}

fn serialized_size<T: KllValue + Ord>(sketch: &KllSketch<T>) -> usize {
    if sketch.is_empty() {
        return EMPTY_SIZE_BYTES;
    }
    if sketch.n == 1 {
        let item = &sketch.levels[0][0];
        return DATA_START_SINGLE_ITEM + T::serialized_size(item);
    }

    let mut size = DATA_START + sketch.levels.len() * 4;
    if let Some(min_item) = &sketch.min_item {
        size += T::serialized_size(min_item);
    }
    if let Some(max_item) = &sketch.max_item {
        size += T::serialized_size(max_item);
    }
    for level in &sketch.levels {
        for item in level {
            size += T::serialized_size(item);
        }
    }
    size
}

fn serialize_with_serde<T: KllValue + Ord>(sketch: &KllSketch<T>) -> Vec<u8> {
    let size = serialized_size(sketch);
    let mut bytes = SketchBytes::with_capacity(size);

    let is_empty = sketch.is_empty();
    let is_single_item = sketch.n == 1;

    let preamble_ints = if is_empty || is_single_item {
        PREAMBLE_INTS_SHORT
    } else {
        PREAMBLE_INTS_FULL
    };
    let serial_version = if is_single_item {
        SERIAL_VERSION_2
    } else {
        SERIAL_VERSION_1
    };

    let flags = (if is_empty { FLAG_EMPTY } else { 0 })
        | (if sketch.is_level_zero_sorted {
            FLAG_LEVEL_ZERO_SORTED
        } else {
            0
        })
        | (if is_single_item { FLAG_SINGLE_ITEM } else { 0 });

    bytes.write_u8(preamble_ints);
    bytes.write_u8(serial_version);
    bytes.write_u8(Family::KLL.id);
    bytes.write_u8(flags);
    bytes.write_u16_le(sketch.k);
    bytes.write_u8(sketch.m);
    bytes.write_u8(0);

    if is_empty {
        return bytes.into_bytes();
    }

    if !is_single_item {
        bytes.write_u64_le(sketch.n);
        bytes.write_u16_le(sketch.min_k);
        bytes.write_u8(sketch.levels.len() as u8);
        bytes.write_u8(0);

        let level_offsets = sketch.level_offsets();
        for offset in level_offsets.iter().take(sketch.levels.len()) {
            bytes.write_u32_le(*offset);
        }

        if let Some(min_item) = &sketch.min_item {
            T::serialize(min_item, &mut bytes);
        }
        if let Some(max_item) = &sketch.max_item {
            T::serialize(max_item, &mut bytes);
        }
    }

    for (level_index, level) in sketch.levels.iter().enumerate() {
        if level_index == 0 && !sketch.is_level_zero_sorted {
            for item in level.iter().rev() {
                T::serialize(item, &mut bytes);
            }
        } else {
            for item in level {
                T::serialize(item, &mut bytes);
            }
        }
    }

    bytes.into_bytes()
}

fn deserialize_with_serde<T: KllValue + Ord>(bytes: &[u8]) -> Result<KllSketch<T>, Error> {
    let mut cursor = SketchSlice::new(bytes);

    let preamble_ints = cursor
        .read_u8()
        .map_err(insufficient_data("preamble_ints"))?;
    let serial_version = cursor
        .read_u8()
        .map_err(insufficient_data("serial_version"))?;
    let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
    let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
    let k = cursor.read_u16_le().map_err(insufficient_data("k"))?;
    let m = cursor.read_u8().map_err(insufficient_data("m"))?;
    let _unused = cursor.read_u8().map_err(insufficient_data("unused"))?;

    if m != DEFAULT_M {
        return Err(Error::deserial(format!(
            "invalid m: expected {DEFAULT_M}, got {m}"
        )));
    }
    Family::KLL.validate_id(family_id)?;
    let is_empty = (flags & FLAG_EMPTY) != 0;
    let is_single_item = (flags & FLAG_SINGLE_ITEM) != 0;
    let is_level_zero_sorted = (flags & FLAG_LEVEL_ZERO_SORTED) != 0;
    if is_empty && is_single_item {
        return Err(Error::deserial(
            "empty and single-item flags must not both be set",
        ));
    }
    if is_empty || is_single_item {
        if preamble_ints != PREAMBLE_INTS_SHORT {
            return Err(Error::invalid_preamble_ints(
                PREAMBLE_INTS_SHORT,
                preamble_ints,
            ));
        }
    } else if preamble_ints != PREAMBLE_INTS_FULL {
        return Err(Error::invalid_preamble_ints(
            PREAMBLE_INTS_FULL,
            preamble_ints,
        ));
    }
    let expected_version = if is_single_item {
        SERIAL_VERSION_2
    } else {
        SERIAL_VERSION_1
    };
    ensure_serial_version_is(expected_version, serial_version)?;

    if !(MIN_K..=MAX_K).contains(&k) {
        return Err(Error::deserial(format!(
            "k must be in [{MIN_K}, {MAX_K}], got {k}"
        )));
    }

    if is_empty {
        let trailing_bytes = cursor.remaining().len();
        if trailing_bytes != 0 {
            return Err(Error::deserial(format!(
                "expected end of KLL image, found {trailing_bytes} trailing bytes"
            )));
        }
        return Ok(KllSketch::make(
            k,
            k,
            0,
            vec![Vec::new()],
            None,
            None,
            is_level_zero_sorted,
        ));
    }

    let (n, min_k, num_levels) = if is_single_item {
        (1u64, k, 1usize)
    } else {
        let n = cursor.read_u64_le().map_err(insufficient_data("n"))?;
        let min_k = cursor.read_u16_le().map_err(insufficient_data("min_k"))?;
        let num_levels = cursor.read_u8().map_err(insufficient_data("num_levels"))?;
        let _unused = cursor.read_u8().map_err(insufficient_data("unused2"))?;
        (n, min_k, num_levels as usize)
    };

    if !(1..=MAX_NUM_LEVELS).contains(&num_levels) {
        return Err(Error::deserial(format!(
            "num_levels must be in [1, {MAX_NUM_LEVELS}], got {num_levels}"
        )));
    }
    if !is_single_item && n < 2 {
        return Err(Error::deserial(format!(
            "full sketch n must be at least 2, got {n}"
        )));
    }
    if min_k < MIN_K || min_k > k {
        return Err(Error::deserial(format!(
            "min_k must be in [{MIN_K}, {k}], got {min_k}"
        )));
    }

    let capacity = total_capacity(k, m, num_levels);
    let mut level_offsets = Vec::with_capacity(num_levels + 1);
    if !is_single_item {
        for _ in 0..num_levels {
            let offset = cursor.read_u32_le().map_err(insufficient_data("levels"))?;
            level_offsets.push(offset);
        }
    } else {
        level_offsets.push(capacity - 1);
    }
    level_offsets.push(capacity);

    if level_offsets[0] > capacity {
        return Err(Error::deserial(format!(
            "first level offset must not exceed capacity {capacity}, got {}",
            level_offsets[0]
        )));
    }
    for (index, window) in level_offsets.windows(2).enumerate() {
        if window[1] < window[0] {
            return Err(Error::deserial(format!(
                "level offsets must be nondecreasing: offset[{index}] is {}, offset[{}] is {}",
                window[0],
                index + 1,
                window[1]
            )));
        }
    }

    let min_item = if is_single_item {
        None
    } else {
        Some(
            T::deserialize(&mut cursor)
                .map_err(|error| error.with_context("KLL item", "minimum"))?,
        )
    };
    let max_item = if is_single_item {
        None
    } else {
        Some(
            T::deserialize(&mut cursor)
                .map_err(|error| error.with_context("KLL item", "maximum"))?,
        )
    };

    let num_retained = (level_offsets[num_levels] - level_offsets[0]) as usize;
    let min_item_bytes = num_retained
        .checked_mul(T::MIN_SERIALIZED_SIZE)
        .ok_or_else(|| {
            Error::deserial(format!(
                "minimum serialized size overflows usize: {num_retained} retained items, {} bytes per item",
                T::MIN_SERIALIZED_SIZE
            ))
        })?;
    let available_item_bytes = cursor.remaining().len();
    if available_item_bytes < min_item_bytes {
        return Err(Error::insufficient_data_of(
            "KLL item payload",
            format_args!("expected {min_item_bytes} bytes, got {available_item_bytes}"),
        ));
    }

    let mut levels = Vec::with_capacity(num_levels);
    for level in 0..num_levels {
        let size = (level_offsets[level + 1] - level_offsets[level]) as usize;
        let mut items = Vec::with_capacity(size);
        for index in 0..size {
            items.push(T::deserialize(&mut cursor).map_err(|error| {
                error
                    .with_context("KLL level", level)
                    .with_context("item index", index)
            })?);
        }
        levels.push(items);
    }
    if !is_level_zero_sorted {
        levels[0].reverse();
    }

    let mut sketch = KllSketch::make(
        k,
        min_k,
        n,
        levels,
        min_item,
        max_item,
        is_level_zero_sorted,
    );

    if is_single_item {
        if let Some(item) = sketch.levels[0].first().cloned() {
            sketch.min_item = Some(item.clone());
            sketch.max_item = Some(item);
        }
    }

    sketch.validate_deserialized_state()?;
    let trailing_bytes = cursor.remaining().len();
    if trailing_bytes != 0 {
        return Err(Error::deserial(format!(
            "expected end of KLL image, found {trailing_bytes} trailing bytes"
        )));
    }

    Ok(sketch)
}

impl<T: KllValue + Ord> KllSketch<T> {
    /// Serializes the sketch to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        serialize_with_serde(self)
    }

    /// Deserializes a sketch from bytes.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the image is truncated, malformed, or contains values that are not
    /// totally ordered.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        deserialize_with_serde(bytes)
    }
}

impl<T: Clone + Ord> KllSketch<T> {
    fn make(
        k: u16,
        min_k: u16,
        n: u64,
        levels: Vec<Vec<T>>,
        min_item: Option<T>,
        max_item: Option<T>,
        is_level_zero_sorted: bool,
    ) -> Self {
        let num_retained = levels.iter().map(Vec::len).sum();
        let capacity = total_capacity(k, DEFAULT_M, levels.len()) as usize;
        Self {
            k,
            m: DEFAULT_M,
            min_k,
            n,
            num_retained,
            capacity,
            is_level_zero_sorted,
            levels,
            min_item,
            max_item,
        }
    }

    fn level_offsets(&self) -> Vec<u32> {
        let capacity = self.capacity as u32;
        let retained = self.num_retained() as u32;
        assert!(
            capacity >= retained,
            "KLL retained item count must not exceed capacity: retained {retained}, capacity {capacity}"
        );

        let mut offsets = Vec::with_capacity(self.levels.len() + 1);
        let mut offset = capacity - retained;
        offsets.push(offset);
        for level in &self.levels {
            offset += level.len() as u32;
            offsets.push(offset);
        }
        offsets
    }

    fn update_min_max(&mut self, item: &T) {
        match self.min_item.as_ref() {
            None => {
                self.min_item = Some(item.clone());
                self.max_item = Some(item.clone());
            }
            Some(min) => {
                if item.cmp(min) == Ordering::Less {
                    self.min_item = Some(item.clone());
                }
                if let Some(max) = &self.max_item {
                    if max.cmp(item) == Ordering::Less {
                        self.max_item = Some(item.clone());
                    }
                }
            }
        }
    }

    fn update_min_max_from_other(&mut self, other: &KllSketch<T>) {
        match (&self.min_item, &self.max_item) {
            (None, None) => {
                self.min_item = other.min_item.clone();
                self.max_item = other.max_item.clone();
            }
            (Some(min), Some(max)) => {
                if let Some(other_min) = &other.min_item {
                    if other_min.cmp(min) == Ordering::Less {
                        self.min_item = Some(other_min.clone());
                    }
                }
                if let Some(other_max) = &other.max_item {
                    if max.cmp(other_max) == Ordering::Less {
                        self.max_item = Some(other_max.clone());
                    }
                }
            }
            _ => {
                self.min_item = other.min_item.clone();
                self.max_item = other.max_item.clone();
            }
        }
    }

    fn internal_update(&mut self, item: T) {
        if self.num_retained >= self.capacity {
            self.compress_while_updating();
        }
        self.n = self.n.checked_add(1).unwrap_or_else(|| {
            panic!(
                "cannot update KLL sketch: stream weight is {}, maximum is {}",
                self.n,
                u64::MAX
            )
        });
        self.num_retained += 1;
        self.is_level_zero_sorted = false;
        self.levels[0].push(item);
    }

    fn compress_while_updating(&mut self) {
        let level = self.find_level_to_compact();
        if level + 1 == self.levels.len() {
            self.levels.push(Vec::new());
        }

        let current = std::mem::take(&mut self.levels[level]);
        let mut above = std::mem::take(&mut self.levels[level + 1]);
        let use_up = above.is_empty();
        let (leftover, promoted) = compact_level(
            current,
            level,
            self.is_level_zero_sorted,
            rand::random::<bool>(),
            use_up,
        );
        if above.is_empty() {
            above = promoted;
        } else {
            above = merge_sorted_vec(promoted, above);
        }
        self.levels[level + 1] = above;

        let mut new_level = Vec::new();
        if let Some(item) = leftover {
            new_level.push(item);
        }
        self.levels[level] = new_level;
        self.refresh_capacity_state();
    }

    fn find_level_to_compact(&self) -> usize {
        let num_levels = self.levels.len();
        for level in 0..num_levels {
            let pop = self.levels[level].len() as u32;
            let cap = level_capacity(self.k, num_levels, level, self.m);
            if pop >= cap {
                return level;
            }
        }
        panic!(
            "KLL sketch has {}/{} retained items but no level reached its compaction capacity (k {}, m {}, levels {num_levels})",
            self.num_retained, self.capacity, self.k, self.m
        );
    }

    fn merge_higher_levels(&mut self, other: &KllSketch<T>) {
        let provisional_levels = self.levels.len().max(other.levels.len());
        let mut self_levels = std::mem::take(&mut self.levels);
        let mut work_levels = vec![Vec::new(); provisional_levels];
        work_levels[0] = std::mem::take(&mut self_levels[0]);

        for level in 1..provisional_levels {
            let left = if level < self_levels.len() {
                std::mem::take(&mut self_levels[level])
            } else {
                Vec::new()
            };
            let right = other.levels.get(level).cloned().unwrap_or_default();

            work_levels[level] = if left.is_empty() {
                right
            } else if right.is_empty() {
                left
            } else {
                merge_sorted_vec(left, right)
            };
        }

        self.levels = general_compress(work_levels, self.k, self.m, self.is_level_zero_sorted);
        self.refresh_capacity_state();
    }

    fn refresh_capacity_state(&mut self) {
        self.num_retained = self.levels.iter().map(Vec::len).sum();
        self.capacity = total_capacity(self.k, self.m, self.levels.len()) as usize;
    }

    fn total_weight(&self) -> u64 {
        self.levels
            .iter()
            .enumerate()
            .map(|(level, items)| (items.len() as u64) << level)
            .sum()
    }

    fn validate_deserialized_state(&self) -> Result<(), Error> {
        let min_item = self
            .min_item
            .as_ref()
            .ok_or_else(|| Error::deserial("non-empty sketch must have a minimum item"))?;
        let max_item = self
            .max_item
            .as_ref()
            .ok_or_else(|| Error::deserial("non-empty sketch must have a maximum item"))?;

        if min_item.cmp(max_item) == Ordering::Greater {
            return Err(Error::deserial(
                "minimum item must not be greater than maximum item",
            ));
        }

        let mut total_weight = 0u64;
        let mut level_weight = 1u64;
        for (level_index, level) in self.levels.iter().enumerate() {
            let level_total = level_weight
                .checked_mul(level.len() as u64)
                .ok_or_else(|| {
                    Error::deserial(format!(
                        "sample weight overflows u64 at level {level_index}: weight {level_weight}, retained items {}",
                        level.len()
                    ))
                })?;
            total_weight = total_weight
                .checked_add(level_total)
                .ok_or_else(|| {
                    Error::deserial(format!(
                        "total sample weight overflows u64 at level {level_index}: accumulated {total_weight}, level contribution {level_total}"
                    ))
                })?;

            let must_be_sorted = level_index > 0 || self.is_level_zero_sorted;
            if must_be_sorted {
                for (item_index, pair) in level.windows(2).enumerate() {
                    if pair[0].cmp(&pair[1]) == Ordering::Greater {
                        return Err(Error::deserial(format!(
                            "level {level_index} must be sorted: item at index {item_index} is greater than item at index {}",
                            item_index + 1
                        )));
                    }
                }
            }

            for (item_index, item) in level.iter().enumerate() {
                if item.cmp(min_item) == Ordering::Less || item.cmp(max_item) == Ordering::Greater {
                    return Err(Error::deserial(format!(
                        "retained item at level {level_index}, index {item_index} is outside the serialized minimum and maximum"
                    )));
                }
            }

            if level_index + 1 < self.levels.len() {
                level_weight = level_weight
                    .checked_mul(2)
                    .ok_or_else(|| {
                        Error::deserial(format!(
                            "level weight overflows u64 after level {level_index}: current weight {level_weight}"
                        ))
                    })?;
            }
        }

        if total_weight != self.n {
            return Err(Error::deserial(format!(
                "total sample weight {total_weight} does not match n {}",
                self.n
            )));
        }

        Ok(())
    }
}

fn normalized_rank_error(k: u16, pmf: bool) -> f64 {
    let k = k as f64;
    if pmf {
        2.446 / k.powf(0.9433)
    } else {
        2.296 / k.powf(0.9723)
    }
}

fn compact_level<T: Ord>(
    mut items: Vec<T>,
    level: usize,
    is_level_zero_sorted: bool,
    offset: bool,
    use_up: bool,
) -> (Option<T>, Vec<T>) {
    let odd = items.len() % 2 == 1;
    let level_zero_needs_sorting = level == 0 && !is_level_zero_sorted;
    let leftover = if odd && level_zero_needs_sorting {
        items.pop()
    } else {
        None
    };
    if level_zero_needs_sorting {
        items.sort_unstable();
    }

    let mut items = items.into_iter();
    let leftover = if odd && !level_zero_needs_sorting {
        items.next()
    } else {
        leftover
    };
    let promoted = downsample(items, offset, use_up);
    (leftover, promoted)
}

fn downsample<T, I: ExactSizeIterator<Item = T>>(items: I, offset: bool, use_up: bool) -> Vec<T> {
    let len = items.len();
    debug_assert!(
        len % 2 == 0,
        "KLL compaction requires an even item count, got {len}"
    );
    let offset = usize::from(offset);
    let parity = if use_up {
        (len - 1 - offset) % 2
    } else {
        offset
    };

    items
        .enumerate()
        .filter_map(|(idx, item)| if idx % 2 == parity { Some(item) } else { None })
        .collect()
}

fn merge_sorted_vec<T: Clone + Ord>(left: Vec<T>, right: Vec<T>) -> Vec<T> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut left_iter = left.into_iter().peekable();
    let mut right_iter = right.into_iter().peekable();

    while let (Some(l), Some(r)) = (left_iter.peek(), right_iter.peek()) {
        if l.cmp(r) == Ordering::Less {
            merged.push(left_iter.next().unwrap());
        } else {
            merged.push(right_iter.next().unwrap());
        }
    }
    merged.extend(left_iter);
    merged.extend(right_iter);
    merged
}

fn general_compress<T: Clone + Ord>(
    mut levels_in: Vec<Vec<T>>,
    k: u16,
    m: u8,
    is_level_zero_sorted: bool,
) -> Vec<Vec<T>> {
    let mut current_num_levels = levels_in.len();
    let mut current_item_count: usize = levels_in.iter().map(|level| level.len()).sum();
    let mut target_item_count = total_capacity(k, m, current_num_levels) as usize;
    let mut levels_out = Vec::with_capacity(current_num_levels + 1);

    let mut current_level = 0usize;
    while current_level < current_num_levels {
        if current_level + 1 >= levels_in.len() {
            levels_in.push(Vec::new());
        }

        let raw_pop = levels_in[current_level].len();
        let cap = level_capacity(k, current_num_levels, current_level, m) as usize;

        if current_item_count < target_item_count || raw_pop < cap {
            levels_out.push(std::mem::take(&mut levels_in[current_level]));
        } else {
            let current = std::mem::take(&mut levels_in[current_level]);
            let mut above = std::mem::take(&mut levels_in[current_level + 1]);
            let use_up = above.is_empty();
            let (leftover, promoted) = compact_level(
                current,
                current_level,
                is_level_zero_sorted,
                rand::random::<bool>(),
                use_up,
            );
            let promoted_len = promoted.len();
            if above.is_empty() {
                above = promoted;
            } else {
                above = merge_sorted_vec(promoted, above);
            }
            levels_in[current_level + 1] = above;

            let mut out_level = Vec::new();
            if let Some(item) = leftover {
                out_level.push(item);
            }
            levels_out.push(out_level);

            current_item_count = current_item_count.saturating_sub(promoted_len);

            if current_level == current_num_levels - 1 {
                current_num_levels += 1;
                target_item_count += level_capacity(k, current_num_levels, 0, m) as usize;
                if levels_in.len() < current_num_levels + 1 {
                    levels_in.resize_with(current_num_levels + 1, Vec::new);
                }
            }
        }
        current_level += 1;
    }

    levels_out.truncate(current_num_levels);
    levels_out
}
