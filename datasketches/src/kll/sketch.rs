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
use super::helper::compute_total_capacity;
use super::helper::level_capacity;
use super::helper::random_bit;
use super::helper::sum_the_sample_weights;
use super::serialization::DATA_START;
use super::serialization::DATA_START_SINGLE_ITEM;
use super::serialization::EMPTY_SIZE_BYTES;
use super::serialization::FLAG_EMPTY;
use super::serialization::FLAG_LEVEL_ZERO_SORTED;
use super::serialization::FLAG_SINGLE_ITEM;
use super::serialization::KLL_FAMILY_ID;
use super::serialization::PREAMBLE_INTS_FULL;
use super::serialization::PREAMBLE_INTS_SHORT;
use super::serialization::SERIAL_VERSION_1;
use super::serialization::SERIAL_VERSION_2;
use super::sorted_view::build_sorted_view;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::error::Error;

/// Trait implemented by item types supported by [`KllSketch`].
pub(crate) trait KllItem: Clone {
    /// Compare two items.
    fn cmp(a: &Self, b: &Self) -> Ordering;

    /// Returns true if the item is NaN.
    fn is_nan(_value: &Self) -> bool {
        false
    }

    /// Serialized size in bytes.
    fn serialized_size(value: &Self) -> usize;

    /// Serialize a single item into the buffer.
    fn serialize(value: &Self, bytes: &mut SketchBytes);

    /// Deserialize a single item from the input.
    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error>;
}

/// KLL sketch for estimating quantiles and ranks.
///
/// See the [kll module level documentation](crate::kll) for more.
#[allow(private_bounds)]
#[derive(Debug, Clone, PartialEq)]
pub struct KllSketch<T: KllItem> {
    k: u16,
    m: u8,
    min_k: u16,
    n: u64,
    is_level_zero_sorted: bool,
    levels: Vec<Vec<T>>,
    min_item: Option<T>,
    max_item: Option<T>,
}

impl<T: KllItem> Default for KllSketch<T> {
    fn default() -> Self {
        Self::new(DEFAULT_K)
    }
}

#[allow(private_bounds)]
impl<T: KllItem> KllSketch<T> {
    /// Creates a new sketch with the given value of k.
    ///
    /// # Panics
    ///
    /// Panics if k is not in [MIN_K, MAX_K].
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::kll::KllSketch;
    /// let sketch = KllSketch::<f64>::new(200);
    /// assert_eq!(sketch.k(), 200);
    /// ```
    pub fn new(k: u16) -> Self {
        assert!(
            (MIN_K..=MAX_K).contains(&k),
            "k must be in [{MIN_K}, {MAX_K}], got {k}"
        );
        Self::make(k, k, 0, vec![Vec::new()], None, None, false)
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
        self.levels.iter().map(|level| level.len()).sum()
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
    /// NaN values are ignored for floating-point types.
    pub fn update(&mut self, item: T) {
        if T::is_nan(&item) {
            return;
        }
        self.update_min_max(&item);
        self.internal_update(item);
    }

    /// Merges another sketch into this one.
    ///
    /// # Panics
    ///
    /// Panics if the sketches have incompatible parameters.
    pub fn merge(&mut self, other: &KllSketch<T>) {
        if other.is_empty() {
            return;
        }

        assert_eq!(
            self.m, other.m,
            "incompatible m values: {} and {}",
            self.m, other.m
        );

        self.update_min_max_from_other(other);

        let final_n = self.n + other.n;
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
    }

    /// Returns the normalized rank of the given item.
    pub fn rank(&self, item: &T, inclusive: bool) -> Option<f64> {
        if self.is_empty() {
            return None;
        }
        let view = build_sorted_view(&self.levels);
        Some(view.rank(item, inclusive))
    }

    /// Returns the quantile for the given normalized rank.
    ///
    /// # Panics
    ///
    /// Panics if rank is not in [0.0, 1.0].
    pub fn quantile(&self, rank: f64, inclusive: bool) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        assert!((0.0..=1.0).contains(&rank), "rank must be in [0.0, 1.0]");
        let view = build_sorted_view(&self.levels);
        Some(view.quantile(rank, inclusive))
    }

    /// Returns the approximate CDF for the given split points.
    pub fn cdf(&self, split_points: &[T], inclusive: bool) -> Option<Vec<f64>> {
        if self.is_empty() {
            return None;
        }
        let view = build_sorted_view(&self.levels);
        Some(view.cdf(split_points, inclusive))
    }

    /// Returns the approximate PMF for the given split points.
    pub fn pmf(&self, split_points: &[T], inclusive: bool) -> Option<Vec<f64>> {
        if self.is_empty() {
            return None;
        }
        let view = build_sorted_view(&self.levels);
        Some(view.pmf(split_points, inclusive))
    }

    /// Returns normalized rank error for the configured k.
    pub fn normalized_rank_error(&self, pmf: bool) -> f64 {
        normalized_rank_error(self.min_k, pmf)
    }

    /// Serializes the sketch to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let size = self.serialized_size();
        let mut bytes = SketchBytes::with_capacity(size);

        let is_empty = self.is_empty();
        let is_single_item = self.n == 1;

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
            | (if self.is_level_zero_sorted {
                FLAG_LEVEL_ZERO_SORTED
            } else {
                0
            })
            | (if is_single_item { FLAG_SINGLE_ITEM } else { 0 });

        bytes.write_u8(preamble_ints);
        bytes.write_u8(serial_version);
        bytes.write_u8(KLL_FAMILY_ID);
        bytes.write_u8(flags);
        bytes.write_u16_le(self.k);
        bytes.write_u8(self.m);
        bytes.write_u8(0);

        if is_empty {
            return bytes.into_bytes();
        }

        if !is_single_item {
            bytes.write_u64_le(self.n);
            bytes.write_u16_le(self.min_k);
            bytes.write_u8(self.levels.len() as u8);
            bytes.write_u8(0);

            let level_offsets = self.level_offsets();
            for offset in level_offsets.iter().take(self.levels.len()) {
                bytes.write_u32_le(*offset);
            }

            if let Some(min_item) = &self.min_item {
                T::serialize(min_item, &mut bytes);
            }
            if let Some(max_item) = &self.max_item {
                T::serialize(max_item, &mut bytes);
            }
        }

        for level in &self.levels {
            for item in level {
                T::serialize(item, &mut bytes);
            }
        }

        bytes.into_bytes()
    }

    /// Deserializes a sketch from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<KllSketch<T>, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }

        let mut cursor = SketchSlice::new(bytes);

        let preamble_ints = cursor.read_u8().map_err(make_error("preamble_ints"))?;
        let serial_version = cursor.read_u8().map_err(make_error("serial_version"))?;
        let family_id = cursor.read_u8().map_err(make_error("family_id"))?;
        let flags = cursor.read_u8().map_err(make_error("flags"))?;
        let k = cursor.read_u16_le().map_err(make_error("k"))?;
        let m = cursor.read_u8().map_err(make_error("m"))?;
        let _unused = cursor.read_u8().map_err(make_error("unused"))?;

        if m != DEFAULT_M {
            return Err(Error::deserial(format!(
                "invalid m: expected {DEFAULT_M}, got {m}"
            )));
        }
        if family_id != KLL_FAMILY_ID {
            return Err(Error::invalid_family(KLL_FAMILY_ID, family_id, "KLL"));
        }
        if serial_version != SERIAL_VERSION_1 && serial_version != SERIAL_VERSION_2 {
            return Err(Error::deserial(format!(
                "invalid serial version: {serial_version}"
            )));
        }

        let is_empty = (flags & FLAG_EMPTY) != 0;
        let is_single_item = (flags & FLAG_SINGLE_ITEM) != 0;
        let is_level_zero_sorted = (flags & FLAG_LEVEL_ZERO_SORTED) != 0;
        if is_empty || is_single_item {
            if preamble_ints != PREAMBLE_INTS_SHORT {
                return Err(Error::deserial(format!(
                    "invalid preamble ints: expected {PREAMBLE_INTS_SHORT}, got {preamble_ints}"
                )));
            }
        } else if preamble_ints != PREAMBLE_INTS_FULL {
            return Err(Error::deserial(format!(
                "invalid preamble ints: expected {PREAMBLE_INTS_FULL}, got {preamble_ints}"
            )));
        }

        if !(MIN_K..=MAX_K).contains(&k) {
            return Err(Error::deserial(format!("k out of range: {k}")));
        }

        if is_empty {
            return Ok(Self::make(
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
            let n = cursor.read_u64_le().map_err(make_error("n"))?;
            let min_k = cursor.read_u16_le().map_err(make_error("min_k"))?;
            let num_levels = cursor.read_u8().map_err(make_error("num_levels"))?;
            let _unused = cursor.read_u8().map_err(make_error("unused2"))?;
            (n, min_k, num_levels as usize)
        };

        if num_levels == 0 {
            return Err(Error::deserial("num_levels must be > 0"));
        }
        if min_k < MIN_K || min_k > k {
            return Err(Error::deserial(format!(
                "min_k must be in [{MIN_K}, {k}], got {min_k}"
            )));
        }

        let capacity = compute_total_capacity(k, m, num_levels) as u32;
        let mut level_offsets = Vec::with_capacity(num_levels + 1);
        if !is_single_item {
            for _ in 0..num_levels {
                let offset = cursor.read_u32_le().map_err(make_error("levels"))?;
                level_offsets.push(offset);
            }
        } else {
            level_offsets.push(capacity - 1);
        }
        level_offsets.push(capacity);

        if level_offsets.is_empty() {
            return Err(Error::deserial("levels array is empty"));
        }
        if level_offsets[0] > capacity {
            return Err(Error::deserial("levels[0] exceeds capacity"));
        }
        for window in level_offsets.windows(2) {
            if window[1] < window[0] {
                return Err(Error::deserial("levels array must be non-decreasing"));
            }
        }
        let last = *level_offsets.last().unwrap();
        if last != capacity {
            return Err(Error::deserial("levels last offset must equal capacity"));
        }

        let min_item = if is_single_item {
            None
        } else {
            Some(T::deserialize(&mut cursor)?)
        };
        let max_item = if is_single_item {
            None
        } else {
            Some(T::deserialize(&mut cursor)?)
        };

        let mut levels = Vec::with_capacity(num_levels);
        for level in 0..num_levels {
            let size = (level_offsets[level + 1] - level_offsets[level]) as usize;
            let mut items = Vec::with_capacity(size);
            for _ in 0..size {
                items.push(T::deserialize(&mut cursor)?);
            }
            levels.push(items);
        }

        let mut sketch = Self::make(
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

        Ok(sketch)
    }

    fn make(
        k: u16,
        min_k: u16,
        n: u64,
        levels: Vec<Vec<T>>,
        min_item: Option<T>,
        max_item: Option<T>,
        is_level_zero_sorted: bool,
    ) -> Self {
        Self {
            k,
            m: DEFAULT_M,
            min_k,
            n,
            is_level_zero_sorted,
            levels,
            min_item,
            max_item,
        }
    }

    fn capacity(&self) -> usize {
        compute_total_capacity(self.k, self.m, self.levels.len()) as usize
    }

    fn level_offsets(&self) -> Vec<u32> {
        let capacity = self.capacity() as u32;
        let retained = self.num_retained() as u32;
        assert!(capacity >= retained, "capacity must be >= retained");

        let mut offsets = Vec::with_capacity(self.levels.len() + 1);
        let mut offset = capacity - retained;
        offsets.push(offset);
        for level in &self.levels {
            offset += level.len() as u32;
            offsets.push(offset);
        }
        offsets
    }

    fn serialized_size(&self) -> usize {
        if self.is_empty() {
            return EMPTY_SIZE_BYTES;
        }
        if self.n == 1 {
            let item = &self.levels[0][0];
            return DATA_START_SINGLE_ITEM + T::serialized_size(item);
        }

        let mut size = DATA_START + self.levels.len() * 4;
        if let Some(min_item) = &self.min_item {
            size += T::serialized_size(min_item);
        }
        if let Some(max_item) = &self.max_item {
            size += T::serialized_size(max_item);
        }
        for level in &self.levels {
            for item in level {
                size += T::serialized_size(item);
            }
        }
        size
    }

    fn update_min_max(&mut self, item: &T) {
        match self.min_item.as_ref() {
            None => {
                self.min_item = Some(item.clone());
                self.max_item = Some(item.clone());
            }
            Some(min) => {
                if T::cmp(item, min) == Ordering::Less {
                    self.min_item = Some(item.clone());
                }
                if let Some(max) = &self.max_item {
                    if T::cmp(max, item) == Ordering::Less {
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
                    if T::cmp(other_min, min) == Ordering::Less {
                        self.min_item = Some(other_min.clone());
                    }
                }
                if let Some(other_max) = &other.max_item {
                    if T::cmp(max, other_max) == Ordering::Less {
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
        if self.num_retained() >= self.capacity() {
            self.compress_while_updating();
        }
        self.n += 1;
        self.is_level_zero_sorted = false;
        self.levels[0].insert(0, item);
    }

    fn compress_while_updating(&mut self) {
        let level = self.find_level_to_compact();
        if level + 1 == self.levels.len() {
            self.levels.push(Vec::new());
        }

        let mut current = std::mem::take(&mut self.levels[level]);
        let mut above = std::mem::take(&mut self.levels[level + 1]);

        let odd = current.len() % 2 == 1;
        let mut leftover = None;
        if odd {
            leftover = Some(current.remove(0));
        }

        if level == 0 && !self.is_level_zero_sorted {
            current.sort_by(T::cmp);
        }

        let use_up = above.is_empty();
        let promoted = downsample(current, random_bit(), use_up);
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
        panic!("no level to compact");
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
    }

    fn total_weight(&self) -> u64 {
        let sizes: Vec<usize> = self.levels.iter().map(|level| level.len()).collect();
        sum_the_sample_weights(&sizes)
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

fn downsample<T: KllItem>(items: Vec<T>, offset: u32, use_up: bool) -> Vec<T> {
    let len = items.len();
    debug_assert!(len % 2 == 0, "length must be even");
    let offset = (offset & 1) as usize;
    let parity = if use_up {
        (len - 1 - offset) % 2
    } else {
        offset
    };

    items
        .into_iter()
        .enumerate()
        .filter_map(|(idx, item)| if idx % 2 == parity { Some(item) } else { None })
        .collect()
}

fn merge_sorted_vec<T: KllItem>(left: Vec<T>, right: Vec<T>) -> Vec<T> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut left_iter = left.into_iter().peekable();
    let mut right_iter = right.into_iter().peekable();

    while let (Some(l), Some(r)) = (left_iter.peek(), right_iter.peek()) {
        if T::cmp(l, r) == Ordering::Less {
            merged.push(left_iter.next().unwrap());
        } else {
            merged.push(right_iter.next().unwrap());
        }
    }
    merged.extend(left_iter);
    merged.extend(right_iter);
    merged
}

fn general_compress<T: KllItem>(
    mut levels_in: Vec<Vec<T>>,
    k: u16,
    m: u8,
    is_level_zero_sorted: bool,
) -> Vec<Vec<T>> {
    let mut current_num_levels = levels_in.len();
    let mut current_item_count: usize = levels_in.iter().map(|level| level.len()).sum();
    let mut target_item_count = compute_total_capacity(k, m, current_num_levels) as usize;
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
            let mut current = std::mem::take(&mut levels_in[current_level]);
            let mut above = std::mem::take(&mut levels_in[current_level + 1]);

            let odd = current.len() % 2 == 1;
            let mut leftover = None;
            if odd {
                leftover = Some(current.remove(0));
            }

            if current_level == 0 && !is_level_zero_sorted {
                current.sort_by(T::cmp);
            }

            let use_up = above.is_empty();
            let promoted = downsample(current, random_bit(), use_up);
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

impl KllItem for f32 {
    fn cmp(a: &Self, b: &Self) -> Ordering {
        a.partial_cmp(b).unwrap_or(Ordering::Greater)
    }

    fn is_nan(value: &Self) -> bool {
        value.is_nan()
    }

    fn serialized_size(_value: &Self) -> usize {
        4
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_f32_le(*value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        input
            .read_f32_le()
            .map_err(|_| Error::insufficient_data("f32"))
    }
}

impl KllItem for f64 {
    fn cmp(a: &Self, b: &Self) -> Ordering {
        a.partial_cmp(b).unwrap_or(Ordering::Greater)
    }

    fn is_nan(value: &Self) -> bool {
        value.is_nan()
    }

    fn serialized_size(_value: &Self) -> usize {
        8
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_f64_le(*value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        input
            .read_f64_le()
            .map_err(|_| Error::insufficient_data("f64"))
    }
}

impl KllItem for i64 {
    fn cmp(a: &Self, b: &Self) -> Ordering {
        a.cmp(b)
    }

    fn serialized_size(_value: &Self) -> usize {
        8
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_i64_le(*value);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        input
            .read_i64_le()
            .map_err(|_| Error::insufficient_data("i64"))
    }
}

impl KllItem for String {
    fn cmp(a: &Self, b: &Self) -> Ordering {
        a.cmp(b)
    }

    fn serialized_size(value: &Self) -> usize {
        4 + value.len()
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_u32_le(value.len() as u32);
        bytes.write(value.as_bytes());
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        let len = input
            .read_u32_le()
            .map_err(|_| Error::insufficient_data("string_len"))? as usize;
        let mut buf = vec![0u8; len];
        input
            .read_exact(&mut buf)
            .map_err(|_| Error::insufficient_data("string_bytes"))?;
        String::from_utf8(buf).map_err(|_| Error::deserial("invalid utf-8 string"))
    }
}
