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

//! Generic REQ sketch implementation.

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::common::NumStdDev;
use crate::common::SearchCriteria;
use crate::error::Error;
use crate::req::DEFAULT_K;
use crate::req::INITIAL_SECTIONS_PER_COMPACTOR;
use crate::req::MAX_K;
use crate::req::MIN_K;
use crate::req::RankAccuracy;
use crate::req::compactor::Compactor;
use crate::req::iter::ReqSketchIterator;
use crate::req::serialization::FLAG_IS_EMPTY;
use crate::req::serialization::FLAG_IS_HIGH_RANK;
use crate::req::serialization::FLAG_IS_LEVEL_ZERO_SORTED;
use crate::req::serialization::FLAG_RAW_ITEMS;
use crate::req::serialization::PREAMBLE_INTS_ESTIMATION;
use crate::req::serialization::PREAMBLE_INTS_EXACT;
use crate::req::serialization::RAW_ITEMS_THRESHOLD;
use crate::req::serialization::SERIAL_VERSION;
use crate::req::serialization::check_preamble_ints;
use crate::req::serialization::check_serial_version;
use crate::req::sorted_view::SortedView;
use crate::req::value::ReqValue;

/// A Relative Error Quantiles sketch for approximate quantile estimation.
///
/// See the [module level documentation](super) for item ordering and floating-point requirements.
#[derive(Debug, Clone)]
pub struct ReqSketch<T> {
    k: u16,
    rank_accuracy: RankAccuracy,
    n: u64,
    max_nom_size: u32,
    num_retained: u32,
    compactors: Vec<Compactor<T>>,
    promotion_buf: Vec<T>,
    min_item: Option<T>,
    max_item: Option<T>,
}

impl<T> Default for ReqSketch<T>
where
    T: Clone + Ord,
{
    fn default() -> Self {
        Self::make(DEFAULT_K, RankAccuracy::HighRank)
    }
}

impl<T> ReqSketch<T>
where
    T: Clone + Ord,
{
    /// Creates a sketch with the given `k` and rank-accuracy mode.
    ///
    /// Larger `k` improves accuracy at the cost of retained memory.
    ///
    /// # Errors
    ///
    /// Returns an error if `k` is odd or outside `[4, 1024]`.
    pub fn new(k: u16, rank_accuracy: RankAccuracy) -> Result<Self, Error> {
        if !(MIN_K..=MAX_K).contains(&k) {
            return Err(Error::invalid_argument(format!(
                "k must be in [{MIN_K}, {MAX_K}], got {k}"
            )));
        }
        if k % 2 != 0 {
            return Err(Error::invalid_argument(format!("k must be even, got {k}")));
        }
        Ok(Self::make(k, rank_accuracy))
    }

    /// Returns the configured `k` parameter.
    pub fn k(&self) -> u16 {
        self.k
    }

    /// Returns the configured rank accuracy.
    pub fn rank_accuracy(&self) -> RankAccuracy {
        self.rank_accuracy
    }

    /// Returns the total number of items observed.
    pub fn n(&self) -> u64 {
        self.n
    }

    /// Returns true if the sketch has observed no items.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Returns true if compaction has occurred.
    pub fn is_estimation_mode(&self) -> bool {
        self.compactors.len() > 1
    }

    /// Returns the number of items currently stored across all compactors.
    pub fn num_retained(&self) -> u32 {
        self.num_retained
    }

    /// Returns the smallest item ever observed, or `None` if empty.
    pub fn min_item(&self) -> Option<&T> {
        self.min_item.as_ref()
    }

    /// Returns the largest item ever observed, or `None` if empty.
    pub fn max_item(&self) -> Option<&T> {
        self.max_item.as_ref()
    }

    /// Updates the sketch with a new item.
    pub fn update(&mut self, item: T) {
        match &mut self.min_item {
            None => self.min_item = Some(item.clone()),
            Some(cur) if item.cmp(cur).is_lt() => *cur = item.clone(),
            _ => {}
        }
        match &mut self.max_item {
            None => self.max_item = Some(item.clone()),
            Some(cur) if item.cmp(cur).is_gt() => *cur = item.clone(),
            _ => {}
        }

        self.compactors[0].append(item);
        self.n += 1;
        self.num_retained += 1;

        if self.num_retained >= self.max_nom_size {
            self.compress();
        }
    }

    /// Resets the sketch to the empty state.
    pub fn reset(&mut self) {
        self.n = 0;
        self.num_retained = 0;
        self.max_nom_size = 0;
        self.min_item = None;
        self.max_item = None;
        self.compactors.clear();
        self.grow();
    }

    /// Returns an iterator over `(item, weight)` pairs.
    pub fn iter(&self) -> ReqSketchIterator<'_, T> {
        ReqSketchIterator::new(&self.compactors)
    }

    /// Returns the approximate rank of `item` in `[0.0, 1.0]`.
    ///
    /// Computed directly from the retained items in a single `O(retained)` pass,
    /// without building a sorted view. The result is identical to
    /// [`SortedView::rank`] on [`Self::sorted_view`].
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty.
    pub fn rank(&self, item: &T, criteria: SearchCriteria) -> Result<f64, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }
        let inclusive = matches!(criteria, SearchCriteria::Inclusive);
        let weight: u64 = self
            .compactors
            .iter()
            .map(|c| c.count_below(item, inclusive) as u64 * c.weight())
            .sum();
        Ok(weight as f64 / self.n as f64)
    }

    /// Returns the approximate quantile at the given normalized rank.
    ///
    /// Builds a transient [`SortedView`] internally. For repeated quantile
    /// queries, take one snapshot with [`Self::sorted_view`] and query it.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty or `rank` is outside `[0.0, 1.0]`.
    pub fn quantile(&self, rank: f64, criteria: SearchCriteria) -> Result<T, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }
        if !(0.0..=1.0).contains(&rank) {
            return Err(Error::invalid_argument(format!(
                "rank {rank} must be in [0, 1]"
            )));
        }
        self.sorted_view().quantile(rank, criteria)
    }

    /// Returns approximate quantiles for the given normalized ranks.
    ///
    /// The sorted view is built once and shared across all ranks.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty or any rank is outside `[0.0, 1.0]`.
    pub fn quantiles(&self, ranks: &[f64], criteria: SearchCriteria) -> Result<Vec<T>, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }
        // Reject invalid ranks before paying for the view build.
        for &r in ranks {
            if !(0.0..=1.0).contains(&r) {
                return Err(Error::invalid_argument(format!(
                    "rank {r} must be in [0, 1]"
                )));
            }
        }
        let view = self.sorted_view();
        ranks.iter().map(|&r| view.quantile(r, criteria)).collect()
    }

    /// Returns the Probability Mass Function over the given split points.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty or the split points are not strictly increasing.
    pub fn pmf(&self, split_points: &[T], criteria: SearchCriteria) -> Result<Vec<f64>, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }
        self.sorted_view().pmf(split_points, criteria)
    }

    /// Returns the Cumulative Distribution Function over the given split points.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketch is empty or the split points are not strictly increasing.
    pub fn cdf(&self, split_points: &[T], criteria: SearchCriteria) -> Result<Vec<f64>, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }
        self.sorted_view().cdf(split_points, criteria)
    }

    /// Returns an owned, sorted snapshot of the sketch's current state.
    ///
    /// An empty sketch yields an empty view; queries on it return an error. The
    /// view is independent of the sketch — it can be queried (and sent to other
    /// threads) while the sketch keeps receiving updates, and it keeps answering
    /// from the state it was taken at.
    ///
    /// Building the view costs `O(retained · log retained)`; each query on it is
    /// then `O(log retained)`. Prefer taking one view for repeated queries over
    /// calling [`Self::quantile`]/[`Self::pmf`]/[`Self::cdf`], which each build a
    /// transient view.
    pub fn sorted_view(&self) -> SortedView<T> {
        let mut weighted_items = Vec::with_capacity(self.num_retained as usize);
        for compactor in &self.compactors {
            let weight = compactor.weight();
            for item in compactor.iter() {
                weighted_items.push((item.clone(), weight));
            }
        }
        SortedView::new(weighted_items)
    }

    /// Merges another sketch into this one.
    ///
    /// # Errors
    ///
    /// Returns an error if the two sketches have different `rank_accuracy`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::req::ReqFloat;
    /// use datasketches::req::ReqSketch;
    ///
    /// let mut first = ReqSketch::default();
    /// first.update(ReqFloat::<f64>::new(1.0)?);
    ///
    /// let mut second = ReqSketch::default();
    /// second.update(ReqFloat::<f64>::new(2.0)?);
    ///
    /// let mut combined = ReqSketch::default();
    /// combined.merge(&first).unwrap();
    /// combined.merge(&second).unwrap();
    ///
    /// assert_eq!(combined.n(), 2);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn merge(&mut self, other: &Self) -> Result<(), Error> {
        if self.rank_accuracy != other.rank_accuracy {
            return Err(Error::invalid_argument(
                "sketches must have the same rank_accuracy",
            ));
        }

        if other.is_empty() {
            return Ok(());
        }

        self.n += other.n;

        if let Some(m) = &other.min_item {
            match &self.min_item {
                None => self.min_item = Some(m.clone()),
                Some(cur) if m.cmp(cur).is_lt() => self.min_item = Some(m.clone()),
                _ => {}
            }
        }
        if let Some(m) = &other.max_item {
            match &self.max_item {
                None => self.max_item = Some(m.clone()),
                Some(cur) if m.cmp(cur).is_gt() => self.max_item = Some(m.clone()),
                _ => {}
            }
        }

        while self.compactors.len() < other.compactors.len() {
            self.grow();
        }

        for (i, other_c) in other.compactors.iter().enumerate() {
            self.compactors[i].merge(other_c);
        }

        self.update_max_nom_size();
        self.update_num_retained();

        if self.num_retained >= self.max_nom_size {
            self.compress();
        }

        Ok(())
    }

    /// Returns the lower bound for the normalized `rank` at the requested confidence level.
    pub fn rank_lower_bound(&self, rank: f64, num_std_dev: NumStdDev) -> f64 {
        self.compute_rank_lower_bound(
            self.k,
            self.compactors.len() as u8,
            rank,
            num_std_dev.as_u8(),
            self.n,
            matches!(self.rank_accuracy, RankAccuracy::HighRank),
        )
    }

    /// Returns the upper bound for the normalized `rank` at the requested confidence level.
    pub fn rank_upper_bound(&self, rank: f64, num_std_dev: NumStdDev) -> f64 {
        self.compute_rank_upper_bound(
            self.k,
            self.compactors.len() as u8,
            rank,
            num_std_dev.as_u8(),
            self.n,
            matches!(self.rank_accuracy, RankAccuracy::HighRank),
        )
    }

    const FIXED_RSE_FACTOR: f64 = 0.084;
    fn relative_rse_factor() -> f64 {
        (0.0512 / INITIAL_SECTIONS_PER_COMPACTOR as f64).sqrt()
    }

    fn compute_rank_lower_bound(
        &self,
        k: u16,
        num_levels: u8,
        rank: f64,
        num_std_dev: u8,
        n: u64,
        hra: bool,
    ) -> f64 {
        if self.is_exact_rank_threshold(k, num_levels, rank, n, hra) {
            return rank;
        }
        let relative = Self::relative_rse_factor() / k as f64 * if hra { 1.0 - rank } else { rank };
        let fixed = Self::FIXED_RSE_FACTOR / k as f64;
        let lb_rel = rank - num_std_dev as f64 * relative;
        let lb_fix = rank - num_std_dev as f64 * fixed;
        lb_rel.max(lb_fix).max(0.0)
    }

    fn compute_rank_upper_bound(
        &self,
        k: u16,
        num_levels: u8,
        rank: f64,
        num_std_dev: u8,
        n: u64,
        hra: bool,
    ) -> f64 {
        if self.is_exact_rank_threshold(k, num_levels, rank, n, hra) {
            return rank;
        }
        let relative = Self::relative_rse_factor() / k as f64 * if hra { 1.0 - rank } else { rank };
        let fixed = Self::FIXED_RSE_FACTOR / k as f64;
        let ub_rel = rank + num_std_dev as f64 * relative;
        let ub_fix = rank + num_std_dev as f64 * fixed;
        ub_rel.min(ub_fix).min(1.0)
    }

    fn is_exact_rank_threshold(
        &self,
        k: u16,
        num_levels: u8,
        rank: f64,
        n: u64,
        hra: bool,
    ) -> bool {
        let base_cap = k as u64 * INITIAL_SECTIONS_PER_COMPACTOR as u64;
        if num_levels == 1 || n <= base_cap {
            return true;
        }
        let exact_rank_thresh = base_cap as f64 / n as f64;
        if hra {
            rank >= 1.0 - exact_rank_thresh
        } else {
            rank <= exact_rank_thresh
        }
    }

    fn flags_byte(&self) -> u8 {
        let mut flags = 0u8;
        if self.is_empty() {
            flags |= FLAG_IS_EMPTY;
        }
        if matches!(self.rank_accuracy, RankAccuracy::HighRank) {
            flags |= FLAG_IS_HIGH_RANK;
        }
        if self.is_raw_items() {
            flags |= FLAG_RAW_ITEMS;
        }
        if self.compactors[0].is_sorted() {
            flags |= FLAG_IS_LEVEL_ZERO_SORTED;
        }
        flags
    }

    fn is_raw_items(&self) -> bool {
        self.n <= RAW_ITEMS_THRESHOLD && self.compactors.len() == 1
    }

    /// Returns the number of bytes required to serialize the sketch.
    pub fn serialized_size_bytes(&self) -> usize
    where
        T: ReqValue,
    {
        // Fixed sketch preamble: 8 bytes (preamble_ints, serial_version, family,
        // flags, k(2), num_levels, num_raw_items).
        let mut size = 8;
        if self.is_empty() {
            return size;
        }
        if self.is_estimation_mode() {
            size += 8; // n
            size += T::serialize_size(self.min_item.as_ref().unwrap());
            size += T::serialize_size(self.max_item.as_ref().unwrap());
        }
        if self.is_raw_items() {
            for item in self.compactors[0].iter() {
                size += T::serialize_size(item);
            }
        } else {
            for c in &self.compactors {
                // 20-byte compactor preamble + items
                size += 20;
                for item in c.iter() {
                    size += T::serialize_size(item);
                }
            }
        }
        size
    }

    /// Serializes the sketch using the C++/Java-compatible REQ wire format.
    pub fn serialize(&self) -> Vec<u8>
    where
        T: ReqValue,
    {
        let mut out = SketchBytes::with_capacity(self.serialized_size_bytes());
        let preamble_ints = if self.is_estimation_mode() {
            PREAMBLE_INTS_ESTIMATION
        } else {
            PREAMBLE_INTS_EXACT
        };
        out.write_u8(preamble_ints);
        out.write_u8(SERIAL_VERSION);
        out.write_u8(Family::REQ.id);
        out.write_u8(self.flags_byte());
        out.write_u16_le(self.k);
        let num_levels = if self.is_empty() {
            0
        } else {
            self.compactors.len() as u8
        };
        out.write_u8(num_levels);
        let num_raw_items = if self.is_raw_items() { self.n as u8 } else { 0 };
        out.write_u8(num_raw_items);

        if self.is_empty() {
            return out.into_bytes();
        }

        if self.is_estimation_mode() {
            out.write_u64_le(self.n);
            self.min_item.as_ref().unwrap().serialize_value(&mut out);
            self.max_item.as_ref().unwrap().serialize_value(&mut out);
        }

        if self.is_raw_items() {
            for item in self.compactors[0].iter() {
                item.serialize_value(&mut out);
            }
        } else {
            for c in &self.compactors {
                c.serialize_into(&mut out);
            }
        }

        out.into_bytes()
    }

    /// Deserialize a sketch from bytes produced by [`Self::serialize`] or by the
    /// C++/Java reference implementations.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is truncated or contains an inconsistent
    /// REQ serialized state.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error>
    where
        T: ReqValue,
    {
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
        let num_levels = cursor.read_u8().map_err(insufficient_data("num_levels"))?;
        let num_raw_items = cursor
            .read_u8()
            .map_err(insufficient_data("num_raw_items"))?;

        check_preamble_ints(preamble_ints, num_levels)?;
        check_serial_version(serial_version)?;
        Family::REQ.validate_id(family_id)?;

        let is_empty = flags & FLAG_IS_EMPTY != 0;
        let hra = flags & FLAG_IS_HIGH_RANK != 0;
        let raw_items = flags & FLAG_RAW_ITEMS != 0;
        let is_level_zero_sorted = flags & FLAG_IS_LEVEL_ZERO_SORTED != 0;

        let rank_accuracy = if hra {
            RankAccuracy::HighRank
        } else {
            RankAccuracy::LowRank
        };

        if !(MIN_K..=MAX_K).contains(&k) {
            return Err(Error::deserial(format!(
                "k must be in [{MIN_K}, {MAX_K}], got {k}"
            )));
        }
        if k % 2 != 0 {
            return Err(Error::deserial(format!("k must be even, got {k}")));
        }

        if is_empty {
            if num_levels != 0 {
                return Err(Error::deserial(format!(
                    "empty REQ sketch must have 0 levels, got {num_levels}"
                )));
            }
            if num_raw_items != 0 {
                return Err(Error::deserial(format!(
                    "empty REQ sketch must have 0 raw items, got {num_raw_items}"
                )));
            }
            return Ok(Self::make(k, rank_accuracy));
        }

        if num_levels == 0 {
            return Err(Error::deserial(
                "non-empty REQ sketch must have at least one level",
            ));
        }
        if num_levels > 64 {
            return Err(Error::deserial(
                "REQ sketch cannot have more than 64 levels",
            ));
        }

        if raw_items {
            if num_levels != 1 {
                return Err(Error::deserial(format!(
                    "raw-items REQ sketch must have exactly 1 level, got {num_levels}"
                )));
            }
            if num_raw_items == 0 || num_raw_items as u64 > RAW_ITEMS_THRESHOLD {
                return Err(Error::deserial(format!(
                    "raw-items REQ sketch must contain 1..={RAW_ITEMS_THRESHOLD} items, got {num_raw_items}"
                )));
            }
        } else if num_raw_items != 0 {
            return Err(Error::deserial(format!(
                "non-raw REQ sketch must have 0 raw items, got {num_raw_items}"
            )));
        }

        let mut min_item: Option<T> = None;
        let mut max_item: Option<T> = None;
        let mut n: u64 = 1;

        if num_levels > 1 {
            n = cursor.read_u64_le().map_err(insufficient_data("n"))?;
            min_item = Some(T::deserialize_value(&mut cursor)?);
            max_item = Some(T::deserialize_value(&mut cursor)?);
            let min = min_item.as_ref().unwrap();
            let max = max_item.as_ref().unwrap();
            if min > max {
                return Err(Error::deserial(
                    "REQ sketch min item is greater than max item",
                ));
            }
        }

        let mut compactors: Vec<Compactor<T>> = Vec::with_capacity(num_levels as usize);

        if raw_items {
            // Single compactor at level 0; items follow directly.
            let mut items = Vec::with_capacity(num_raw_items as usize);
            for _ in 0..num_raw_items {
                items.push(T::deserialize_value(&mut cursor)?);
            }
            let c =
                Compactor::<T>::raw_items_compactor(k, rank_accuracy, items, is_level_zero_sorted);
            compactors.push(c);
        } else {
            for i in 0..num_levels {
                let level_sorted = i > 0 || is_level_zero_sorted;
                let c =
                    Compactor::<T>::deserialize(&mut cursor, k, i, rank_accuracy, level_sorted)?;
                compactors.push(c);
            }
        }

        if num_levels == 1 {
            // Recover n / min / max from level 0 (these aren't in the preamble for exact mode).
            let level0 = &compactors[0];
            n = level0.num_items() as u64;
            let mut iter = level0.iter();
            if let Some(first) = iter.next() {
                let mut mn = first.clone();
                let mut mx = first.clone();
                for x in iter {
                    if x < &mn {
                        mn = x.clone();
                    }
                    if x > &mx {
                        mx = x.clone();
                    }
                }
                min_item = Some(mn);
                max_item = Some(mx);
            }
        }

        if n == 0 {
            return Err(Error::deserial("non-empty REQ sketch contains no items"));
        }
        let (Some(min), Some(max)) = (&min_item, &max_item) else {
            return Err(Error::deserial("non-empty REQ sketch contains no items"));
        };
        if compactors
            .iter()
            .flat_map(Compactor::iter)
            .any(|item| item < min || item > max)
        {
            return Err(Error::deserial(
                "REQ retained item falls outside the min/max range",
            ));
        }

        let expected_raw_items = num_levels == 1 && n <= RAW_ITEMS_THRESHOLD;
        if raw_items != expected_raw_items {
            return Err(Error::deserial(
                "REQ sketch RAW_ITEMS flag is inconsistent with num_levels and n",
            ));
        }

        let (retained_count, nominal_capacity, weighted_count) = compactors
            .iter()
            .try_fold(
                (0u32, 0u32, 0u64),
                |(retained, capacity, weighted), compactor| {
                    Some((
                        retained.checked_add(compactor.num_items())?,
                        capacity.checked_add(compactor.nominal_capacity())?,
                        weighted.checked_add(
                            (compactor.num_items() as u64).checked_mul(compactor.weight())?,
                        )?,
                    ))
                },
            )
            .ok_or_else(|| Error::deserial("REQ compactor totals overflow"))?;
        if weighted_count != n {
            return Err(Error::deserial(format!(
                "REQ retained weighted count {weighted_count} does not match n {n}"
            )));
        }
        let mut sketch = Self::make(k, rank_accuracy);
        sketch.n = n;
        sketch.min_item = min_item;
        sketch.max_item = max_item;
        sketch.compactors = compactors;
        sketch.max_nom_size = nominal_capacity;
        sketch.num_retained = retained_count;
        Ok(sketch)
    }

    fn make(k: u16, rank_accuracy: RankAccuracy) -> Self {
        debug_assert!(
            (MIN_K..=MAX_K).contains(&k),
            "k must be in [{MIN_K}, {MAX_K}], got {k}"
        );
        debug_assert_eq!(k % 2, 0, "k must be even, got {k}");
        let mut sketch = Self {
            k,
            rank_accuracy,
            n: 0,
            max_nom_size: 0,
            num_retained: 0,
            compactors: vec![],
            promotion_buf: Vec::with_capacity(k as usize),
            min_item: None,
            max_item: None,
        };
        // C++ parity: an empty sketch has a level-0 compactor present from the start.
        // This makes is_raw_items() and flags_byte() byte-compatible with the C++/Java
        // wire format for the empty case.
        sketch.grow();
        sketch
    }

    fn grow(&mut self) {
        let level = self.compactors.len() as u8;
        let compactor = Compactor::new(level, self.k, self.rank_accuracy);
        self.compactors.push(compactor);
        self.update_max_nom_size();
    }

    fn compress(&mut self) {
        for h in 0..self.compactors.len() {
            if self.compactors[h].num_items() >= self.compactors[h].nominal_capacity() {
                if h == 0 {
                    self.compactors[0].sort();
                }
                if h + 1 >= self.compactors.len() {
                    self.grow();
                }
                self.promotion_buf.clear();
                self.compactors[h].compact_into(self.rank_accuracy, &mut self.promotion_buf);
                if !self.promotion_buf.is_empty() {
                    self.compactors[h + 1].sort();
                    self.compactors[h + 1].merge_sorted(&self.promotion_buf);
                }
                self.update_max_nom_size();
                self.update_num_retained();
            }
        }
    }

    fn update_max_nom_size(&mut self) {
        self.max_nom_size = self.compactors.iter().map(|c| c.nominal_capacity()).sum();
    }

    fn update_num_retained(&mut self) {
        self.num_retained = self.compactors.iter().map(|c| c.num_items()).sum();
    }
}
