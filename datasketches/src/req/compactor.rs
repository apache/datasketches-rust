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

//! Compactor implementation for REQ sketch levels.
//!
//! Each level in the REQ sketch uses a compactor to maintain a bounded set of items
//! with deterministic compaction when capacity is exceeded.

use crate::error::Error;
use crate::req::INITIAL_SECTIONS_PER_COMPACTOR;
use crate::req::MIN_K;
use crate::req::RankAccuracy;
use crate::req::compare;
use crate::req::is_self_comparable;
use crate::req::nearest_even_section_size;
use crate::req::serialization::validate_compactor_state;
use crate::req::value::ReqValue;

fn normalized_sort_state<T: PartialOrd>(items: &[T], claimed_sorted: bool) -> Result<bool, Error> {
    let mut previous: Option<&T> = None;
    let mut sorted = claimed_sorted;
    for item in items {
        if !is_self_comparable(item) {
            return Err(Error::deserial("REQ compactor item is self-incomparable"));
        }
        if sorted && previous.is_some_and(|previous| compare(previous, item).is_gt()) {
            sorted = false;
        }
        previous = Some(item);
    }
    Ok(sorted)
}

/// A compactor maintains items at a specific level of the REQ sketch.
///
/// When the compactor reaches its nominal capacity, it performs compaction
/// by keeping approximately half the items and promoting the rest to the next level.
#[derive(Debug, Clone)]
pub(super) struct Compactor<T> {
    /// Current items in the compactor
    items: Vec<T>,
    /// Whether items are currently sorted
    is_sorted: bool,
    /// State for deterministic compaction
    state: u64,
    /// Reusable scratch buffer for compaction operations
    scratch_buffer: Vec<T>,

    /// Actual section size (rounded to integer)
    section_size: u32,
    /// Number of sections in this compactor
    num_sections: u8,
    /// The level of this compactor (0 = base level)
    lg_weight: u8,

    /// Whether this compactor is configured for high rank accuracy
    rank_accuracy: RankAccuracy,
    /// Raw section size (may be fractional)
    section_size_raw: f32,
    /// Random bit for compaction
    coin: bool,
}

impl<T> Compactor<T>
where
    T: Clone + PartialOrd,
{
    /// Creates a new compactor for the given level.
    ///
    /// # Arguments
    /// * `lg_weight` - The level (log weight) of this compactor
    /// * `k` - The k parameter from the parent sketch
    /// * `rank_accuracy` - Rank accuracy configuration
    pub(super) fn new(lg_weight: u8, k: u16, rank_accuracy: RankAccuracy) -> Self {
        let section_size_raw = k as f32;
        let section_size = nearest_even_section_size(section_size_raw);
        let num_sections = INITIAL_SECTIONS_PER_COMPACTOR;

        let nominal: usize = (2 * section_size * num_sections as u32) as usize;

        Self {
            items: Vec::with_capacity(nominal),
            is_sorted: true,
            state: 0,
            scratch_buffer: Vec::with_capacity(nominal / 2 + 8),

            section_size,
            num_sections,
            lg_weight,

            rank_accuracy,
            section_size_raw,
            coin: false,
        }
    }

    /// Returns the number of items currently in this compactor.
    pub(super) fn num_items(&self) -> u32 {
        self.items.len() as u32
    }

    /// Returns the nominal capacity of this compactor.
    pub(super) fn nominal_capacity(&self) -> u32 {
        2 * self.section_size * self.num_sections as u32
    }

    /// Returns whether the items are currently sorted.
    pub(super) fn is_sorted(&self) -> bool {
        self.is_sorted
    }

    /// Appends an item to this compactor.
    #[inline(always)]
    pub(super) fn append(&mut self, item: T) {
        self.items.push(item);
        if self.items.len() > 1 {
            self.is_sorted = false;
        }
    }

    /// Merges items from another compactor into this one.
    pub(super) fn merge(&mut self, other: &Self) {
        debug_assert_eq!(self.lg_weight, other.lg_weight);
        self.state |= other.state;
        if !other.items.is_empty() {
            self.sort();
            if other.is_sorted {
                self.merge_sorted(&other.items);
            } else {
                let mut other_items = other.items.clone();
                other_items.sort_unstable_by(compare);
                self.merge_sorted(&other_items);
            }
        }
        // OR-ing the schedule counters can advance state past several doubling
        // thresholds at once. Loop until no more doublings are needed (C++:
        // req_compactor_impl.hpp:250 — `while (ensure_enough_sections()) {}`).
        while self.ensure_enough_sections() {}
    }

    /// Counts the items at-or-below (`inclusive`) or strictly below `item`.
    ///
    /// Uses binary search when this compactor is sorted, and a linear scan
    /// otherwise. This lets [`ReqSketch::rank`](crate::req::ReqSketch::rank) sum
    /// per-level weights directly without first building a sorted view.
    pub(super) fn count_below(&self, item: &T, inclusive: bool) -> usize {
        if self.is_sorted {
            if inclusive {
                self.items.partition_point(|x| compare(x, item).is_le())
            } else {
                self.items.partition_point(|x| compare(x, item).is_lt())
            }
        } else {
            self.items
                .iter()
                .filter(|x| {
                    let ord = compare(*x, item);
                    if inclusive { ord.is_le() } else { ord.is_lt() }
                })
                .count()
        }
    }

    /// Merges pre-sorted items into this compactor.
    /// Merges sorted items into this compactor using scratch buffer to avoid allocation.
    /// Both this compactor's items and the input must be sorted.
    #[inline(always)]
    pub(super) fn merge_sorted(&mut self, items: &[T]) {
        if items.is_empty() {
            return;
        }

        if self.items.is_empty() {
            self.items.extend_from_slice(items);
            self.is_sorted = true;
            return;
        }

        // Ensure sorted on both inputs by contract
        let total = self.items.len() + items.len();
        self.scratch_buffer.clear();
        if self.scratch_buffer.capacity() < total {
            self.scratch_buffer
                .reserve(total - self.scratch_buffer.capacity());
        }

        let (mut i, mut j) = (0usize, 0usize);
        let (a, b) = (&self.items, items);

        // Two-pointer merge into scratch buffer
        while i < a.len() && j < b.len() {
            if compare(&a[i], &b[j]).is_le() {
                self.scratch_buffer.push(a[i].clone());
                i += 1;
            } else {
                self.scratch_buffer.push(b[j].clone());
                j += 1;
            }
        }

        // Add remaining elements
        if i < a.len() {
            self.scratch_buffer.extend_from_slice(&a[i..]);
        }
        if j < b.len() {
            self.scratch_buffer.extend_from_slice(&b[j..]);
        }

        // Swap scratch buffer with items (zero-copy)
        self.items.clear();
        std::mem::swap(&mut self.items, &mut self.scratch_buffer);
        self.is_sorted = true;
    }

    /// Sorts the items in this compactor if not already sorted.
    #[inline(always)]
    pub(super) fn sort(&mut self) {
        if !self.is_sorted {
            // Use unstable sort for better performance (stable not needed for REQ sketch)
            self.items.sort_unstable_by(compare);
            self.is_sorted = true;
        }
    }

    /// Compacts into the provided output buffer without allocating.
    /// Writes promoted items into `out` and removes the compacted range in-place via `copy_within +
    /// truncate`.
    #[inline(always)]
    pub(super) fn compact_into(&mut self, _rank_accuracy: RankAccuracy, out: &mut Vec<T>) {
        if self.items.is_empty() {
            out.clear();
            return;
        }

        // Sort entire buffer (C++ sorts full buffer before compaction)
        self.sort();

        // Calculate sections to compact based on state
        let secs_to_compact =
            ((!self.state).trailing_zeros() + 1).min(self.num_sections as u32) as u8;
        let compaction_range = self.compute_compaction_range(secs_to_compact);

        // Must have at least 2 items to compact
        if compaction_range.1 <= compaction_range.0 || (compaction_range.1 - compaction_range.0) < 2
        {
            out.clear();
            return;
        }

        if (self.state & 1) == 1 {
            self.coin = !self.coin; // flip coin for odd states
        } else {
            self.coin = rand::random::<bool>(); // random coin flip for even states
        }
        let odds = self.coin;

        // Build promoted items directly into output buffer (no alloc)
        out.clear();
        let (start, end) = compaction_range;
        let mut i = start + if odds { 1 } else { 0 };
        while i < end {
            out.push(self.items[i].clone()); // TODO: use Copy fast-path for numeric types
            i += 2;
        }

        // Remove the compacted range in-place by rotating elements left
        let removed = end - start;
        if end < self.items.len() {
            // Use rotate_left to move tail elements to fill the gap
            self.items[start..].rotate_left(removed);
        }
        self.items.truncate(self.items.len() - removed);

        // Update state, then ensure enough sections (C++ order)
        self.state = self.state.wrapping_add(1);
        self.ensure_enough_sections();
    }

    /// Returns an iterator over the items in this compactor.
    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }

    /// Returns a slice of items for zero-allocation iteration.
    pub(super) fn items_slice(&self) -> &[T] {
        &self.items
    }

    /// Returns the weight (2^lg_weight) for items in this compactor.
    pub(super) fn weight(&self) -> u64 {
        1u64 << self.lg_weight
    }

    // Private helper methods

    fn ensure_enough_sections(&mut self) -> bool {
        let Some(threshold) = self
            .num_sections
            .checked_sub(1)
            .and_then(|shift| 1u64.checked_shl(u32::from(shift)))
        else {
            return false;
        };
        let Some(num_sections) = self.num_sections.checked_mul(2) else {
            return false;
        };
        let section_size_raw = self.section_size_raw / std::f32::consts::SQRT_2;
        let section_size = nearest_even_section_size(section_size_raw);

        if self.state >= threshold && section_size >= u32::from(MIN_K) {
            self.section_size_raw = section_size_raw;
            self.section_size = section_size;
            self.num_sections = num_sections;
            return true;
        }
        false
    }

    #[inline(always)]
    fn compute_compaction_range(&self, secs_to_compact: u8) -> (usize, usize) {
        let nom_capacity = self.nominal_capacity() as usize;
        let mut non_compact = nom_capacity / 2
            + (self.num_sections - secs_to_compact) as usize * self.section_size as usize;

        // if (((num_items_ - non_compact) & 1) == 1) ++non_compact;
        if self.items.len() >= non_compact && ((self.items.len() - non_compact) & 1) == 1 {
            non_compact += 1;
        }

        let (low, high) = match self.rank_accuracy {
            RankAccuracy::HighRank => {
                // HRA: Protect high ranks by compacting LOW sections (low values)
                // This means we compact from [0, num_items - non_compact] (bottom end)
                let high = if self.items.len() >= non_compact {
                    self.items.len() - non_compact
                } else {
                    0
                };
                (0, high)
            }
            RankAccuracy::LowRank => {
                // LRA: Protect low ranks by compacting HIGH sections (high values)
                // This means we compact from [non_compact, num_items] (top end)
                let low = non_compact.min(self.items.len());
                (low, self.items.len())
            }
        };

        // Empty window safety: ensure we have at least 2 items to compact
        if high <= low || (high - low) < 2 {
            return (0, 0); // Signal no compaction needed
        }

        (low, high)
    }

    /// Serialize this compactor (preamble + items) into the byte buffer.
    pub(super) fn serialize_into(&self, bytes: &mut crate::codec::SketchBytes)
    where
        T: ReqValue,
    {
        bytes.write_u64_le(self.state);
        bytes.write_f32_le(self.section_size_raw);
        bytes.write_u8(self.lg_weight);
        bytes.write_u8(self.num_sections);
        bytes.write_u16_le(0); // padding
        bytes.write_u32_le(self.num_items());
        for item in self.iter() {
            item.serialize_value(bytes);
        }
    }

    /// Deserialize a compactor (preamble + items) from the byte cursor.
    pub(super) fn deserialize(
        cursor: &mut crate::codec::SketchSlice<'_>,
        k: u16,
        expected_lg_weight: u8,
        rank_accuracy: RankAccuracy,
        sorted: bool,
    ) -> Result<Self, crate::error::Error>
    where
        T: ReqValue,
    {
        use crate::codec::assert::insufficient_data;
        let state = cursor
            .read_u64_le()
            .map_err(insufficient_data("compactor.state"))?;
        let section_size_raw = cursor
            .read_f32_le()
            .map_err(insufficient_data("compactor.section_size_raw"))?;
        let lg_weight = cursor
            .read_u8()
            .map_err(insufficient_data("compactor.lg_weight"))?;
        let num_sections = cursor
            .read_u8()
            .map_err(insufficient_data("compactor.num_sections"))?;
        let _padding = cursor
            .read_u16_le()
            .map_err(insufficient_data("compactor.padding"))?;
        let num_items = cursor
            .read_u32_le()
            .map_err(insufficient_data("compactor.num_items"))?;

        validate_compactor_state(
            k,
            expected_lg_weight,
            state,
            section_size_raw,
            lg_weight,
            num_sections,
        )?;

        // Don't trust `num_items` for the allocation: a malformed length could request
        // a multi-gigabyte reservation before the per-item reads below fail. The buffer
        // holds at most `remaining` more items (each item is ≥ 1 byte), so cap the
        // pre-allocation there; `push` still grows the Vec as the validated data needs.
        let capacity = (num_items as usize).min(cursor.remaining().len());
        let mut items = Vec::with_capacity(capacity);
        for _ in 0..num_items {
            items.push(T::deserialize_value(cursor)?);
        }
        let sorted = normalized_sort_state(&items, sorted)?;

        Ok(Compactor::from_serialized_state(
            lg_weight,
            section_size_raw,
            num_sections,
            state,
            items,
            sorted,
            rank_accuracy,
        ))
    }

    /// Build a level-0 compactor from raw items (used by the `RAW_ITEMS` deserialize path).
    ///
    /// The wire format omits the compactor preamble for tiny sketches (n ≤ 4); this
    /// helper synthesises a fresh compactor and seeds it with the deserialized items.
    /// `is_sorted` is taken verbatim from the wire — both C++ and Java produce valid
    /// sketches but disagree on this flag for n=1 (C++ sets true, Java sets false),
    /// so faithful round-trip requires preserving whatever the input said.
    pub(super) fn raw_items_compactor(
        k: u16,
        rank_accuracy: RankAccuracy,
        items: Vec<T>,
        is_sorted: bool,
    ) -> Result<Self, Error> {
        let is_sorted = normalized_sort_state(&items, is_sorted)?;
        let mut c = Self::new(0, k, rank_accuracy);
        for item in items {
            c.append(item);
        }
        // append() may have flipped is_sorted off; restore the wire flag verbatim.
        c.is_sorted = is_sorted;
        Ok(c)
    }

    /// Reconstruct a Compactor from deserialized state.
    ///
    /// Used by [`Compactor::deserialize`]. Transient state (random coin, scratch
    /// buffer) is reset; the deterministic `state` counter and the persistent
    /// configuration (`lg_weight`, `section_size_raw`, `num_sections`) are preserved
    /// from the wire data.
    pub(super) fn from_serialized_state(
        lg_weight: u8,
        section_size_raw: f32,
        num_sections: u8,
        state: u64,
        items: Vec<T>,
        is_sorted: bool,
        rank_accuracy: RankAccuracy,
    ) -> Self {
        Self {
            items,
            is_sorted,
            state,
            scratch_buffer: vec![],
            section_size: nearest_even_section_size(section_size_raw),
            num_sections,
            lg_weight,
            rank_accuracy,
            section_size_raw,
            coin: false,
        }
    }
}

#[cfg(test)]
impl<T> Compactor<T>
where
    T: Clone + PartialOrd,
{
    /// Returns the level (log weight) of this compactor. Test-only accessor.
    pub(super) fn lg_weight(&self) -> u8 {
        self.lg_weight
    }

    /// Returns the current state for deterministic compaction. Test-only accessor.
    pub(super) fn state(&self) -> u64 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use googletest::assert_that;
    use googletest::prelude::ge;

    use super::*;

    #[test]
    fn test_new_compactor() {
        let compactor: Compactor<i32> = Compactor::new(0, 12, RankAccuracy::HighRank);
        assert_eq!(compactor.lg_weight(), 0);
        assert_eq!(compactor.num_items(), 0);
        assert!(compactor.is_sorted());
        assert_eq!(compactor.weight(), 1);
    }

    #[test]
    fn test_append_and_sort() {
        let mut compactor = Compactor::new(0, 12, RankAccuracy::HighRank);

        compactor.append(5);
        assert_eq!(compactor.num_items(), 1);
        assert!(compactor.is_sorted()); // Single item is sorted

        compactor.append(3);
        assert_eq!(compactor.num_items(), 2);
        assert!(!compactor.is_sorted()); // Multiple items, not sorted

        compactor.sort();
        assert!(compactor.is_sorted());

        let items: Vec<&i32> = compactor.iter().collect();
        assert_eq!(items, vec![&3, &5]);
    }

    #[test]
    fn test_nearest_even_section_size() {
        assert_eq!(nearest_even_section_size(0.0), 0); // 0/2=0, round(0)=0, 0<<1=0
        assert_eq!(nearest_even_section_size(1.0), 2); // 1/2=0.5, round(0.5)=1, 1<<1=2
        assert_eq!(nearest_even_section_size(2.0), 2); // 2/2=1, round(1)=1, 1<<1=2
        assert_eq!(nearest_even_section_size(3.0), 4); // 3/2=1.5, round(1.5)=2, 2<<1=4
        assert_eq!(nearest_even_section_size(4.0), 4); // 4/2=2, round(2)=2, 2<<1=4
        assert_eq!(nearest_even_section_size(4.6), 4); // 4.6/2=2.3, round(2.3)=2, 2<<1=4
        assert_eq!(nearest_even_section_size(5.6), 6); // 5.6/2=2.8, round(2.8)=3, 3<<1=6
        assert_eq!(nearest_even_section_size(13.0), 14); // 13/2=6.5, round(6.5)=7, 7<<1=14
    }

    #[test]
    fn test_merge_sorted() {
        let mut compactor = Compactor::new(0, 12, RankAccuracy::HighRank);

        compactor.append(1);
        compactor.append(3);
        compactor.append(5);
        compactor.sort();

        let other_items = vec![2, 4, 6];
        compactor.merge_sorted(&other_items);

        assert!(compactor.is_sorted());
        let items: Vec<&i32> = compactor.iter().collect();
        assert_eq!(items, vec![&1, &2, &3, &4, &5, &6]);
    }

    #[test]
    fn compactor_serialization_round_trip() {
        use crate::codec::SketchBytes;
        use crate::codec::SketchSlice;

        let mut c: Compactor<f32> = Compactor::new(0, 12, RankAccuracy::HighRank);
        for i in 0..30 {
            c.append(i as f32);
        }
        c.sort();

        let mut bytes = SketchBytes::with_capacity(256);
        c.serialize_into(&mut bytes);
        let raw = bytes.into_bytes();

        let mut cursor = SketchSlice::new(&raw);
        let c2 = Compactor::<f32>::deserialize(&mut cursor, 12, 0, RankAccuracy::HighRank, true)
            .unwrap();

        assert_eq!(c.num_items(), c2.num_items());
        assert_eq!(c.lg_weight(), c2.lg_weight());
        assert_eq!(c.state(), c2.state());
        let xs: Vec<f32> = c.iter().copied().collect();
        let ys: Vec<f32> = c2.iter().copied().collect();
        assert_eq!(xs, ys);
    }

    #[test]
    fn merge_loops_ensure_enough_sections_for_high_state() {
        // Regression test for the bug where Compactor::merge called
        // ensure_enough_sections() once instead of looping. Without the loop,
        // num_sections doubles at most once per merge — but OR-ing a high state
        // can advance past several doubling thresholds at once and require
        // multiple doublings (matching the C++ reference at
        // req_compactor_impl.hpp:250 — `while (ensure_enough_sections()) {}`).
        //
        // Setup: a fresh compactor (state=0, num_sections=3) merged with another
        // whose state is 0xFFFF. After merge, state |= 0xFFFF = 0xFFFF.
        // ensure_enough_sections doublings (k=12, section_size_raw=12):
        //   - state=0xFFFF >= (1<<2)=4    ✓ → num_sections=6,  ssr≈8.49
        //   - state=0xFFFF >= (1<<5)=32   ✓ → num_sections=12, ssr≈6.00
        //   - state=0xFFFF >= (1<<11)=2048 ✓ → num_sections=24, ssr≈4.24
        //   - state=0xFFFF >= (1<<23)=8388608 ✗ → stop
        // Expected: num_sections == 24 with the fix; == 6 with only one call.
        let mut a: Compactor<f64> = Compactor::new(0, 12, RankAccuracy::HighRank);
        let mut b: Compactor<f64> = Compactor::new(0, 12, RankAccuracy::HighRank);
        b.state = 0xFFFF;

        assert_eq!(a.num_sections, 3, "default num_sections sanity");

        a.merge(&b);

        assert_that!(a.num_sections, ge(12));
    }
}
