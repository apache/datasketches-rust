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
//! Each level compacts a selected range, retaining one item from each pair at twice the weight.

use crate::common::random::random_bit;
use crate::error::Error;
use crate::req::INITIAL_SECTIONS_PER_COMPACTOR;
use crate::req::MIN_K;
use crate::req::RankAccuracy;
use crate::req::nearest_even_section_size;
use crate::req::serialization::validate_compactor_state;
use crate::req::value::ReqValue;

/// A compactor maintains items at a specific level of the REQ sketch.
///
/// Compaction preserves the configured accuracy tail and promotes samples from the other tail.
#[derive(Debug, Clone)]
pub struct Compactor<T> {
    items: Vec<T>,
    is_sorted: bool,
    /// Determines which sections are compacted and when their count doubles.
    state: u64,
    scratch_buffer: Vec<T>,
    section_size: u32,
    num_sections: u8,
    lg_weight: u8,
    rank_accuracy: RankAccuracy,
    /// Preserves fractional section sizes across successive divisions by sqrt(2).
    section_size_raw: f32,
    coin: bool,
}

impl<T> Compactor<T>
where
    T: Clone + Ord,
{
    /// Creates a new compactor for the given level.
    pub fn new(lg_weight: u8, k: u16, rank_accuracy: RankAccuracy) -> Self {
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
    pub fn num_items(&self) -> u32 {
        self.items.len() as u32
    }

    /// Returns the nominal capacity of this compactor.
    pub fn nominal_capacity(&self) -> u32 {
        2 * self.section_size * self.num_sections as u32
    }

    pub fn allocated_size(&self) -> usize {
        self.items.capacity() * size_of::<T>() + self.scratch_buffer.capacity() * size_of::<T>()
    }

    /// Returns whether the items are currently sorted.
    pub fn is_sorted(&self) -> bool {
        self.is_sorted
    }

    /// Appends an item to this compactor.
    #[inline(always)]
    pub fn append(&mut self, item: T) {
        self.items.push(item);
        if self.items.len() > 1 {
            self.is_sorted = false;
        }
    }

    /// Merges items from another compactor into this one.
    pub fn merge(&mut self, other: &Self) {
        debug_assert_eq!(self.lg_weight, other.lg_weight);
        self.state |= other.state;
        if !other.items.is_empty() {
            self.sort();
            if other.is_sorted {
                self.merge_sorted(&other.items);
            } else {
                let mut other_items = other.items.clone();
                other_items.sort_unstable();
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
    pub fn count_below(&self, item: &T, inclusive: bool) -> usize {
        if self.is_sorted {
            if inclusive {
                self.items.partition_point(|x| x <= item)
            } else {
                self.items.partition_point(|x| x < item)
            }
        } else {
            self.items
                .iter()
                .filter(|x| if inclusive { *x <= item } else { *x < item })
                .count()
        }
    }

    /// Merges sorted items using the reusable scratch buffer.
    ///
    /// Both this compactor's items and the input must be sorted.
    #[inline(always)]
    pub fn merge_sorted(&mut self, items: &[T]) {
        if items.is_empty() {
            return;
        }

        if self.items.is_empty() {
            self.items.extend_from_slice(items);
            self.is_sorted = true;
            return;
        }

        let total = self.items.len() + items.len();
        self.scratch_buffer.clear();
        self.scratch_buffer.reserve(total);

        let (mut i, mut j) = (0usize, 0usize);
        let (a, b) = (&self.items, items);

        while i < a.len() && j < b.len() {
            if a[i] <= b[j] {
                self.scratch_buffer.push(a[i].clone());
                i += 1;
            } else {
                self.scratch_buffer.push(b[j].clone());
                j += 1;
            }
        }

        self.scratch_buffer.extend_from_slice(&a[i..]);
        self.scratch_buffer.extend_from_slice(&b[j..]);

        self.items.clear();
        std::mem::swap(&mut self.items, &mut self.scratch_buffer);
        self.is_sorted = true;
    }

    /// Sorts the items in this compactor if not already sorted.
    #[inline(always)]
    pub fn sort(&mut self) {
        if !self.is_sorted {
            self.items.sort_unstable();
            self.is_sorted = true;
        }
    }

    /// Writes promoted samples into `out` and removes their source range from this level.
    #[inline(always)]
    pub fn compact_into(&mut self, out: &mut Vec<T>) {
        out.clear();
        self.sort();

        let secs_to_compact =
            ((!self.state).trailing_zeros() + 1).min(self.num_sections as u32) as u8;
        let (start, end) = self.compute_compaction_range(secs_to_compact);
        if end - start < 2 {
            return;
        }

        // Complement consecutive choices so each pair of compactions promotes both parities.
        if (self.state & 1) == 1 {
            self.coin = !self.coin;
        } else {
            self.coin = random_bit();
        }

        let mut i = start + usize::from(self.coin);
        while i < end {
            out.push(self.items[i].clone());
            i += 2;
        }

        let removed = end - start;
        if end < self.items.len() {
            self.items[start..].rotate_left(removed);
        }
        self.items.truncate(self.items.len() - removed);

        self.state = self.state.wrapping_add(1);
        self.ensure_enough_sections();
    }

    /// Returns an iterator over the items in this compactor.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }

    /// Returns a slice of items for zero-allocation iteration.
    pub fn items_slice(&self) -> &[T] {
        &self.items
    }

    /// Returns the weight (2^lg_weight) for items in this compactor.
    pub fn weight(&self) -> u64 {
        1u64 << self.lg_weight
    }

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

        // Preserve an extra item when needed to make the compacted range even.
        if self.items.len() >= non_compact && ((self.items.len() - non_compact) & 1) == 1 {
            non_compact += 1;
        }

        match self.rank_accuracy {
            RankAccuracy::HighRank => (0, self.items.len().saturating_sub(non_compact)),
            RankAccuracy::LowRank => {
                let low = non_compact.min(self.items.len());
                (low, self.items.len())
            }
        }
    }

    /// Serialize this compactor (preamble + items) into the byte buffer.
    pub fn serialize_into(&self, bytes: &mut crate::codec::SketchBytes)
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
    pub fn deserialize(
        cursor: &mut crate::codec::SketchSlice<'_>,
        k: u16,
        expected_lg_weight: u8,
        rank_accuracy: RankAccuracy,
        sorted: bool,
    ) -> Result<Self, Error>
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
        let sorted = sorted && items.is_sorted();

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
    /// A false wire flag stays false for byte-stable C++/Java round trips; a true flag
    /// is cleared if the items are not actually sorted.
    pub fn raw_items_compactor(
        k: u16,
        rank_accuracy: RankAccuracy,
        items: Vec<T>,
        is_sorted: bool,
    ) -> Self {
        let is_sorted = is_sorted && items.is_sorted();
        let mut c = Self::new(0, k, rank_accuracy);
        c.items = items;
        c.is_sorted = is_sorted;
        c
    }

    /// Reconstruct a Compactor from deserialized state.
    ///
    /// Used by [`Compactor::deserialize`]. Transient state (random coin, scratch
    /// buffer) is reset; the deterministic `state` counter and the persistent
    /// configuration (`lg_weight`, `section_size_raw`, `num_sections`) are preserved
    /// from the wire data.
    fn from_serialized_state(
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
mod tests {
    use googletest::assert_that;
    use googletest::prelude::ge;

    use super::*;
    use crate::req::ReqFloat;

    #[test]
    fn test_new_compactor() {
        let compactor: Compactor<i32> = Compactor::new(0, 12, RankAccuracy::HighRank);
        assert_eq!(compactor.lg_weight, 0);
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

        let mut c: Compactor<ReqFloat<f32>> = Compactor::new(0, 12, RankAccuracy::HighRank);
        for i in 0..30 {
            c.append(ReqFloat::<f32>::new(i as f32).unwrap());
        }
        c.sort();

        let mut bytes = SketchBytes::with_capacity(256);
        c.serialize_into(&mut bytes);
        let raw = bytes.into_bytes();

        let mut cursor = SketchSlice::new(&raw);
        let c2 = Compactor::<ReqFloat<f32>>::deserialize(
            &mut cursor,
            12,
            0,
            RankAccuracy::HighRank,
            true,
        )
        .unwrap();

        assert_eq!(c.num_items(), c2.num_items());
        assert_eq!(c.lg_weight, c2.lg_weight);
        assert_eq!(c.state, c2.state);
        let xs: Vec<ReqFloat<f32>> = c.iter().copied().collect();
        let ys: Vec<ReqFloat<f32>> = c2.iter().copied().collect();
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
        let mut a: Compactor<i32> = Compactor::new(0, 12, RankAccuracy::HighRank);
        let mut b: Compactor<i32> = Compactor::new(0, 12, RankAccuracy::HighRank);
        b.state = 0xFFFF;

        assert_eq!(a.num_sections, 3, "default num_sections sanity");

        a.merge(&b);

        assert_that!(a.num_sections, ge(12));
    }
}
