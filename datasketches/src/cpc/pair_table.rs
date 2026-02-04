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

const UPSIZE_NUMERATOR: u32 = 3;
const UPSIZE_DENOMINATOR: u32 = 4;
const DOWNSIZE_NUMERATOR: u32 = 1;
const DOWNSIZE_DENOMINATOR: u32 = 4;

/// A highly specialized hash table used for sparse data.
///
/// This table stores `(row, col)` pairs and uses linear probing for collision resolution. It is
/// optimized for scenarios where the cardinality of entries is low.
#[derive(Debug, Clone)]
pub(super) struct PairTable {
    /// log2 of number of slots
    lg_size: u8,
    num_valid_bits: u8,
    num_items: u32,
    slots: Vec<u32>,
}

impl PairTable {
    pub fn new(lg_size: u8, num_valid_bits: u8) -> Self {
        assert!(
            (2..=26).contains(&lg_size),
            "lg_size must be in [2, 26], got {lg_size}"
        );
        assert!(
            ((lg_size + 1)..=32).contains(&num_valid_bits),
            "num_valid_bits must be in [lg_size + 1, 32], got {num_valid_bits} where lg_size = {lg_size}"
        );
        Self {
            lg_size,
            num_valid_bits,
            num_items: 0,
            slots: vec![u32::MAX; 1 << lg_size],
        }
    }

    /// A constructor specifically tailored to be a part of FM85 decompression scheme.
    pub fn from_slots(lg_size: u8, num_items: u32, slots: Vec<u32>) -> Self {
        let mut lg_num_slots = 2;
        while UPSIZE_DENOMINATOR * num_items > (UPSIZE_NUMERATOR * (1 << lg_num_slots)) {
            lg_num_slots += 1;
        }

        let mut table = Self::new(lg_num_slots, 6 + lg_size);

        // Note: there is a possible "snowplow effect" here because the caller is passing in a
        // sorted pairs array. However, we are starting out with the correct final table size, so
        // the problem might not occur.

        for slot in slots {
            table.must_insert(slot);
        }
        table.num_items = num_items;
        table
    }

    pub fn slots(&self) -> &[u32] {
        &self.slots
    }

    pub fn clear(&mut self) {
        self.slots.fill(u32::MAX);
        self.num_items = 0;
    }

    pub fn maybe_delete(&mut self, item: u32) -> bool {
        let index = self.lookup(item) as usize;
        if self.slots[index] == u32::MAX {
            return false;
        }
        assert_eq!(
            self.slots[index], item,
            "item {item} not found at index {index}"
        );
        assert!(self.num_items > 0, "no items to delete");

        // delete the item
        self.slots[index] = u32::MAX;
        self.num_items -= 1;

        // re-insert all items between the freed slot and the next empty slot
        let mask = (1 << self.lg_size) - 1;
        let mut probe = (index + 1) & mask;
        let mut fetched = self.slots[probe];
        while fetched != u32::MAX {
            self.slots[probe] = u32::MAX;
            self.must_insert(fetched);
            probe = (probe + 1) & mask;
            fetched = self.slots[probe];
        }

        // shrink if necessary
        while ((DOWNSIZE_DENOMINATOR * self.num_items) < (DOWNSIZE_NUMERATOR * (1 << self.lg_size)))
            && (self.lg_size > 2)
        {
            self.rebuild(self.lg_size - 1);
        }

        true
    }

    pub fn maybe_insert(&mut self, item: u32) -> bool {
        let index = self.lookup(item) as usize;
        if self.slots[index] == item {
            return false;
        }
        assert_eq!(
            self.slots[index],
            u32::MAX,
            "no empty slot found for item {item} at index {index}"
        );
        self.slots[index] = item;
        self.num_items += 1;
        while (UPSIZE_DENOMINATOR * self.num_items) > (UPSIZE_NUMERATOR * (1 << self.lg_size)) {
            self.rebuild(self.lg_size + 1);
        }
        true
    }

    /// While extracting the items from a linear probing hashtable, this will usually undo the
    /// wrap-around provided that the table isn't too full.
    ///
    /// Experiments suggest that for sufficiently large tables the load factor would have to be
    /// over 90 percent before this would fail frequently, and even then the subsequent sort would
    /// fix things up.
    ///
    /// The result is nearly sorted, so make sure to use an efficient sort for that case.
    pub fn unwrapping_get_items(&self) -> Vec<u32> {
        if self.slots.is_empty() {
            return vec![];
        }

        let table_size = 1usize << self.lg_size;
        let mut result = vec![0; self.num_items as usize];
        let mut i = 0usize;
        let mut l = 0usize;
        let mut r = self.num_items as usize - 1;

        // special rules for the region before the first empty slot
        let hi_bit = 1 << (self.num_valid_bits - 1);
        while i < table_size && self.slots[i] != u32::MAX {
            let item = self.slots[i];
            i += 1;
            if (item & hi_bit) != 0 {
                // this item was probably wrapped, so move to end
                result[r] = item;
                r -= 1;
            } else {
                result[l] = item;
                l += 1;
            }
        }

        // the rest of the table is processed normally
        while i < table_size {
            let item = self.slots[i];
            i += 1;
            if item != u32::MAX {
                result[l] = item;
                l += 1;
            }
        }

        assert_eq!(l, r + 1, "number of items mismatch during extraction");
        result
    }

    fn must_insert(&mut self, item: u32) {
        let index = self.lookup(item) as usize;
        assert_ne!(
            self.slots[index], item,
            "item {item} already present in table"
        );
        assert_eq!(
            self.slots[index],
            u32::MAX,
            "no empty slot found for item {item} at index {index}"
        );
        self.slots[index] = item;
        // counts and resizing must be handled by the caller.
    }

    fn lookup(&self, item: u32) -> u32 {
        let size = 1 << self.lg_size;
        let mask = size - 1;

        let shift = self.num_valid_bits - self.lg_size;

        // extract high table size bits
        let mut probe = item >> shift;
        assert!(probe <= mask, "probe = {probe}, mask = {mask}");

        loop {
            let slot = self.slots[probe as usize];
            if slot != item && slot != u32::MAX {
                probe = (probe + 1) & mask;
            } else {
                break;
            }
        }

        probe
    }

    /// Rebuilds to a larger size. `num_items` and `num_valid_bits` remain unchanged.
    fn rebuild(&mut self, lg_size: u8) {
        assert!(
            (2..=26).contains(&lg_size),
            "lg_size must be in [2, 26], got {lg_size}"
        );
        assert!(
            ((lg_size + 1)..=32).contains(&self.num_valid_bits),
            "num_valid_bits must be in [lg_size + 1, 32], got {} where lg_size = {lg_size}",
            self.num_valid_bits
        );

        let new_size = 1u32 << lg_size;
        assert!(
            new_size > self.num_items,
            "new table size ({new_size}) must be larger than number of items {}",
            self.num_items
        );

        let slots = std::mem::replace(&mut self.slots, vec![u32::MAX; new_size as usize]);
        self.lg_size = lg_size;
        for slot in slots {
            if slot != u32::MAX {
                self.must_insert(slot);
            }
        }
    }
}

/// This should be used pair with [`PairTable::unwrapping_get_items`].
///
/// In applications where the input array is already nearly sorted, insertion sort runs in linear
/// time with a very small constant.
///
/// This introspective version of insertion sort protects against the quadratic cost of sorting bad
/// input arrays. It keeps track of how much work has been done, and if that exceeds a constant
/// times the array length, it switches to a different sorting algorithm.
pub fn introspective_insertion_sort(a: &mut [u32]) {
    let cost_limit = 8 * a.len();

    let mut cost = 0;
    for i in 1..a.len() {
        let mut j = i;
        let v = a[i];
        while j >= 1 && v < a[j - 1] {
            a[j] = a[j - 1];
            j -= 1;
        }
        a[j] = v;
        cost += i - j; // distance moved is a measure of work
        if cost > cost_limit {
            knuth_shell_sort3(a);
            return;
        }
    }
}

fn knuth_shell_sort3(a: &mut [u32]) {
    let len = a.len();

    let mut h = 0;
    while h < len / 9 {
        h = 3 * h + 1;
    }

    while h > 0 {
        for i in h..len {
            let v = a[i];
            let mut j = i;
            while j >= h && v < a[j - h] {
                a[j] = a[j - h];
                j -= h;
            }
            a[j] = v;
        }
        h /= 3;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_1() {
        let data = (0..10)
            .map(|_| rand::random_range(0..10000))
            .collect::<Vec<_>>();
        let mut sorted = data.clone();
        introspective_insertion_sort(&mut sorted);
        assert!(
            sorted.is_sorted(),
            "data was not sorted correctly: origin={data:?}, sorted={sorted:?}"
        );
    }

    #[test]
    fn test_sort_2() {
        let data = (0..10)
            .map(|_| rand::random_range(0..10000) + 3_000_000_000)
            .collect::<Vec<_>>();
        let mut sorted = data.clone();
        introspective_insertion_sort(&mut sorted);
        assert!(
            sorted.is_sorted(),
            "data was not sorted correctly: origin={data:?}, sorted={sorted:?}"
        );
    }

    #[test]
    fn test_sort_3() {
        let len = 20;
        let data = (0..len).map(|i| len - i + 1).collect::<Vec<_>>();
        let mut sorted = data.clone();
        introspective_insertion_sort(&mut sorted);
        assert!(
            sorted.is_sorted(),
            "data was not sorted correctly: origin={data:?}, sorted={sorted:?}"
        );
    }
}
