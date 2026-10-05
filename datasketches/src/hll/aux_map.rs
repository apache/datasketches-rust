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

use crate::hll::Coupon;
use crate::hll::RESIZE_DENOMINATOR;
use crate::hll::RESIZE_NUMERATOR;

/// Absolute register values for the `AUX_TOKEN` slots in `Array4`.
#[derive(Debug, Clone)]
pub struct AuxMap {
    lg_size: u8,
    lg_config_k: u8,
    entries: Box<[Coupon]>,
    count: u32,
}

impl PartialEq for AuxMap {
    fn eq(&self, other: &Self) -> bool {
        // Capacity and slot order do not affect the logical entries.
        if self.lg_config_k != other.lg_config_k || self.count != other.count {
            return false;
        }

        let mut entries1: Vec<Coupon> = self
            .entries
            .iter()
            .filter(|&&e| !e.is_empty())
            .copied()
            .collect();
        let mut entries2: Vec<Coupon> = other
            .entries
            .iter()
            .filter(|&&e| !e.is_empty())
            .copied()
            .collect();

        entries1.sort_unstable();
        entries2.sort_unstable();

        entries1 == entries2
    }
}

/// Initial table sizes from DataSketches C++:
/// <https://github.com/apache/datasketches-cpp/blob/5a055521/hll/include/HllUtil.hpp>
fn lg_aux_arr_ints(lg_config_k: u8) -> u8 {
    static LG_AUX_ARR_INTS: &[u8] = &[
        0, 2, 2, 2, 2, 2, 2, 3, 3, 3, // 0-9
        4, 4, 5, 5, 6, 7, 8, 9, 10, 11, // 10-19
        12, 13, 14, 15, 16, 17, 18, // 20-26
    ];

    LG_AUX_ARR_INTS[lg_config_k as usize]
}

impl AuxMap {
    pub fn new(lg_config_k: u8) -> Self {
        let lg_size = lg_aux_arr_ints(lg_config_k);
        Self {
            lg_size,
            lg_config_k,
            entries: vec![Coupon::EMPTY; 1 << lg_size].into_boxed_slice(),
            count: 0,
        }
    }

    pub fn insert(&mut self, slot: u32, value: u8) {
        let index = self.find(slot);
        match index {
            FindResult::Found(_) => {
                // Array4 uses insert only when a slot first becomes an exception.
                unreachable!("slot {} already exists in aux map", slot);
            }
            FindResult::Empty(idx) => {
                self.entries[idx] = Coupon::pack(slot, value);
                self.count += 1;
                self.check_grow();
            }
        }
    }

    pub fn get(&self, slot: u32) -> Option<u8> {
        match self.find(slot) {
            FindResult::Found(idx) => Some(self.entries[idx].value()),
            FindResult::Empty(_) => None,
        }
    }

    pub fn replace(&mut self, slot: u32, value: u8) {
        match self.find(slot) {
            FindResult::Found(idx) => {
                self.entries[idx] = Coupon::pack(slot, value);
            }
            FindResult::Empty(_) => {
                // Every AUX_TOKEN slot must already have an entry.
                unreachable!("slot {} not found in aux map", slot);
            }
        }
    }

    fn find(&self, slot: u32) -> FindResult {
        let mask = (1 << self.lg_size) - 1;
        let config_k_mask = (1 << self.lg_config_k) - 1;
        let mut probe = slot & mask;
        let start = probe;

        loop {
            let entry = self.entries[probe as usize];

            if entry.is_empty() {
                return FindResult::Empty(probe as usize);
            }

            let entry_slot = entry.slot() & config_k_mask;
            if entry_slot == slot {
                return FindResult::Found(probe as usize);
            }

            // An odd stride visits every slot in a power-of-two table.
            let stride = (slot >> self.lg_size) | 1;
            probe = (probe + stride) & mask;

            if probe == start {
                // insert grows the table past 75% occupancy, leaving an empty slot.
                unreachable!("AuxMap full; no empty slots");
            }
        }
    }

    fn check_grow(&mut self) {
        let size = 1 << self.lg_size;
        if (RESIZE_DENOMINATOR * self.count) > (RESIZE_NUMERATOR * size) {
            self.grow();
        }
    }

    fn grow(&mut self) {
        let new_lg_size = self.lg_size + 1;
        let new_size = 1 << new_lg_size;
        let new_mask = (1 << new_lg_size) - 1;
        let mut new_entries = vec![Coupon::EMPTY; new_size].into_boxed_slice();

        for &entry in self.entries.iter() {
            if !entry.is_empty() {
                let slot = entry.slot();

                let mut probe = slot & new_mask;
                let start_position = probe;

                loop {
                    if new_entries[probe as usize].is_empty() {
                        new_entries[probe as usize] = entry;
                        break;
                    }

                    let stride = (slot >> new_lg_size) | 1;
                    probe = (probe + stride) & new_mask;
                    if probe == start_position {
                        // Doubling capacity leaves room for every existing entry.
                        unreachable!("AuxMap full; no empty slots");
                    }
                }
            }
        }

        self.entries = new_entries;
        self.lg_size = new_lg_size;
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, u8)> + '_ {
        let config_k_mask = (1 << self.lg_config_k) - 1;
        self.entries.iter().filter_map(move |&entry| {
            if !entry.is_empty() {
                Some((entry.slot() & config_k_mask, entry.value()))
            } else {
                None
            }
        })
    }

    /// Returns the estimated size of the heap allocations in bytes
    pub fn estimated_size(&self) -> usize {
        self.entries.len() * size_of::<Coupon>()
    }
}

pub struct AuxMapIter {
    entries: std::vec::IntoIter<Coupon>,
    config_k_mask: u32,
}

impl Iterator for AuxMapIter {
    type Item = (u32, u8);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.entries.next() {
                Some(entry) if !entry.is_empty() => {
                    let slot = entry.slot() & self.config_k_mask;
                    let value = entry.value();
                    return Some((slot, value));
                }
                Some(_) => continue,
                None => return None,
            }
        }
    }
}

impl IntoIterator for AuxMap {
    type Item = (u32, u8);
    type IntoIter = AuxMapIter;

    fn into_iter(self) -> Self::IntoIter {
        AuxMapIter {
            entries: self.entries.into_vec().into_iter(),
            config_k_mask: (1 << self.lg_config_k) - 1,
        }
    }
}

enum FindResult {
    Found(usize),
    Empty(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aux_map_basic_operations() {
        let mut map = AuxMap::new(10);

        map.insert(10, 20);
        map.insert(50, 30);
        map.insert(100, 40);

        assert_eq!(map.get(10), Some(20));
        assert_eq!(map.get(50), Some(30));
        assert_eq!(map.get(100), Some(40));
        assert_eq!(map.get(999), None);

        map.replace(50, 35);
        assert_eq!(map.get(50), Some(35));
    }

    #[test]
    fn test_aux_map_growth() {
        let mut map = AuxMap::new(8);

        map.insert(1, 15);
        map.insert(2, 16);
        map.insert(3, 17);
        map.insert(4, 18);

        assert_eq!(map.get(1), Some(15));
        assert_eq!(map.get(2), Some(16));
        assert_eq!(map.get(3), Some(17));
        assert_eq!(map.get(4), Some(18));
    }

    #[test]
    #[should_panic(expected = "already exists")]
    fn test_aux_map_duplicate_insert() {
        let mut map = AuxMap::new(10);
        map.insert(10, 20);
        map.insert(10, 30);
    }

    #[test]
    #[should_panic(expected = "not found")]
    fn test_aux_map_replace_missing() {
        let mut map = AuxMap::new(10);
        map.replace(999, 20);
    }
}
