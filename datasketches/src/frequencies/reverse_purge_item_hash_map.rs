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

//! Reverse purge hash map for generic items.

use std::hash::Hash;
use std::hash::Hasher;

use crate::hash::MurmurHash3X64128;

const LOAD_FACTOR: f64 = 0.75;
const DRIFT_LIMIT: usize = 1024;
const MAX_SAMPLE_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub struct ReversePurgeItemHashMap<T> {
    lg_length: u8,
    load_threshold: usize,
    keys: Vec<Option<T>>,
    values: Vec<i64>,
    states: Vec<u16>,
    num_active: usize,
}

impl<T: Eq + Hash> ReversePurgeItemHashMap<T> {
    pub fn new(map_size: usize) -> Self {
        assert!(map_size.is_power_of_two(), "map_size must be power of 2");
        let lg_length = map_size.trailing_zeros() as u8;
        let load_threshold = (map_size as f64 * LOAD_FACTOR) as usize;
        Self {
            lg_length,
            load_threshold,
            keys: (0..map_size).map(|_| None).collect(),
            values: vec![0; map_size],
            states: vec![0; map_size],
            num_active: 0,
        }
    }

    pub fn get(&self, key: &T) -> i64 {
        let probe = self.hash_probe(key);
        if self.states[probe] > 0 {
            return self.values[probe];
        }
        0
    }

    pub fn adjust_or_put_value(&mut self, key: T, adjust_amount: i64) {
        let mask = self.keys.len() - 1;
        let mut probe = (hash_item(&key) as usize) & mask;
        let mut drift: usize = 1;
        while self.states[probe] != 0 {
            let matches = self.keys[probe]
                .as_ref()
                .map(|existing| existing == &key)
                .unwrap_or(false);
            if matches {
                break;
            }
            probe = (probe + 1) & mask;
            drift += 1;
            debug_assert!(drift < DRIFT_LIMIT, "drift limit exceeded");
        }
        if self.states[probe] == 0 {
            self.keys[probe] = Some(key);
            self.values[probe] = adjust_amount;
            self.states[probe] = drift as u16;
            self.num_active += 1;
        } else {
            self.values[probe] += adjust_amount;
        }
    }

    pub fn keep_only_positive_counts(&mut self) {
        let len = self.keys.len();
        let mut first_probe = len - 1;
        while self.states[first_probe] > 0 {
            first_probe -= 1;
        }
        for probe in (0..first_probe).rev() {
            if self.states[probe] > 0 && self.values[probe] <= 0 {
                self.hash_delete(probe);
                self.num_active -= 1;
            }
        }
        for probe in (first_probe..len).rev() {
            if self.states[probe] > 0 && self.values[probe] <= 0 {
                self.hash_delete(probe);
                self.num_active -= 1;
            }
        }
    }

    pub fn adjust_all_values_by(&mut self, adjust_amount: i64) {
        for value in &mut self.values {
            *value += adjust_amount;
        }
    }

    pub fn purge(&mut self, sample_size: usize) -> i64 {
        let limit = sample_size.min(self.num_active).min(MAX_SAMPLE_SIZE);
        let mut samples = Vec::with_capacity(limit);
        let mut i = 0usize;
        while samples.len() < limit {
            if self.is_active(i) {
                samples.push(self.values[i]);
            }
            i += 1;
        }
        let mid = samples.len() / 2;
        samples.select_nth_unstable(mid);
        let median = samples[mid];
        self.adjust_all_values_by(-median);
        self.keep_only_positive_counts();
        median
    }

    pub fn resize(&mut self, new_size: usize) {
        assert!(new_size.is_power_of_two(), "new_size must be power of 2");
        let mut old_keys = std::mem::take(&mut self.keys);
        let old_values = std::mem::take(&mut self.values);
        let old_states = std::mem::take(&mut self.states);
        self.keys = (0..new_size).map(|_| None).collect();
        self.values = vec![0; new_size];
        self.states = vec![0; new_size];
        self.lg_length = new_size.trailing_zeros() as u8;
        self.load_threshold = (new_size as f64 * LOAD_FACTOR) as usize;
        self.num_active = 0;
        for i in 0..old_keys.len() {
            if old_states[i] > 0 {
                if let Some(key) = old_keys[i].take() {
                    self.adjust_or_put_value(key, old_values[i]);
                }
            }
        }
    }

    pub fn get_length(&self) -> usize {
        self.keys.len()
    }

    pub fn get_lg_length(&self) -> u8 {
        self.lg_length
    }

    pub fn get_capacity(&self) -> usize {
        self.load_threshold
    }

    pub fn get_num_active(&self) -> usize {
        self.num_active
    }

    pub fn get_active_keys(&self) -> Vec<T>
    where
        T: Clone,
    {
        if self.num_active == 0 {
            return Vec::new();
        }
        let mut keys = Vec::with_capacity(self.num_active);
        for i in 0..self.keys.len() {
            if self.states[i] > 0 {
                if let Some(key) = self.keys[i].as_ref() {
                    keys.push(key.clone());
                }
            }
        }
        keys
    }

    pub fn get_active_values(&self) -> Vec<i64> {
        if self.num_active == 0 {
            return Vec::new();
        }
        let mut values = Vec::with_capacity(self.num_active);
        for i in 0..self.values.len() {
            if self.states[i] > 0 {
                values.push(self.values[i]);
            }
        }
        values
    }

    pub fn iter(&self) -> ReversePurgeItemIter<'_, T> {
        ReversePurgeItemIter::new(self)
    }

    fn is_active(&self, probe: usize) -> bool {
        self.states[probe] > 0
    }

    fn hash_probe(&self, key: &T) -> usize {
        let mask = self.keys.len() - 1;
        let mut probe = (hash_item(key) as usize) & mask;
        while self.states[probe] > 0 {
            let matches = self.keys[probe]
                .as_ref()
                .map(|existing| existing == key)
                .unwrap_or(false);
            if matches {
                break;
            }
            probe = (probe + 1) & mask;
        }
        probe
    }

    fn hash_delete(&mut self, mut delete_probe: usize) {
        self.states[delete_probe] = 0;
        self.keys[delete_probe] = None;
        let mut drift: usize = 1;
        let mask = self.keys.len() - 1;
        let mut probe = (delete_probe + drift) & mask;
        while self.states[probe] != 0 {
            if self.states[probe] as usize > drift {
                self.keys[delete_probe] = self.keys[probe].take();
                self.values[delete_probe] = self.values[probe];
                self.states[delete_probe] = self.states[probe] - drift as u16;
                self.states[probe] = 0;
                drift = 0;
                delete_probe = probe;
            }
            probe = (probe + 1) & mask;
            drift += 1;
            debug_assert!(drift < DRIFT_LIMIT, "drift limit exceeded");
        }
    }
}

pub struct ReversePurgeItemIter<'a, T> {
    map: &'a ReversePurgeItemHashMap<T>,
    index: usize,
    count: usize,
    stride: usize,
    mask: usize,
}

impl<'a, T> ReversePurgeItemIter<'a, T> {
    fn new(map: &'a ReversePurgeItemHashMap<T>) -> Self {
        let size = map.keys.len();
        let stride = ((size as f64 * 0.6180339887498949) as usize) | 1;
        let mask = size - 1;
        let index = 0usize.wrapping_sub(stride);
        Self {
            map,
            index,
            count: 0,
            stride,
            mask,
        }
    }
}

impl<'a, T> Iterator for ReversePurgeItemIter<'a, T> {
    type Item = (&'a T, i64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.map.num_active {
            return None;
        }
        loop {
            self.index = self.index.wrapping_add(self.stride) & self.mask;
            if self.map.states[self.index] > 0 {
                self.count += 1;
                let key = self.map.keys[self.index].as_ref().expect("active key missing");
                return Some((key, self.map.values[self.index]));
            }
        }
    }
}

#[inline]
fn hash_item<T: Hash>(item: &T) -> u64 {
    let mut hasher = MurmurHash3X64128::default();
    item.hash(&mut hasher);
    hasher.finish()
}
