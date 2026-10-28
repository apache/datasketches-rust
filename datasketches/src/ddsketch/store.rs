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

use std::collections::VecDeque;

use super::CollapseDirection;

// Indices increase in numerical value order, including in the negative store.
// A collapsed store retains a full window; its boundary absorbs all omitted values.
#[derive(Clone, Debug, Default)]
pub struct Store {
    pub offset: i64,
    pub counts: VecDeque<u64>,
    pub collapsed: bool,
}

impl Store {
    pub fn reset(&mut self) {
        self.offset = 0;
        self.counts.clear();
        self.collapsed = false;
    }

    pub fn last_index(&self) -> i64 {
        self.offset + self.counts.len() as i64 - 1
    }

    pub fn add(&mut self, index: i64, weight: u64, limit: u32, direction: CollapseDirection) {
        if self.counts.is_empty() {
            self.offset = index;
            self.counts.push_back(weight);
            return;
        }

        let index = if self.collapsed {
            match direction {
                CollapseDirection::Lowest => index.max(self.offset),
                CollapseDirection::Highest => index.min(self.last_index()),
            }
        } else {
            index
        };
        if index < self.offset || index > self.last_index() {
            self.extend(index, limit, direction);
        }
        let index = index.clamp(self.offset, self.last_index());
        // The sketch checks the combined total before any store mutation.
        self.counts[(index - self.offset) as usize] += weight;
    }

    pub fn merge(&mut self, other: &Self, limit: u32, direction: CollapseDirection) {
        for (position, &weight) in other.counts.iter().enumerate() {
            if weight != 0 {
                self.add(other.offset + position as i64, weight, limit, direction);
            }
        }
        // Equal limits imply that inserting the other store's endpoints aligns
        // the collapsed boundary before its loss of information is propagated.
        self.collapsed |= other.collapsed;
    }

    pub fn is_collapsed_index(&self, index: i64, direction: CollapseDirection) -> bool {
        self.collapsed
            && index
                == match direction {
                    CollapseDirection::Lowest => self.offset,
                    CollapseDirection::Highest => self.last_index(),
                }
    }

    fn extend(&mut self, index: i64, limit: u32, direction: CollapseDirection) {
        let mut low = self.offset.min(index);
        let mut high = self.last_index().max(index);
        if high - low + 1 > i64::from(limit) {
            match direction {
                CollapseDirection::Lowest => low = high - i64::from(limit) + 1,
                CollapseDirection::Highest => high = low + i64::from(limit) - 1,
            }
            self.collapsed = true;
        }

        let mut lower_weight = 0;
        while !self.counts.is_empty() && self.offset < low {
            lower_weight += self.counts.pop_front().unwrap();
            self.offset += 1;
        }
        let mut upper_weight = 0;
        while !self.counts.is_empty() && self.last_index() > high {
            upper_weight += self.counts.pop_back().unwrap();
        }
        if self.counts.is_empty() {
            self.offset = low;
        }
        while self.offset > low {
            self.counts.push_front(0);
            self.offset -= 1;
        }
        // Work and allocation depend on the retained window, never on the size
        // of an index gap (which can span hundreds of billions of empty bins).
        self.counts.resize((high - low + 1) as usize, 0);
        *self.counts.front_mut().unwrap() += lower_weight;
        *self.counts.back_mut().unwrap() += upper_weight;
    }
}
