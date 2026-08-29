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

//! Iterator implementations for REQ sketch inspection.

use crate::req::compactor::Compactor;

/// Iterator over (item, weight) pairs in a REQ sketch.
///
/// Provides access to all items in the sketch along with their weights,
/// which depend on the level of the compactor they're stored in.
///
/// Zero-allocation implementation that works directly with slices.
pub struct ReqSketchIterator<'a, T> {
    compactors: &'a [Compactor<T>],
    current_level: usize,
    current_level_iter: Option<std::slice::Iter<'a, T>>,
    current_weight: u64,
}

impl<'a, T: Clone> ReqSketchIterator<'a, T> {
    /// Creates a new iterator over the compactors.
    pub(super) fn new(compactors: &'a [Compactor<T>]) -> Self {
        let mut iter = Self {
            compactors,
            current_level: 0,
            current_level_iter: None,
            current_weight: 0,
        };
        iter.advance_to_next_level();
        iter
    }

    fn advance_to_next_level(&mut self) {
        while self.current_level < self.compactors.len() {
            let compactor = &self.compactors[self.current_level];
            // Access items slice directly without allocation
            let items_slice = compactor.items_slice();

            if !items_slice.is_empty() {
                self.current_level_iter = Some(items_slice.iter());
                self.current_weight = compactor.weight();
                return;
            }

            self.current_level += 1;
        }

        self.current_level_iter = None;
    }
}

impl<T: Clone> Iterator for ReqSketchIterator<'_, T> {
    type Item = (T, u64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut level_iter) = self.current_level_iter {
                if let Some(item) = level_iter.next() {
                    return Some((item.clone(), self.current_weight));
                }
            }

            // Current level exhausted, move to next
            self.current_level += 1;
            self.advance_to_next_level();

            self.current_level_iter.as_ref()?;
        }
    }
}
