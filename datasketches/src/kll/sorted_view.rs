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

use super::sketch::KllComparator;
use super::sketch::KllItem;

#[derive(Debug, Clone)]
pub(crate) struct SortedView<T: KllItem, C: KllComparator<T>> {
    comparator: C,
    entries: Vec<Entry<T>>,
    total_weight: u64,
}

#[derive(Debug, Clone)]
struct Entry<T> {
    item: T,
    weight: u64,
}

impl<T: KllItem, C: KllComparator<T>> SortedView<T, C> {
    fn new(mut entries: Vec<Entry<T>>, comparator: C) -> Self {
        entries.sort_by(|a, b| comparator.compare(&a.item, &b.item));
        let mut total_weight = 0u64;
        for entry in &mut entries {
            total_weight += entry.weight;
            entry.weight = total_weight;
        }
        Self {
            comparator,
            entries,
            total_weight,
        }
    }

    pub fn rank(&self, item: &T, inclusive: bool) -> f64 {
        if self.entries.is_empty() {
            return 0.0;
        }

        let idx = if inclusive {
            upper_bound(&self.entries, item, &self.comparator)
        } else {
            lower_bound(&self.entries, item, &self.comparator)
        };

        if idx == 0 {
            return 0.0;
        }
        let weight = self.entries[idx - 1].weight;
        weight as f64 / self.total_weight as f64
    }

    pub fn quantile(&self, rank: f64, inclusive: bool) -> T {
        let weight = if inclusive {
            (rank * self.total_weight as f64).ceil() as u64
        } else {
            (rank * self.total_weight as f64) as u64
        };

        let idx = if inclusive {
            lower_bound_by_weight(&self.entries, weight)
        } else {
            upper_bound_by_weight(&self.entries, weight)
        };

        if idx >= self.entries.len() {
            return self.entries[self.entries.len() - 1].item.clone();
        }
        self.entries[idx].item.clone()
    }

    pub fn cdf(&self, split_points: &[T], inclusive: bool) -> Vec<f64> {
        check_split_points(split_points, &self.comparator);
        let mut ranks = Vec::with_capacity(split_points.len() + 1);
        for item in split_points {
            ranks.push(self.rank(item, inclusive));
        }
        ranks.push(1.0);
        ranks
    }

    pub fn pmf(&self, split_points: &[T], inclusive: bool) -> Vec<f64> {
        let mut buckets = self.cdf(split_points, inclusive);
        for i in (1..buckets.len()).rev() {
            buckets[i] -= buckets[i - 1];
        }
        buckets
    }
}

pub(crate) fn build_sorted_view<T: KllItem, C: KllComparator<T>>(
    levels: &[Vec<T>],
    comparator: C,
) -> SortedView<T, C> {
    let num_retained: usize = levels.iter().map(|level| level.len()).sum();
    let mut entries = Vec::with_capacity(num_retained);

    for (level_idx, level) in levels.iter().enumerate() {
        let weight = 1u64 << level_idx;
        for item in level {
            entries.push(Entry {
                item: item.clone(),
                weight,
            });
        }
    }

    SortedView::new(entries, comparator)
}

#[track_caller]
fn check_split_points<T: KllItem, C: KllComparator<T>>(split_points: &[T], comparator: &C) {
    assert!(
        split_points.iter().all(|point| !T::is_nan(point)),
        "split_points must not contain NaN values"
    );
    for pair in split_points.windows(2) {
        assert!(
            comparator.compare(&pair[0], &pair[1]) == Ordering::Less,
            "split_points must be unique and monotonically increasing"
        );
    }
}

fn lower_bound<T: KllItem, C: KllComparator<T>>(
    entries: &[Entry<T>],
    item: &T,
    comparator: &C,
) -> usize {
    let mut left = 0usize;
    let mut right = entries.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if comparator.compare(&entries[mid].item, item) == Ordering::Less {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

fn upper_bound<T: KllItem, C: KllComparator<T>>(
    entries: &[Entry<T>],
    item: &T,
    comparator: &C,
) -> usize {
    let mut left = 0usize;
    let mut right = entries.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if comparator.compare(&entries[mid].item, item) == Ordering::Greater {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    left
}

fn lower_bound_by_weight<T: KllItem>(entries: &[Entry<T>], weight: u64) -> usize {
    let mut left = 0usize;
    let mut right = entries.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if entries[mid].weight < weight {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

fn upper_bound_by_weight<T: KllItem>(entries: &[Entry<T>], weight: u64) -> usize {
    let mut left = 0usize;
    let mut right = entries.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if entries[mid].weight > weight {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    left
}
