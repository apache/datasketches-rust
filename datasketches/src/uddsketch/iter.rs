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

use std::fmt;
use std::iter::FusedIterator;
use std::num::NonZeroU64;
use std::slice;

use super::mapping::Mapping;
use super::store::Bucket;

/// Iterator over non-empty `(value, weight)` buckets of a [`UddSketch`](super::UddSketch) in
/// ascending numerical order.
///
/// Each item pairs the bucket's raw representative value (`-mapping.value_at_level(key,
/// num_collapses)` for negative buckets, `0.0` for the zero bucket, and
/// `mapping.value_at_level(key, num_collapses)` for positive buckets) with its accumulated stream
/// weight. Outer bucket representatives are unclamped and may lie outside `[min_value, max_value]`,
/// whereas [`UddSketch::quantile`](super::UddSketch::quantile) clamps estimates to
/// `[min_value, max_value]`.
#[derive(Clone)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct UddSketchIter<'a> {
    mapping: Mapping,
    num_collapses: u8,
    neg_iter: slice::Iter<'a, Bucket>,
    zero_weight: Option<NonZeroU64>,
    pos_iter: slice::Iter<'a, Bucket>,
    remaining: usize,
}

impl<'a> UddSketchIter<'a> {
    pub(super) fn new(
        mapping: Mapping,
        num_collapses: u8,
        neg_iter: slice::Iter<'a, Bucket>,
        zero_weight: u64,
        pos_iter: slice::Iter<'a, Bucket>,
        remaining: usize,
    ) -> Self {
        Self {
            mapping,
            num_collapses,
            neg_iter,
            zero_weight: NonZeroU64::new(zero_weight),
            pos_iter,
            remaining,
        }
    }
}

impl fmt::Debug for UddSketchIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UddSketchIter")
            .field("remaining", &self.remaining)
            .finish()
    }
}

impl Iterator for UddSketchIter<'_> {
    type Item = (f64, u64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(bucket) = self.neg_iter.next_back() {
            self.remaining -= 1;
            return Some((
                -self.mapping.value_at_level(bucket.key, self.num_collapses),
                bucket.count,
            ));
        }
        if let Some(weight) = self.zero_weight.take() {
            self.remaining -= 1;
            return Some((0.0, weight.get()));
        }
        let bucket = self.pos_iter.next()?;
        self.remaining -= 1;
        Some((
            self.mapping.value_at_level(bucket.key, self.num_collapses),
            bucket.count,
        ))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl DoubleEndedIterator for UddSketchIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(bucket) = self.pos_iter.next_back() {
            self.remaining -= 1;
            return Some((
                self.mapping.value_at_level(bucket.key, self.num_collapses),
                bucket.count,
            ));
        }
        if let Some(weight) = self.zero_weight.take() {
            self.remaining -= 1;
            return Some((0.0, weight.get()));
        }
        let bucket = self.neg_iter.next()?;
        self.remaining -= 1;
        Some((
            -self.mapping.value_at_level(bucket.key, self.num_collapses),
            bucket.count,
        ))
    }
}

impl ExactSizeIterator for UddSketchIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

impl FusedIterator for UddSketchIter<'_> {}
