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

use super::mapping::IndexMapping;
use super::store::StoreIter;

/// Iterator over non-empty `(value, weight)` buckets of a [`DdSketch`](super::DdSketch) in
/// ascending numerical order.
///
/// Each item pairs the bucket's representative value (`0.0` for the zero bucket) with its
/// accumulated stream weight.
#[derive(Clone)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct DdSketchIter<'a> {
    mapping: IndexMapping,
    neg_iter: StoreIter<'a>,
    zero_weight: Option<NonZeroU64>,
    pos_iter: StoreIter<'a>,
    remaining: usize,
}

impl<'a> DdSketchIter<'a> {
    pub(super) fn new(
        mapping: IndexMapping,
        neg_iter: StoreIter<'a>,
        zero_weight: u64,
        pos_iter: StoreIter<'a>,
        remaining: usize,
    ) -> Self {
        Self {
            mapping,
            neg_iter,
            zero_weight: NonZeroU64::new(zero_weight),
            pos_iter,
            remaining,
        }
    }
}

impl fmt::Debug for DdSketchIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DdSketchIter")
            .field("remaining", &self.remaining)
            .finish()
    }
}

impl Iterator for DdSketchIter<'_> {
    type Item = (f64, u64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((index, weight)) = self.neg_iter.next_back() {
            self.remaining -= 1;
            return Some((-self.mapping.value(index), weight));
        }
        if let Some(weight) = self.zero_weight.take() {
            self.remaining -= 1;
            return Some((0.0, weight.get()));
        }
        if let Some((index, weight)) = self.pos_iter.next() {
            self.remaining -= 1;
            return Some((self.mapping.value(index), weight));
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl DoubleEndedIterator for DdSketchIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some((index, weight)) = self.pos_iter.next_back() {
            self.remaining -= 1;
            return Some((self.mapping.value(index), weight));
        }
        if let Some(weight) = self.zero_weight.take() {
            self.remaining -= 1;
            return Some((0.0, weight.get()));
        }
        if let Some((index, weight)) = self.neg_iter.next() {
            self.remaining -= 1;
            return Some((-self.mapping.value(index), weight));
        }
        None
    }
}

impl ExactSizeIterator for DdSketchIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

impl FusedIterator for DdSketchIter<'_> {}
