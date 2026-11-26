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

use super::mapping::IndexMapping;
use super::store::StoreIter;
use super::weight::DdSketchWeight;

/// Iterator over non-empty `(value, weight)` buckets of a [`DdSketch`](super::DdSketch) in
/// ascending numerical order.
///
/// Each item pairs the bucket's representative value (`0.0` for the zero bucket) with its
/// accumulated stream weight.
#[derive(Clone)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct DdSketchIter<'a, W = u64> {
    mapping: IndexMapping,
    neg_iter: StoreIter<'a, W>,
    zero_weight: Option<W>,
    pos_iter: StoreIter<'a, W>,
    remaining: usize,
}

impl<'a, W: DdSketchWeight> DdSketchIter<'a, W> {
    pub(super) fn new(
        mapping: IndexMapping,
        neg_iter: StoreIter<'a, W>,
        zero_weight: W,
        pos_iter: StoreIter<'a, W>,
        remaining: usize,
    ) -> Self {
        Self {
            mapping,
            neg_iter,
            zero_weight: (!zero_weight.is_zero()).then_some(zero_weight),
            pos_iter,
            remaining,
        }
    }
}

impl<W> fmt::Debug for DdSketchIter<'_, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DdSketchIter")
            .field("remaining", &self.remaining)
            .finish()
    }
}

impl<W: DdSketchWeight> Iterator for DdSketchIter<'_, W> {
    type Item = (f64, W);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((index, weight)) = self.neg_iter.next_back() {
            self.remaining -= 1;
            return Some((-self.mapping.value(index), weight));
        }
        if let Some(weight) = self.zero_weight.take() {
            self.remaining -= 1;
            return Some((0.0, weight));
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

impl<W: DdSketchWeight> DoubleEndedIterator for DdSketchIter<'_, W> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some((index, weight)) = self.pos_iter.next_back() {
            self.remaining -= 1;
            return Some((self.mapping.value(index), weight));
        }
        if let Some(weight) = self.zero_weight.take() {
            self.remaining -= 1;
            return Some((0.0, weight));
        }
        if let Some((index, weight)) = self.neg_iter.next() {
            self.remaining -= 1;
            return Some((-self.mapping.value(index), weight));
        }
        None
    }
}

impl<W: DdSketchWeight> ExactSizeIterator for DdSketchIter<'_, W> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

impl<W: DdSketchWeight> FusedIterator for DdSketchIter<'_, W> {}
