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
use std::slice::ChunksExact;

use crate::density::sketch::Level;

/// Iterator over `(point, weight)` pairs retained in a
/// [`DensitySketch`](crate::density::DensitySketch).
///
/// Each yielded entry borrows the point slice directly from the sketch together with its level
/// weight `1_u64 << height`.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct DensitySketchIter<'a, T> {
    levels: &'a [Level<T>],
    dim: usize,
    height: usize,
    level_iter: Option<ChunksExact<'a, T>>,
    remaining: usize,
}

impl<'a, T> DensitySketchIter<'a, T> {
    pub(super) fn new(levels: &'a [Level<T>], dim: usize, num_retained: usize) -> Self {
        let mut iter = Self {
            levels,
            dim,
            height: 0,
            level_iter: None,
            remaining: num_retained,
        };
        iter.advance_level();
        iter
    }

    fn advance_level(&mut self) {
        while self.height < self.levels.len() {
            let coords = &self.levels[self.height].coords;
            if !coords.is_empty() {
                self.level_iter = Some(coords.chunks_exact(self.dim));
                return;
            }
            self.height += 1;
        }
        self.level_iter = None;
    }
}

impl<T> Clone for DensitySketchIter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            levels: self.levels,
            dim: self.dim,
            height: self.height,
            level_iter: self.level_iter.clone(),
            remaining: self.remaining,
        }
    }
}

impl<T> fmt::Debug for DensitySketchIter<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DensitySketchIter")
            .field("remaining", &self.remaining)
            .field("current_level", &self.height)
            .field("num_levels", &self.levels.len())
            .finish()
    }
}

impl<'a, T> Iterator for DensitySketchIter<'a, T> {
    type Item = (&'a [T], u64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(level_iter) = &mut self.level_iter else {
                return None;
            };
            if let Some(point) = level_iter.next() {
                self.remaining -= 1;
                return Some((point, 1_u64 << self.height));
            }
            self.height += 1;
            self.advance_level();
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<T> ExactSizeIterator for DensitySketchIter<'_, T> {
    fn len(&self) -> usize {
        self.remaining
    }
}

impl<T> FusedIterator for DensitySketchIter<'_, T> {}
