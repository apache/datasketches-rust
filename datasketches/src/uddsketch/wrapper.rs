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

use crate::error::Error;
use crate::uddsketch::mapping::Mapping;
use crate::uddsketch::serialization::BUCKET_BYTES;
use crate::uddsketch::serialization::decode_bucket;
use crate::uddsketch::serialization::validate_sketch_image;
use crate::uddsketch::sketch::check_rank;
use crate::uddsketch::sketch::quantile_in_sources;
use crate::uddsketch::sketch::quantiles_in_sources;
use crate::uddsketch::store::Bucket;
use crate::uddsketch::store::BucketSource;

#[derive(Clone, Copy)]
struct RawBuckets<'a> {
    bytes: &'a [u8],
    total_weight: u64,
}

impl BucketSource for RawBuckets<'_> {
    #[inline]
    fn total_weight(self) -> u64 {
        self.total_weight
    }

    #[inline]
    fn len(self) -> usize {
        self.bytes.len() / BUCKET_BYTES
    }

    #[inline]
    fn iter_asc(self) -> impl Iterator<Item = Bucket> {
        self.bytes.chunks_exact(BUCKET_BYTES).map(decode_bucket)
    }

    #[inline]
    fn iter_desc(self) -> impl Iterator<Item = Bucket> {
        self.bytes
            .chunks_exact(BUCKET_BYTES)
            .rev()
            .map(decode_bucket)
    }
}

/// Zero-copy read-only wrapper over a serialized [`UddSketch`](super::UddSketch) image.
///
/// Validates the serialized header, bucket invariants, extrema, and collapse bounds on
/// construction without copying buckets onto the heap, and answers quantile queries directly from
/// the borrowed byte slice.
#[derive(Clone, Copy)]
pub struct UddSketchWrapper<'a> {
    max_buckets: u32,
    mapping: Mapping,
    num_collapses: u8,
    total_weight: u64,
    zero_weight: u64,
    min: f64,
    max: f64,
    neg: RawBuckets<'a>,
    pos: RawBuckets<'a>,
}

impl fmt::Debug for UddSketchWrapper<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UddSketchWrapper")
            .field("max_buckets", &self.max_buckets)
            .field("initial_alpha", &self.initial_alpha())
            .field("alpha", &self.alpha())
            .field("num_collapses", &self.num_collapses)
            .field("is_empty", &self.is_empty())
            .field("total_weight", &self.total_weight)
            .field("zero_weight", &self.zero_weight)
            .field("num_retained", &self.num_retained())
            .field("min_value", &self.min_value())
            .field("max_value", &self.max_value())
            .finish()
    }
}

impl<'a> UddSketchWrapper<'a> {
    /// Validates and wraps a serialized UDDSketch byte slice without allocating bucket storage.
    ///
    /// See the [module documentation](crate::uddsketch#serialization) for format
    /// compatibility notes.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidData`](crate::error::ErrorKind::InvalidData) if the image is truncated, has
    /// an unsupported header, or violates bucket ordering, count, budget, or extrema invariants.
    pub fn new(bytes: &'a [u8]) -> Result<Self, Error> {
        let image = validate_sketch_image(bytes)?;
        Ok(Self {
            max_buckets: image.max_buckets,
            mapping: image.mapping,
            num_collapses: image.num_collapses,
            total_weight: image.total_weight,
            zero_weight: image.zero_weight,
            min: image.min,
            max: image.max,
            neg: RawBuckets {
                bytes: image.neg_bytes,
                total_weight: image.neg_weight,
            },
            pos: RawBuckets {
                bytes: image.pos_bytes,
                total_weight: image.pos_weight,
            },
        })
    }

    /// Returns the maximum number of nonzero buckets retained by this sketch.
    pub fn max_buckets(&self) -> u32 {
        self.max_buckets
    }

    /// Returns the configured initial relative error parameter before any uniform collapses.
    pub fn initial_alpha(&self) -> f64 {
        self.mapping.initial_alpha()
    }

    /// Returns the effective algorithmic relative value error bound at the current collapse level.
    pub fn alpha(&self) -> f64 {
        self.mapping.alpha_at_level(self.num_collapses)
    }

    /// Returns the number of uniform collapses performed on this sketch.
    pub fn num_collapses(&self) -> u8 {
        self.num_collapses
    }

    /// Returns `true` if this sketch has not observed any finite values.
    pub fn is_empty(&self) -> bool {
        self.total_weight == 0
    }

    /// Returns the total accumulated stream weight across negative, zero, and positive values.
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns the accumulated stream weight of exact zero (`0.0` and `-0.0`) values.
    pub fn zero_weight(&self) -> u64 {
        self.zero_weight
    }

    /// Returns the number of nonzero buckets currently retained (`<= max_buckets()`).
    pub fn num_retained(&self) -> usize {
        self.neg.len() + self.pos.len()
    }

    /// Returns the minimum finite value seen by this sketch, or `None` if it is empty.
    pub fn min_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.min)
        }
    }

    /// Returns the maximum finite value seen by this sketch, or `None` if it is empty.
    pub fn max_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.max)
        }
    }

    /// Returns the approximate lower discrete quantile for `rank` in `[0.0, 1.0]`.
    ///
    /// Returns `Ok(None)` if the sketch is empty.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `rank` is NaN or
    /// outside `[0.0, 1.0]`, even if the sketch is empty.
    pub fn quantile(&self, rank: f64) -> Result<Option<f64>, Error> {
        check_rank(rank)?;
        if self.is_empty() {
            return Ok(None);
        }
        Ok(Some(quantile_in_sources(
            self.mapping,
            self.num_collapses,
            self.total_weight,
            self.zero_weight,
            self.min,
            self.max,
            self.neg,
            self.pos,
            rank,
        )))
    }

    /// Returns approximate lower discrete quantiles for each normalized rank in `ranks`.
    ///
    /// Returns `Ok(None)` if the sketch is empty. Otherwise, an empty `ranks` slice returns
    /// `Ok(Some(vec![]))`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if any rank is NaN or
    /// outside `[0.0, 1.0]`, even if the sketch is empty.
    pub fn quantiles(&self, ranks: &[f64]) -> Result<Option<Vec<f64>>, Error> {
        for &rank in ranks {
            check_rank(rank)?;
        }
        if self.is_empty() {
            return Ok(None);
        }
        Ok(Some(quantiles_in_sources(
            self.mapping,
            self.num_collapses,
            self.total_weight,
            self.zero_weight,
            self.min,
            self.max,
            self.neg,
            self.pos,
            ranks,
        )))
    }
}
