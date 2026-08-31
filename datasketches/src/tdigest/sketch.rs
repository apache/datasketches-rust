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
use std::convert::identity;
use std::num::NonZeroU64;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::tdigest::serialization::COMPAT_DOUBLE;
use crate::tdigest::serialization::COMPAT_FLOAT;
use crate::tdigest::serialization::FLAGS_IS_EMPTY;
use crate::tdigest::serialization::FLAGS_IS_SINGLE_VALUE;
use crate::tdigest::serialization::FLAGS_REVERSE_MERGE;
use crate::tdigest::serialization::PREAMBLE_LONGS_EMPTY_OR_SINGLE;
use crate::tdigest::serialization::PREAMBLE_LONGS_MULTIPLE;
use crate::tdigest::serialization::SERIAL_VERSION;

/// The default value of K if one is not specified.
const DEFAULT_K: u16 = 200;
/// Multiplier for unmerged values relative to the target number of centroids.
const UNMERGED_MULTIPLIER: usize = 4;
/// Unmerged-value capacity allocated by the first update to a digest.
const INITIAL_UNMERGED_CAPACITY: usize = 8;
/// Default weight for single values.
const DEFAULT_WEIGHT: NonZeroU64 = NonZeroU64::new(1).unwrap();

// Centroids are stored as `[compressed prefix | unmerged unit-weight tail]`. Carrying a unit weight
// for each unmerged value lets updates, compression, and merge reuse one allocation instead of
// converting raw values into a second vector during compression.
#[derive(Debug, Clone, Default)]
struct TDigestBuffer {
    centroids: Vec<Centroid>,
    unmerged_tail_len: usize,
}

impl TDigestBuffer {
    fn new(centroids: Vec<Centroid>, unmerged_tail_len: usize) -> Self {
        debug_assert!(unmerged_tail_len <= centroids.len());
        TDigestBuffer {
            centroids,
            unmerged_tail_len,
        }
    }

    fn len(&self) -> usize {
        self.centroids.len()
    }

    fn is_empty(&self) -> bool {
        self.centroids.is_empty()
    }

    fn unmerged_len(&self) -> usize {
        self.unmerged_tail_len
    }

    fn compressed_prefix_len(&self) -> usize {
        self.centroids.len() - self.unmerged_tail_len
    }

    fn push_unmerged(&mut self, value: f64, max_unmerged: usize) {
        debug_assert!(self.unmerged_tail_len < max_unmerged);
        if self.centroids.len() == self.centroids.capacity() {
            let target_unmerged = if self.unmerged_tail_len == 0 {
                INITIAL_UNMERGED_CAPACITY
            } else if self.unmerged_tail_len == INITIAL_UNMERGED_CAPACITY {
                // Once a digest outgrows a tiny group, skip an extra allocator round trip while
                // keeping the first allocation small.
                (INITIAL_UNMERGED_CAPACITY * UNMERGED_MULTIPLIER * UNMERGED_MULTIPLIER)
                    .min(max_unmerged)
            } else {
                self.unmerged_tail_len
                    .saturating_mul(UNMERGED_MULTIPLIER)
                    .min(max_unmerged)
            };
            let target_capacity = self.compressed_prefix_len().saturating_add(target_unmerged);
            self.centroids
                .reserve_exact(target_capacity.saturating_sub(self.centroids.len()));
        }

        self.centroids.push(Centroid {
            mean: value,
            weight: DEFAULT_WEIGHT,
        });
        self.unmerged_tail_len += 1;
    }

    /// Returns all centroids in the tie order expected by stable compression sorting.
    ///
    /// The buffer is rotated from `[compressed | unmerged]` to `[unmerged | compressed]`, so new
    /// values stay before existing centroids when their means are equal.
    fn into_centroids_for_compression(mut self) -> Vec<Centroid> {
        debug_assert_ne!(self.unmerged_tail_len, 0);
        let compressed_prefix_len = self.compressed_prefix_len();
        self.centroids.rotate_left(compressed_prefix_len);
        self.centroids
    }

    /// Combines this buffer with a non-empty borrowed buffer in stable mean order.
    fn into_merged_centroids(mut self, other: &TDigestBuffer) -> Vec<Centroid> {
        debug_assert!(!other.is_empty(), "an empty right-hand buffer is a no-op");
        if self.unmerged_tail_len == 0
            && other.unmerged_tail_len == 0
            && centroids_are_sorted(&self.centroids)
            && centroids_are_sorted(&other.centroids)
        {
            merge_sorted_centroids(&mut self.centroids, &other.centroids);
            return self.centroids;
        }

        let compressed_prefix_len = self.compressed_prefix_len();
        self.centroids.reserve(other.len());
        let other_prefix_len = other.compressed_prefix_len();
        self.centroids
            .extend_from_slice(&other.centroids[other_prefix_len..]);
        self.centroids
            .extend_from_slice(&other.centroids[..other_prefix_len]);
        // Preserve the stable tie order: left unmerged, right unmerged and compressed, then the
        // left compressed prefix.
        self.centroids.rotate_left(compressed_prefix_len);
        self.centroids.sort_by(centroid_cmp);
        self.centroids
    }

    fn compressed_centroids(&self) -> &[Centroid] {
        assert_eq!(
            self.unmerged_tail_len, 0,
            "t-digest buffer must be compressed before reading centroids"
        );
        &self.centroids
    }

    fn into_compressed_centroids(self) -> Vec<Centroid> {
        assert_eq!(
            self.unmerged_tail_len, 0,
            "t-digest buffer must be compressed before reading centroids"
        );
        self.centroids
    }

    fn estimated_size(&self) -> usize {
        self.centroids.capacity() * size_of::<Centroid>()
    }
}

/// T-Digest sketch for estimating quantiles and ranks.
///
/// See the [module level documentation](super) for more.
#[derive(Debug, Clone)]
pub struct TDigestMut {
    k: u16,

    reverse_merge: bool,
    min: f64,
    max: f64,

    buffer: TDigestBuffer,
    // Weight represented by the compressed prefix. The unmerged tail contributes one per
    // centroid and is counted separately by `TDigestBuffer::unmerged_len`.
    compressed_weight: u64,
}

impl Default for TDigestMut {
    fn default() -> Self {
        Self::make(
            DEFAULT_K,
            false,
            f64::INFINITY,
            f64::NEG_INFINITY,
            TDigestBuffer::default(),
            0,
        )
    }
}

impl TDigestMut {
    /// Creates a mutable t-digest with the given `k` value.
    ///
    /// # Errors
    ///
    /// Returns an error if `k` is less than `10`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let sketch = TDigestMut::new(100).unwrap();
    /// assert_eq!(sketch.k(), 100);
    /// ```
    pub fn new(k: u16) -> Result<Self, Error> {
        if k < 10 {
            return Err(Error::invalid_argument(format!(
                "k must be at least 10, got {k}"
            )));
        }

        Ok(Self::make(
            k,
            false,
            f64::INFINITY,
            f64::NEG_INFINITY,
            TDigestBuffer::default(),
            0,
        ))
    }

    fn make(
        k: u16,
        reverse_merge: bool,
        min: f64,
        max: f64,
        buffer: TDigestBuffer,
        compressed_weight: u64,
    ) -> Self {
        debug_assert!(k >= 10, "k must be at least 10");
        debug_assert!(buffer.unmerged_tail_len <= buffer.centroids.len());
        debug_assert!(buffer.compressed_prefix_len() != 0 || compressed_weight == 0);

        TDigestMut {
            k,
            reverse_merge,
            min,
            max,
            buffer,
            compressed_weight,
        }
    }

    fn target_centroids(&self) -> usize {
        let fudge = if self.k < 30 { 30 } else { 10 };
        (usize::from(self.k) * 2) + fudge
    }

    fn max_unmerged(&self) -> usize {
        self.target_centroids() * UNMERGED_MULTIPLIER
    }

    fn target_retained_capacity(&self) -> usize {
        self.target_centroids() + self.max_unmerged()
    }

    /// Updates this t-digest with the given value.
    ///
    /// [f64::NAN], [f64::INFINITY], and [f64::NEG_INFINITY] values are ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// sketch.update(1.0);
    /// assert!(sketch.total_weight() >= 1);
    /// ```
    pub fn update(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }

        let max_unmerged = self.max_unmerged();
        if self.buffer.unmerged_len() >= max_unmerged {
            self.compress();
        }
        self.buffer.push_unmerged(value, max_unmerged);
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Returns the compression parameter `k` used to configure this t-digest.
    pub fn k(&self) -> u16 {
        self.k
    }

    /// Returns `true` if this t-digest has not seen any data.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the minimum value seen by this t-digest, or `None` if it is empty.
    pub fn min_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.min)
        }
    }

    /// Returns the maximum value seen by this t-digest, or `None` if it is empty.
    pub fn max_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.max)
        }
    }

    /// Returns the total weight.
    pub fn total_weight(&self) -> u64 {
        self.compressed_weight + self.buffer.unmerged_len() as u64
    }

    /// Merges the given t-digest into this one.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut left = TDigestMut::new(100).unwrap();
    /// let mut right = TDigestMut::new(100).unwrap();
    /// left.update(1.0);
    /// right.update(2.0);
    /// left.merge(&right);
    /// assert_eq!(left.total_weight(), 2);
    /// ```
    pub fn merge(&mut self, other: &TDigestMut) {
        if other.is_empty() {
            return;
        }

        let self_unmerged_weight = self.buffer.unmerged_len() as u64;
        let centroids = std::mem::take(&mut self.buffer).into_merged_centroids(&other.buffer);
        self.compress_sorted_centroids(centroids, self_unmerged_weight + other.total_weight())
    }

    /// Converts this mutable t-digest into an immutable one.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// sketch.update(1.0);
    /// let frozen = sketch.freeze();
    /// assert!(!frozen.is_empty());
    /// ```
    pub fn freeze(mut self) -> TDigest {
        self.compress();
        let mut centroids = self.buffer.into_compressed_centroids();
        // A mutable digest retains update workspace for reuse. The immutable form cannot use that
        // spare capacity, so release it at this consuming boundary.
        centroids.shrink_to_fit();
        TDigest {
            k: self.k,
            reverse_merge: self.reverse_merge,
            min: self.min,
            max: self.max,
            centroids,
            centroids_weight: self.compressed_weight,
        }
    }

    fn view(&mut self) -> TDigestView<'_> {
        self.compress(); // side effect
        TDigestView {
            min: self.min,
            max: self.max,
            centroids: self.buffer.compressed_centroids(),
            centroids_weight: self.compressed_weight,
        }
    }

    /// Returns the cumulative distribution approximation described by [`TDigest::cdf`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// for value in [1.0, 2.0, 3.0] {
    ///     sketch.update(value);
    /// }
    /// let cdf = sketch.cdf(&[1.5]).unwrap();
    /// assert_eq!(cdf.len(), 2);
    /// ```
    pub fn cdf(&mut self, split_points: &[f64]) -> Option<Vec<f64>> {
        check_split_points(split_points);

        if self.is_empty() {
            return None;
        }

        self.view().cdf(split_points)
    }

    /// Returns the probability mass approximation described by [`TDigest::pmf`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// for value in [1.0, 2.0, 3.0] {
    ///     sketch.update(value);
    /// }
    /// let pmf = sketch.pmf(&[1.5]).unwrap();
    /// assert_eq!(pmf.len(), 2);
    /// ```
    pub fn pmf(&mut self, split_points: &[f64]) -> Option<Vec<f64>> {
        check_split_points(split_points);

        if self.is_empty() {
            return None;
        }

        self.view().pmf(split_points)
    }

    /// Returns the normalized rank described by [`TDigest::rank`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// for value in [1.0, 2.0, 3.0] {
    ///     sketch.update(value);
    /// }
    /// let rank = sketch.rank(2.0).unwrap();
    /// assert!((0.0..=1.0).contains(&rank));
    /// ```
    pub fn rank(&mut self, value: f64) -> Option<f64> {
        assert!(!value.is_nan(), "value must not be NaN");

        if self.is_empty() {
            return None;
        }
        if value < self.min {
            return Some(0.0);
        }
        if value > self.max {
            return Some(1.0);
        }
        // one centroid and value == min == max
        if self.buffer.len() == 1 {
            return Some(0.5);
        }

        self.view().rank(value)
    }

    /// Returns the quantile described by [`TDigest::quantile`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// for value in [1.0, 2.0, 3.0] {
    ///     sketch.update(value);
    /// }
    /// let median = sketch.quantile(0.5).unwrap();
    /// assert!((1.0..=3.0).contains(&median));
    /// ```
    pub fn quantile(&mut self, rank: f64) -> Option<f64> {
        assert!((0.0..=1.0).contains(&rank), "rank must be in [0.0, 1.0]");

        if self.is_empty() {
            return None;
        }

        self.view().quantile(rank)
    }

    /// Serializes this mutable t-digest to bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// sketch.update(1.0);
    /// let bytes = sketch.serialize();
    /// let decoded = TDigestMut::deserialize(&bytes).unwrap();
    /// assert_eq!(decoded.max_value(), Some(1.0));
    /// ```
    pub fn serialize(&mut self) -> Vec<u8> {
        self.compress();
        serialize_compressed(
            self.k,
            self.reverse_merge,
            self.min,
            self.max,
            self.buffer.compressed_centroids(),
            self.compressed_weight,
        )
    }

    /// Deserializes a mutable t-digest from the standard double-precision format.
    ///
    /// The format of the [reference implementation](https://github.com/tdunning/t-digest) is
    /// auto-detected. Use [`deserialize_f32()`](Self::deserialize_f32) for the compact
    /// DataSketches C++ `tdigest<float>` format.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// sketch.update(1.0);
    /// sketch.update(2.0);
    /// let bytes = sketch.serialize();
    /// let decoded = TDigestMut::deserialize(&bytes).unwrap();
    /// assert_eq!(decoded.max_value(), Some(2.0));
    /// ```
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_impl(bytes, false)
    }

    /// Deserializes a mutable t-digest from the compact single-precision DataSketches format.
    ///
    /// This format stores centroid means and weights as `(f32, u32)` and is emitted by the C++
    /// `tdigest<float>` implementation. Its header does not identify the scalar width, so callers
    /// must select this entry point explicitly.
    pub fn deserialize_f32(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_impl(bytes, true)
    }

    fn deserialize_impl(bytes: &[u8], is_f32: bool) -> Result<Self, Error> {
        let mut cursor = SketchSlice::new(bytes);

        let preamble_longs = cursor
            .read_u8()
            .map_err(insufficient_data("preamble_longs"))?;
        let serial_version = cursor
            .read_u8()
            .map_err(insufficient_data("serial_version"))?;
        let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
        if let Err(err) = Family::TDIGEST.validate_id(family_id) {
            return if preamble_longs == 0 && serial_version == 0 && family_id == 0 {
                Self::deserialize_compat(bytes)
            } else {
                Err(err)
            };
        }
        ensure_serial_version_is(SERIAL_VERSION, serial_version)?;
        let k = cursor.read_u16_le().map_err(insufficient_data("k"))?;
        if k < 10 {
            return Err(Error::deserial(format!("k must be at least 10, got {k}")));
        }
        let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
        let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
        let is_single_value = (flags & FLAGS_IS_SINGLE_VALUE) != 0;
        let expected_preamble_longs = if is_empty || is_single_value {
            PREAMBLE_LONGS_EMPTY_OR_SINGLE
        } else {
            PREAMBLE_LONGS_MULTIPLE
        };
        ensure_preamble_longs_in(&[expected_preamble_longs], preamble_longs)?;
        cursor
            .read_u16_le()
            .map_err(insufficient_data("<unused>"))?; // unused
        if is_empty {
            return TDigestMut::new(k);
        }

        let reverse_merge = (flags & FLAGS_REVERSE_MERGE) != 0;
        if is_single_value {
            let value = if is_f32 {
                cursor
                    .read_f32_le()
                    .map_err(insufficient_data("single_value"))? as f64
            } else {
                cursor
                    .read_f64_le()
                    .map_err(insufficient_data("single_value"))?
            };
            check_non_nan(value, "single_value")?;
            check_finite(value, "single_value")?;
            return Ok(TDigestMut::make(
                k,
                reverse_merge,
                value,
                value,
                TDigestBuffer::new(
                    vec![Centroid {
                        mean: value,
                        weight: DEFAULT_WEIGHT,
                    }],
                    0,
                ),
                1,
            ));
        }
        let num_centroids = cursor
            .read_u32_le()
            .map_err(insufficient_data("num_centroids"))? as usize;
        let num_buffered = cursor
            .read_u32_le()
            .map_err(insufficient_data("num_buffered"))? as usize;
        let (min, max) = if is_f32 {
            (
                cursor.read_f32_le().map_err(insufficient_data("min"))? as f64,
                cursor.read_f32_le().map_err(insufficient_data("max"))? as f64,
            )
        } else {
            (
                cursor.read_f64_le().map_err(insufficient_data("min"))?,
                cursor.read_f64_le().map_err(insufficient_data("max"))?,
            )
        };
        check_non_nan(min, "min")?;
        check_non_nan(max, "max")?;
        check_finite(min, "min")?;
        check_finite(max, "max")?;
        let (centroid_bytes, buffered_value_bytes) = if is_f32 {
            (size_of::<f32>() + size_of::<u32>(), size_of::<f32>())
        } else {
            (size_of::<f64>() + size_of::<u64>(), size_of::<f64>())
        };
        let centroid_payload_bytes = num_centroids
            .checked_mul(centroid_bytes)
            .ok_or_else(|| Error::deserial("TDigest payload size exceeds the supported size"))?;
        let buffered_payload_bytes = num_buffered
            .checked_mul(buffered_value_bytes)
            .ok_or_else(|| Error::deserial("TDigest payload size exceeds the supported size"))?;
        let required_payload_bytes = centroid_payload_bytes
            .checked_add(buffered_payload_bytes)
            .ok_or_else(|| Error::deserial("TDigest payload size exceeds the supported size"))?;
        let remaining = cursor.remaining();
        if remaining.len() < required_payload_bytes {
            return Err(Error::insufficient_data(format!(
                "TDigest payload requires {required_payload_bytes} bytes, got {}",
                remaining.len()
            )));
        }
        // Check the whole payload once so fixed-width records can be decoded without per-field I/O.
        let (centroid_payload, buffered_payload) =
            remaining[..required_payload_bytes].split_at(centroid_payload_bytes);
        let stored_centroids = num_centroids.checked_add(num_buffered).ok_or_else(|| {
            Error::deserial("num_centroids and num_buffered exceed the supported size")
        })?;
        let mut centroids = Vec::with_capacity(stored_centroids);
        let mut compressed_weight = 0u64;
        for bytes in centroid_payload.chunks_exact(centroid_bytes) {
            let (mean, weight) = if is_f32 {
                (
                    f32::from_le_bytes(bytes[..4].try_into().unwrap()) as f64,
                    u32::from_le_bytes(bytes[4..].try_into().unwrap()) as u64,
                )
            } else {
                (
                    f64::from_le_bytes(bytes[..8].try_into().unwrap()),
                    u64::from_le_bytes(bytes[8..].try_into().unwrap()),
                )
            };
            check_non_nan(mean, "centroid mean")?;
            check_finite(mean, "centroid")?;
            let weight = check_nonzero(weight, "centroid weight")?;
            compressed_weight = checked_weight_sum(compressed_weight, weight.get())?;
            centroids.push(Centroid { mean, weight });
        }
        checked_weight_sum(compressed_weight, num_buffered as u64)?;
        for bytes in buffered_payload.chunks_exact(buffered_value_bytes) {
            let value = if is_f32 {
                f32::from_le_bytes(bytes.try_into().unwrap()) as f64
            } else {
                f64::from_le_bytes(bytes.try_into().unwrap())
            };
            check_non_nan(value, "buffered_value mean")?;
            check_finite(value, "buffered_value mean")?;
            centroids.push(Centroid {
                mean: value,
                weight: DEFAULT_WEIGHT,
            });
        }
        Ok(TDigestMut::make(
            k,
            reverse_merge,
            min,
            max,
            TDigestBuffer::new(centroids, num_buffered),
            compressed_weight,
        ))
    }

    // compatibility with the format of the reference implementation
    // default byte order of ByteBuffer is used there, which is big endian
    fn deserialize_compat(bytes: &[u8]) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data_of("compat format", tag)
        }

        let mut cursor = SketchSlice::new(bytes);

        let ty = cursor.read_u32_be().map_err(make_error("type"))?;
        match ty {
            COMPAT_DOUBLE => {
                fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
                    move |_| Error::insufficient_data_of("compat double format", tag)
                }
                // compatibility with asBytes()
                let min = cursor.read_f64_be().map_err(make_error("min"))?;
                let max = cursor.read_f64_be().map_err(make_error("max"))?;
                check_non_nan(min, "min in compat double format")?;
                check_non_nan(max, "max in compat double format")?;
                check_finite(min, "min in compat double format")?;
                check_finite(max, "max in compat double format")?;
                let k = cursor.read_f64_be().map_err(make_error("k"))? as u16;
                if k < 10 {
                    return Err(Error::deserial(format!(
                        "k must be at least 10 in compat double format, got {k}"
                    )));
                }
                let num_centroids =
                    cursor.read_u32_be().map_err(make_error("num_centroids"))? as usize;
                let mut total_weight = 0u64;
                let mut centroids = Vec::with_capacity(num_centroids);
                for _ in 0..num_centroids {
                    let weight = cursor.read_f64_be().map_err(make_error("weight"))?;
                    let mean = cursor.read_f64_be().map_err(make_error("mean"))?;
                    let weight =
                        check_compat_weight(weight, "centroid weight in compat double format")?;
                    check_non_nan(mean, "centroid mean in compat double format")?;
                    check_finite(mean, "centroid mean in compat double format")?;
                    total_weight = checked_weight_sum(total_weight, weight.get())?;
                    centroids.push(Centroid { mean, weight });
                }
                Ok(TDigestMut::make(
                    k,
                    false,
                    min,
                    max,
                    TDigestBuffer::new(centroids, 0),
                    total_weight,
                ))
            }
            COMPAT_FLOAT => {
                fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
                    move |_| Error::insufficient_data_of("compat float format", tag)
                }
                // COMPAT_FLOAT: compatibility with asSmallBytes()
                // reference implementation uses doubles for min and max
                let min = cursor.read_f64_be().map_err(make_error("min"))?;
                let max = cursor.read_f64_be().map_err(make_error("max"))?;
                check_non_nan(min, "min in compat float format")?;
                check_non_nan(max, "max in compat float format")?;
                check_finite(min, "min in compat float format")?;
                check_finite(max, "max in compat float format")?;
                let k = cursor.read_f32_be().map_err(make_error("k"))? as u16;
                if k < 10 {
                    return Err(Error::deserial(format!(
                        "k must be at least 10 in compat float format, got {k}"
                    )));
                }
                // reference implementation stores capacities of the array of centroids and the
                // buffer as shorts they can be derived from k in the constructor
                cursor.read_u32_be().map_err(make_error("<unused>"))?;
                let num_centroids =
                    cursor.read_u16_be().map_err(make_error("num_centroids"))? as usize;
                let mut total_weight = 0u64;
                let mut centroids = Vec::with_capacity(num_centroids);
                for _ in 0..num_centroids {
                    let weight = cursor.read_f32_be().map_err(make_error("weight"))? as f64;
                    let mean = cursor.read_f32_be().map_err(make_error("mean"))? as f64;
                    let weight =
                        check_compat_weight(weight, "centroid weight in compat float format")?;
                    check_non_nan(mean, "centroid mean in compat float format")?;
                    check_finite(mean, "centroid mean in compat float format")?;
                    total_weight = checked_weight_sum(total_weight, weight.get())?;
                    centroids.push(Centroid { mean, weight });
                }
                Ok(TDigestMut::make(
                    k,
                    false,
                    min,
                    max,
                    TDigestBuffer::new(centroids, 0),
                    total_weight,
                ))
            }
            ty => Err(Error::deserial(format!("unknown TDigest compat type {ty}"))),
        }
    }

    /// Processes unmerged values and merges centroids if needed.
    fn compress(&mut self) {
        let additional_weight = self.buffer.unmerged_len() as u64;
        if additional_weight == 0 {
            // Also preserves fully compressed deserialized images verbatim.
            return;
        }
        let centroids = std::mem::take(&mut self.buffer).into_centroids_for_compression();
        self.compress_centroids(centroids, additional_weight);
    }

    /// Compresses the given centroids into this t-digest.
    ///
    /// # Contract
    ///
    /// * `centroids` must contain at least one centroid.
    /// * `centroids` contains every centroid to be merged, including all centroids previously
    ///   stored in `self`.
    /// * `additional_weight` is the total weight not yet included in `self.compressed_weight`.
    /// * Every centroid mean in `centroids` is finite.
    /// * `self.buffer` has no unmerged values before returning.
    fn compress_centroids(&mut self, mut centroids: Vec<Centroid>, additional_weight: u64) {
        debug_assert!(!centroids.is_empty());
        centroids.sort_by(centroid_cmp);
        self.compress_sorted_centroids(centroids, additional_weight);
    }

    fn compress_sorted_centroids(&mut self, mut centroids: Vec<Centroid>, additional_weight: u64) {
        debug_assert!(!centroids.is_empty());
        debug_assert!(centroids_are_sorted(&centroids));
        if self.reverse_merge {
            centroids.reverse();
        }
        self.compressed_weight += additional_weight;

        let mut num_centroids = 1;
        let len = centroids.len();
        let compressed_weight = self.compressed_weight as f64;
        let normalizer = scale_function::normalizer(2.0 * f64::from(self.k), compressed_weight);
        let mut current = 1;
        let mut weight_so_far = 0.;
        while current < len {
            let c = centroids[current];
            let proposed_weight = centroids[num_centroids - 1].weight() + c.weight();
            let mut add_this = false;
            if (current != 1) && (current != (len - 1)) {
                let q0 = weight_so_far / compressed_weight;
                let q2 = (weight_so_far + proposed_weight) / compressed_weight;
                add_this = proposed_weight
                    <= (compressed_weight
                        * scale_function::max(q0, normalizer)
                            .min(scale_function::max(q2, normalizer)));
            }
            if add_this {
                // merge into existing centroid
                centroids[num_centroids - 1].add(c);
            } else {
                // copy to a new centroid
                weight_so_far += centroids[num_centroids - 1].weight();
                centroids[num_centroids] = c;
                num_centroids += 1;
            }
            current += 1;
        }

        centroids.truncate(num_centroids);
        if self.reverse_merge {
            centroids.reverse();
        }
        self.min = self.min.min(centroids[0].mean);
        self.max = self.max.max(centroids[num_centroids - 1].mean);
        self.reverse_merge = !self.reverse_merge;
        self.reduce_retained_capacity(&mut centroids);
        self.buffer = TDigestBuffer::new(centroids, 0);
    }

    fn reduce_retained_capacity(&self, centroids: &mut Vec<Centroid>) {
        let target_capacity = self.target_retained_capacity().max(centroids.len());
        if centroids.capacity() <= target_capacity {
            return;
        }

        // A merge can temporarily exceed the update-path target. Shrink after compaction so one
        // unusually large input does not pin that peak capacity for the rest of the digest's life.
        centroids.shrink_to(target_capacity);
    }

    /// Returns the estimated size of the sketch in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.buffer.estimated_size()
    }
}

fn serialize_compressed(
    k: u16,
    reverse_merge: bool,
    min: f64,
    max: f64,
    centroids: &[Centroid],
    total_weight: u64,
) -> Vec<u8> {
    let is_empty = centroids.is_empty();
    let is_single_value = total_weight == 1;
    let mut total_size = if is_empty || is_single_value {
        // Preamble, serial version, family, k, flags, and two unused bytes.
        size_of::<u64>()
    } else {
        // The short header plus centroid and buffered-value counts.
        size_of::<u64>() * 2
    };
    if is_single_value {
        total_size += size_of::<f64>();
    } else if !is_empty {
        total_size += size_of::<f64>() * 2;
        total_size += centroids.len() * (size_of::<f64>() + size_of::<u64>());
    }

    let mut bytes = SketchBytes::with_capacity(total_size);
    bytes.write_u8(if is_empty || is_single_value {
        PREAMBLE_LONGS_EMPTY_OR_SINGLE
    } else {
        PREAMBLE_LONGS_MULTIPLE
    });
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(Family::TDIGEST.id);
    bytes.write_u16_le(k);
    bytes.write_u8({
        let mut flags = 0;
        if is_empty {
            flags |= FLAGS_IS_EMPTY;
        }
        if is_single_value {
            flags |= FLAGS_IS_SINGLE_VALUE;
        }
        if reverse_merge {
            flags |= FLAGS_REVERSE_MERGE;
        }
        flags
    });
    bytes.write_u16_le(0); // unused
    if is_empty {
        return bytes.into_bytes();
    }
    if is_single_value {
        bytes.write_f64_le(min);
        return bytes.into_bytes();
    }
    bytes.write_u32_le(centroids.len() as u32);
    bytes.write_u32_le(0); // no buffered values
    bytes.write_f64_le(min);
    bytes.write_f64_le(max);
    for centroid in centroids {
        bytes.write_f64_le(centroid.mean);
        bytes.write_u64_le(centroid.weight.get());
    }
    bytes.into_bytes()
}

/// Immutable (frozen) T-Digest sketch for estimating quantiles and ranks.
///
/// See the [module level documentation](super) for more.
pub struct TDigest {
    k: u16,

    reverse_merge: bool,
    min: f64,
    max: f64,

    centroids: Vec<Centroid>,
    centroids_weight: u64,
}

impl TDigest {
    /// Returns the compression parameter `k` used to configure this t-digest.
    pub fn k(&self) -> u16 {
        self.k
    }

    /// Returns `true` if this t-digest has not seen any data.
    pub fn is_empty(&self) -> bool {
        self.centroids.is_empty()
    }

    /// Returns the minimum value seen by this t-digest, or `None` if it is empty.
    pub fn min_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.min)
        }
    }

    /// Returns the maximum value seen by this t-digest, or `None` if it is empty.
    pub fn max_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.max)
        }
    }

    /// Returns the total weight.
    pub fn total_weight(&self) -> u64 {
        self.centroids_weight
    }

    /// Serializes this immutable t-digest to bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigest;
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// sketch.update(1.0);
    /// let digest = sketch.freeze();
    /// let bytes = digest.serialize();
    /// let decoded = TDigest::deserialize(&bytes).unwrap();
    /// assert_eq!(decoded.max_value(), Some(1.0));
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        serialize_compressed(
            self.k,
            self.reverse_merge,
            self.min,
            self.max,
            &self.centroids,
            self.centroids_weight,
        )
    }

    /// Deserializes an immutable t-digest from the standard double-precision format.
    ///
    /// The format of the [reference implementation](https://github.com/tdunning/t-digest) is
    /// auto-detected. Use [`deserialize_f32()`](Self::deserialize_f32) for the compact
    /// DataSketches C++ `tdigest<float>` format.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Ok(TDigestMut::deserialize(bytes)?.freeze())
    }

    /// Deserializes an immutable t-digest from the compact single-precision DataSketches format.
    pub fn deserialize_f32(bytes: &[u8]) -> Result<Self, Error> {
        Ok(TDigestMut::deserialize_f32(bytes)?.freeze())
    }

    fn view(&self) -> TDigestView<'_> {
        TDigestView {
            min: self.min,
            max: self.max,
            centroids: &self.centroids,
            centroids_weight: self.centroids_weight,
        }
    }

    /// Returns an approximation to the Cumulative Distribution Function (CDF), which is the
    /// cumulative analog of the PMF, of the input stream given a set of split points.
    ///
    /// # Arguments
    ///
    /// * `split_points`: An array of _m_ unique, monotonically increasing values that divide the
    ///   input domain into _m+1_ consecutive disjoint intervals.
    ///
    /// # Returns
    ///
    /// An array of m+1 doubles, which are a consecutive approximation to the CDF of the input
    /// stream given the split points. The value at array position j of the returned CDF array
    /// is the sum of the returned values in positions 0 through j of the returned PMF array.
    /// This can be viewed as array of ranks of the given split points plus one more value that
    /// is always 1. An empty `split_points` slice returns the single value `[1.0]`.
    ///
    /// Returns `None` if this t-digest is empty.
    ///
    /// # Panics
    ///
    /// Panics if `split_points` is not unique, not monotonically increasing, or contains `NaN`
    /// values.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// for value in [1.0, 2.0, 3.0] {
    ///     sketch.update(value);
    /// }
    /// let digest = sketch.freeze();
    /// let cdf = digest.cdf(&[1.5]).unwrap();
    /// assert_eq!(cdf.len(), 2);
    /// ```
    pub fn cdf(&self, split_points: &[f64]) -> Option<Vec<f64>> {
        self.view().cdf(split_points)
    }

    /// Returns an approximation to the Probability Mass Function (PMF) of the input stream
    /// given a set of split points.
    ///
    /// # Arguments
    ///
    /// * `split_points`: An array of _m_ unique, monotonically increasing values that divide the
    ///   input domain into _m+1_ consecutive disjoint intervals (bins).
    ///
    /// # Returns
    ///
    /// An array of m+1 doubles each of which is an approximation to the fraction of the input
    /// stream values (the mass) that fall into one of those intervals.
    /// An empty `split_points` slice returns the single value `[1.0]`.
    ///
    /// Returns `None` if this t-digest is empty.
    ///
    /// # Panics
    ///
    /// Panics if `split_points` is not unique, not monotonically increasing, or contains `NaN`
    /// values.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// for value in [1.0, 2.0, 3.0] {
    ///     sketch.update(value);
    /// }
    /// let digest = sketch.freeze();
    /// let pmf = digest.pmf(&[1.5]).unwrap();
    /// assert_eq!(pmf.len(), 2);
    /// ```
    pub fn pmf(&self, split_points: &[f64]) -> Option<Vec<f64>> {
        self.view().pmf(split_points)
    }

    /// Computes the approximate normalized rank in `[0.0, 1.0]` of the given value.
    ///
    /// Returns `None` if this t-digest is empty.
    ///
    /// # Panics
    ///
    /// Panics if the value is `NaN`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// for value in [1.0, 2.0, 3.0] {
    ///     sketch.update(value);
    /// }
    /// let digest = sketch.freeze();
    /// let rank = digest.rank(2.0).unwrap();
    /// assert!((0.0..=1.0).contains(&rank));
    /// ```
    pub fn rank(&self, value: f64) -> Option<f64> {
        assert!(!value.is_nan(), "value must not be NaN");
        self.view().rank(value)
    }

    /// Computes the approximate quantile for the given normalized rank.
    ///
    /// Returns `None` if this t-digest is empty.
    ///
    /// # Panics
    ///
    /// Panics if `rank` is outside `[0.0, 1.0]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// for value in [1.0, 2.0, 3.0] {
    ///     sketch.update(value);
    /// }
    /// let digest = sketch.freeze();
    /// let q = digest.quantile(0.5).unwrap();
    /// assert!((1.0..=3.0).contains(&q));
    /// ```
    pub fn quantile(&self, rank: f64) -> Option<f64> {
        assert!((0.0..=1.0).contains(&rank), "rank must be in [0.0, 1.0]");
        self.view().quantile(rank)
    }

    /// Converts this immutable t-digest into a mutable one.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100).unwrap();
    /// sketch.update(1.0);
    /// let digest = sketch.freeze();
    /// let mut mutable = digest.unfreeze();
    /// mutable.update(2.0);
    /// assert_eq!(mutable.total_weight(), 2);
    /// ```
    pub fn unfreeze(self) -> TDigestMut {
        TDigestMut::make(
            self.k,
            self.reverse_merge,
            self.min,
            self.max,
            TDigestBuffer::new(self.centroids, 0),
            self.centroids_weight,
        )
    }

    /// Returns the estimated size of the sketch in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.centroids.capacity() * size_of::<Centroid>()
    }
}

struct TDigestView<'a> {
    min: f64,
    max: f64,
    centroids: &'a [Centroid],
    centroids_weight: u64,
}

impl TDigestView<'_> {
    fn pmf(&self, split_points: &[f64]) -> Option<Vec<f64>> {
        let mut buckets = self.cdf(split_points)?;
        for i in (1..buckets.len()).rev() {
            buckets[i] -= buckets[i - 1];
        }
        Some(buckets)
    }

    fn cdf(&self, split_points: &[f64]) -> Option<Vec<f64>> {
        check_split_points(split_points);

        if self.centroids.is_empty() {
            return None;
        }

        let mut ranks = Vec::with_capacity(split_points.len() + 1);
        for &p in split_points {
            match self.rank(p) {
                Some(rank) => ranks.push(rank),
                None => unreachable!("checked non-empty above"),
            }
        }
        ranks.push(1.0);
        Some(ranks)
    }

    fn rank(&self, value: f64) -> Option<f64> {
        debug_assert!(!value.is_nan(), "value must not be NaN");

        if self.centroids.is_empty() {
            return None;
        }
        if value < self.min {
            return Some(0.0);
        }
        if value > self.max {
            return Some(1.0);
        }
        // one centroid and value == min == max
        if self.centroids.len() == 1 {
            return Some(0.5);
        }

        let centroids_weight = self.centroids_weight as f64;
        let num_centroids = self.centroids.len();

        // left tail
        let first_mean = self.centroids[0].mean;
        if value < first_mean {
            if first_mean - self.min > 0. {
                return Some(if value == self.min {
                    0.5 / centroids_weight
                } else {
                    (1. + (((value - self.min) / (first_mean - self.min))
                        * ((self.centroids[0].weight() / 2.) - 1.)))
                        / centroids_weight
                });
            }
            return Some(0.); // should never happen
        }

        // right tail
        let last_mean = self.centroids[num_centroids - 1].mean;
        if value > last_mean {
            if self.max - last_mean > 0. {
                return Some(if value == self.max {
                    1. - (0.5 / centroids_weight)
                } else {
                    1.0 - ((1.0
                        + (((self.max - value) / (self.max - last_mean))
                            * ((self.centroids[num_centroids - 1].weight() / 2.) - 1.)))
                        / centroids_weight)
                });
            }
            return Some(1.); // should never happen
        }

        let mut lower = self
            .centroids
            .binary_search_by(|c| centroid_lower_bound(c, value))
            .unwrap_or_else(identity);
        assert_ne!(lower, num_centroids, "get_rank: lower == end");
        let mut upper = self
            .centroids
            .binary_search_by(|c| centroid_upper_bound(c, value))
            .unwrap_or_else(identity);
        assert_ne!(upper, 0, "get_rank: upper == begin");
        if value < self.centroids[lower].mean {
            lower -= 1;
        }
        if (upper == num_centroids) || (self.centroids[upper - 1].mean >= value) {
            upper -= 1;
        }

        let mut weight_below = 0.;
        let mut i = 0;
        while i < lower {
            weight_below += self.centroids[i].weight();
            i += 1;
        }
        weight_below += self.centroids[lower].weight() / 2.;

        let mut weight_delta = 0.;
        while i < upper {
            weight_delta += self.centroids[i].weight();
            i += 1;
        }
        weight_delta -= self.centroids[lower].weight() / 2.;
        weight_delta += self.centroids[upper].weight() / 2.;
        Some(
            if self.centroids[upper].mean - self.centroids[lower].mean > 0. {
                (weight_below
                    + (weight_delta * (value - self.centroids[lower].mean)
                        / (self.centroids[upper].mean - self.centroids[lower].mean)))
                    / centroids_weight
            } else {
                (weight_below + weight_delta / 2.) / centroids_weight
            },
        )
    }

    fn quantile(&self, rank: f64) -> Option<f64> {
        debug_assert!((0.0..=1.0).contains(&rank), "rank must be in [0.0, 1.0]");

        if self.centroids.is_empty() {
            return None;
        }

        if self.centroids.len() == 1 {
            return Some(self.centroids[0].mean);
        }

        // at least 2 centroids
        let centroids_weight = self.centroids_weight as f64;
        let num_centroids = self.centroids.len();
        let weight = rank * centroids_weight;
        if weight < 1. {
            return Some(self.min);
        }
        if weight > centroids_weight - 1. {
            return Some(self.max);
        }
        let first_weight = self.centroids[0].weight();
        if first_weight > 1. && weight < first_weight / 2. {
            return Some(
                self.min
                    + (((weight - 1.) / ((first_weight / 2.) - 1.))
                        * (self.centroids[0].mean - self.min)),
            );
        }
        let last_weight = self.centroids[num_centroids - 1].weight();
        if last_weight > 1. && (centroids_weight - weight <= last_weight / 2.) {
            if last_weight == 2. {
                return Some(self.max);
            }
            return Some(
                self.max
                    - (((centroids_weight - weight - 1.) / ((last_weight / 2.) - 1.))
                        * (self.max - self.centroids[num_centroids - 1].mean)),
            );
        }

        // interpolate between extremes
        let mut weight_so_far = first_weight / 2.;
        for i in 0..(num_centroids - 1) {
            let dw = (self.centroids[i].weight() + self.centroids[i + 1].weight()) / 2.;
            if weight_so_far + dw > weight {
                // the target weight is between centroids i and i+1
                let mut left_weight = 0.;
                if self.centroids[i].weight.get() == 1 {
                    if weight - weight_so_far < 0.5 {
                        return Some(self.centroids[i].mean);
                    }
                    left_weight = 0.5;
                }
                let mut right_weight = 0.;
                if self.centroids[i + 1].weight.get() == 1 {
                    if weight_so_far + dw - weight <= 0.5 {
                        return Some(self.centroids[i + 1].mean);
                    }
                    right_weight = 0.5;
                }
                // Each centroid is weighted by the distance from the target to the *other*
                // centroid, so the estimate approaches the nearer one.
                let distance_from_left = weight - weight_so_far - left_weight;
                let distance_to_right = weight_so_far + dw - weight - right_weight;
                return Some(weighted_average(
                    self.centroids[i].mean,
                    distance_to_right,
                    self.centroids[i + 1].mean,
                    distance_from_left,
                ));
            }
            weight_so_far += dw;
        }

        let w1 = weight - (centroids_weight) - ((self.centroids[num_centroids - 1].weight()) / 2.);
        let w2 = (self.centroids[num_centroids - 1].weight() / 2.) - w1;
        Some(weighted_average(
            self.centroids[num_centroids - 1].mean,
            w1,
            self.max,
            w2,
        ))
    }
}

/// Checks the sequential validity of the given array of double values.
/// They must be unique, monotonically increasing and not NaN.
#[track_caller]
fn check_split_points(split_points: &[f64]) {
    if split_points.iter().any(|split_point| split_point.is_nan()) {
        panic!("split_points must not contain NaN values: {split_points:?}");
    }
    if !split_points.windows(2).all(|pair| pair[0] < pair[1]) {
        panic!("split_points must be unique and monotonically increasing: {split_points:?}");
    }
}

fn centroid_cmp(a: &Centroid, b: &Centroid) -> Ordering {
    match a.mean.partial_cmp(&b.mean) {
        Some(order) => order,
        None => unreachable!("NaN values should never be present in centroids"),
    }
}

fn centroids_are_sorted(centroids: &[Centroid]) -> bool {
    centroids
        .windows(2)
        .all(|pair| centroid_cmp(&pair[0], &pair[1]) != Ordering::Greater)
}

fn merge_sorted_centroids(left: &mut Vec<Centroid>, right: &[Centroid]) {
    debug_assert!(!right.is_empty());
    debug_assert!(centroids_are_sorted(left));
    debug_assert!(centroids_are_sorted(right));

    let mut left_index = left.len();
    let mut right_index = right.len();
    let mut output_index = left_index + right_index;
    left.reserve(right.len());
    left.resize(output_index, right[0]);

    while left_index > 0 && right_index > 0 {
        let left_centroid = left[left_index - 1];
        let right_centroid = right[right_index - 1];
        output_index -= 1;
        // Taking the left side on ties while filling backward keeps the right side first in the
        // final order, matching a stable sort after rotating the compressed left prefix.
        if centroid_cmp(&left_centroid, &right_centroid) != Ordering::Less {
            left_index -= 1;
            left[output_index] = left_centroid;
        } else {
            right_index -= 1;
            left[output_index] = right_centroid;
        }
    }
    if right_index > 0 {
        left[..right_index].copy_from_slice(&right[..right_index]);
    }
}

fn centroid_lower_bound(c: &Centroid, value: f64) -> Ordering {
    if c.mean < value {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

fn centroid_upper_bound(c: &Centroid, value: f64) -> Ordering {
    if c.mean > value {
        Ordering::Greater
    } else {
        Ordering::Less
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Centroid {
    mean: f64,
    weight: NonZeroU64,
}

impl Centroid {
    fn add(&mut self, other: Centroid) {
        let (self_weight, other_weight) = (self.weight(), other.weight());
        let total_weight = self_weight + other_weight;
        self.weight = self
            .weight
            .checked_add(other.weight.get())
            .expect("weight overflow");

        let (self_mean, other_mean) = (self.mean, other.mean);
        let ratio_other = other_weight / total_weight;
        let delta = other_mean - self_mean;
        self.mean = if delta.is_finite() {
            delta.mul_add(ratio_other, self_mean)
        } else {
            let ratio_self = self_weight / total_weight;
            self_mean.mul_add(ratio_self, other_mean * ratio_other)
        };

        debug_assert!(
            self.mean.is_finite(),
            "Centroid's mean must be finite; self: {}, other: {}",
            self_mean,
            other_mean
        );
    }

    fn weight(&self) -> f64 {
        self.weight.get() as f64
    }
}

fn check_non_nan(value: f64, tag: &'static str) -> Result<(), Error> {
    if value.is_nan() {
        return Err(Error::deserial(format!(
            "malformed data: {tag} cannot be NaN"
        )));
    }

    Ok(())
}

fn check_finite(value: f64, tag: &'static str) -> Result<(), Error> {
    if value.is_infinite() {
        return Err(Error::deserial(format!(
            "malformed data: {tag} cannot be infinite"
        )));
    }

    Ok(())
}

fn check_nonzero(value: u64, tag: &'static str) -> Result<NonZeroU64, Error> {
    NonZeroU64::new(value)
        .ok_or_else(|| Error::deserial(format!("malformed data: {tag} cannot be zero")))
}

fn check_compat_weight(value: f64, tag: &'static str) -> Result<NonZeroU64, Error> {
    check_non_nan(value, tag)?;
    check_finite(value, tag)?;
    if !(1.0..u64::MAX as f64).contains(&value) {
        return Err(Error::deserial(format!(
            "malformed data: {tag} must be representable as a positive u64"
        )));
    }
    if value.trunc() != value {
        return Err(Error::deserial(format!(
            "malformed data: {tag} must not have a fractional part"
        )));
    }
    check_nonzero(value as u64, tag)
}

fn checked_weight_sum(total_weight: u64, weight: u64) -> Result<u64, Error> {
    total_weight
        .checked_add(weight)
        .ok_or_else(|| Error::deserial("malformed data: total weight overflow"))
}

/// Generates cluster sizes proportional to `q*(1-q)`.
///
/// The use of a normalizing function results in a strictly bounded number of clusters no matter
/// how many samples.
///
/// Corresponds to K_2 in the reference implementation
mod scale_function {
    pub fn max(q: f64, normalizer: f64) -> f64 {
        q * (1. - q) / normalizer
    }

    pub fn normalizer(compression: f64, n: f64) -> f64 {
        compression / z(compression, n)
    }

    pub fn z(compression: f64, n: f64) -> f64 {
        4. * (n / compression).ln() + 24.
    }
}

fn weighted_average(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {
    let total_weight = w1 + w2;
    let ratio = w2 / total_weight;
    if x1.is_sign_positive() != x2.is_sign_positive() {
        // Subtracting opposite-signed finite extremes can overflow.
        x1 * (1. - ratio) + x2 * ratio
    } else {
        // Same-sign subtraction is finite and avoids summing two near-maximum terms.
        (x2 - x1).mul_add(ratio, x1)
    }
}
