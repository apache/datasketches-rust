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

// The update buffer has two physical representations:
//
// * `Staging` stores raw `f64` values compactly before the first compression.
// * `Centroids` stores `[compressed prefix | unmerged unit-weight tail]` in one allocation. The
//   tail length identifies the boundary between the two regions.
//
// Compression permanently transitions a non-empty buffer from `Staging` to `Centroids`.
#[derive(Debug, Clone)]
enum TDigestBuffer {
    Staging(Vec<f64>),
    Centroids {
        centroids: Vec<Centroid>,
        unmerged_tail_len: usize,
    },
}

impl Default for TDigestBuffer {
    fn default() -> Self {
        TDigestBuffer::Staging(vec![])
    }
}

impl TDigestBuffer {
    fn len(&self) -> usize {
        match self {
            TDigestBuffer::Staging(values) => values.len(),
            TDigestBuffer::Centroids { centroids, .. } => centroids.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn unmerged_len(&self) -> usize {
        match self {
            TDigestBuffer::Staging(values) => values.len(),
            TDigestBuffer::Centroids {
                unmerged_tail_len, ..
            } => *unmerged_tail_len,
        }
    }

    /// Returns compressed centroids after the caller has processed staged values.
    fn compressed_centroids(&self) -> &[Centroid] {
        match self {
            TDigestBuffer::Staging(values) if values.is_empty() => &[],
            TDigestBuffer::Centroids {
                centroids,
                unmerged_tail_len: 0,
            } => centroids,
            _ => unreachable!(
                "t-digest buffer must be compressed before reading centroids: {self:?}"
            ),
        }
    }

    fn into_compressed_centroids(self) -> Vec<Centroid> {
        match self {
            TDigestBuffer::Staging(values) if values.is_empty() => vec![],
            TDigestBuffer::Centroids {
                centroids,
                unmerged_tail_len: 0,
            } => centroids,
            _ => unreachable!(
                "t-digest buffer must be compressed before reading centroids: {self:?}"
            ),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            TDigestBuffer::Staging(values) => values.capacity() * size_of::<f64>(),
            TDigestBuffer::Centroids { centroids, .. } => {
                centroids.capacity() * size_of::<Centroid>()
            }
        }
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
    // Weight represented by the compressed prefix. Staged values or the unmerged tail contribute
    // one each and are counted separately by `TDigestBuffer::unmerged_len`.
    compressed_weight: u64,
}

impl Default for TDigestMut {
    fn default() -> Self {
        TDigestMut::new(DEFAULT_K)
    }
}

impl TDigestMut {
    /// Creates a mutable t-digest with the given `k` value.
    ///
    /// The fallible version of this method is [`TDigestMut::try_new`].
    ///
    /// # Panics
    ///
    /// Panics if `k` is less than `10`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let sketch = TDigestMut::new(100);
    /// assert_eq!(sketch.k(), 100);
    /// ```
    pub fn new(k: u16) -> Self {
        Self::make(
            k,
            false,
            f64::INFINITY,
            f64::NEG_INFINITY,
            TDigestBuffer::Staging(vec![]),
            0,
        )
    }

    /// Creates a mutable t-digest with the given `k` value.
    ///
    /// The panicking version of this method is [`TDigestMut::new`].
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
    /// let sketch = TDigestMut::try_new(20).unwrap();
    /// assert_eq!(sketch.k(), 20);
    /// ```
    pub fn try_new(k: u16) -> Result<Self, Error> {
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
            TDigestBuffer::Staging(vec![]),
            0,
        ))
    }

    // for deserialization
    fn make(
        k: u16,
        reverse_merge: bool,
        min: f64,
        max: f64,
        buffer: TDigestBuffer,
        compressed_weight: u64,
    ) -> Self {
        assert!(k >= 10, "k must be at least 10");
        debug_assert!(match &buffer {
            TDigestBuffer::Staging(_) => compressed_weight == 0,
            TDigestBuffer::Centroids {
                centroids,
                unmerged_tail_len,
            } => *unmerged_tail_len <= centroids.len(),
        });

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
    /// let mut sketch = TDigestMut::new(100);
    /// sketch.update(1.0);
    /// assert!(sketch.total_weight() >= 1);
    /// ```
    pub fn update(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }

        let max_unmerged = self.max_unmerged();
        if let TDigestBuffer::Staging(values) = &mut self.buffer {
            // Compress only at the exact threshold for compatibility with deserialized images
            // whose buffered section is already over the normal update-path limit.
            if values.len() != max_unmerged {
                if values.len() == values.capacity() {
                    let target_capacity = if values.capacity() == 0 {
                        INITIAL_UNMERGED_CAPACITY
                    } else if values.capacity() == INITIAL_UNMERGED_CAPACITY {
                        // Once a digest outgrows a tiny group, skip an extra allocator round trip
                        // while keeping the first allocation small.
                        (INITIAL_UNMERGED_CAPACITY * UNMERGED_MULTIPLIER * UNMERGED_MULTIPLIER)
                            .min(max_unmerged)
                    } else {
                        values
                            .capacity()
                            .saturating_mul(UNMERGED_MULTIPLIER)
                            .min(max_unmerged)
                    };
                    values.reserve_exact(target_capacity.saturating_sub(values.len()));
                }

                values.push(value);
                self.min = self.min.min(value);
                self.max = self.max.max(value);
                return;
            }
            self.compress();
        }

        if matches!(
            &self.buffer,
            TDigestBuffer::Centroids { unmerged_tail_len, .. } if *unmerged_tail_len == max_unmerged
        ) {
            // The same equality check preserves the lifecycle of accepted overfull mixed images.
            self.compress();
        }

        let TDigestBuffer::Centroids {
            centroids,
            unmerged_tail_len,
        } = &mut self.buffer
        else {
            unreachable!("a full staging buffer must become centroid-backed after compression");
        };
        if centroids.len() == centroids.capacity() {
            let target_unmerged = if *unmerged_tail_len == 0 {
                INITIAL_UNMERGED_CAPACITY
            } else if *unmerged_tail_len == INITIAL_UNMERGED_CAPACITY {
                // Once a digest outgrows a tiny group, skip an extra allocator round trip while
                // keeping the first allocation small.
                (INITIAL_UNMERGED_CAPACITY * UNMERGED_MULTIPLIER * UNMERGED_MULTIPLIER)
                    .min(max_unmerged)
            } else {
                unmerged_tail_len
                    .saturating_mul(UNMERGED_MULTIPLIER)
                    .min(max_unmerged)
            };
            let num_merged = centroids.len() - *unmerged_tail_len;
            let target_capacity = num_merged.saturating_add(target_unmerged);
            centroids.reserve_exact(target_capacity.saturating_sub(centroids.len()));
        }

        centroids.push(Centroid {
            mean: value,
            weight: DEFAULT_WEIGHT,
        });
        *unmerged_tail_len += 1;
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
    /// let mut left = TDigestMut::new(100);
    /// let mut right = TDigestMut::new(100);
    /// left.update(1.0);
    /// right.update(2.0);
    /// left.merge(&right);
    /// assert_eq!(left.total_weight(), 2);
    /// ```
    pub fn merge(&mut self, other: &TDigestMut) {
        if other.is_empty() {
            return;
        }

        let buffer = std::mem::take(&mut self.buffer);
        let (mut merge_buffer, existing_prefix_len, self_unmerged_weight) = match buffer {
            TDigestBuffer::Staging(values) => {
                let self_unmerged_weight = values.len() as u64;
                let mut merge_buffer = Vec::with_capacity(values.len() + other.buffer.len());
                merge_buffer.extend(values.into_iter().map(|mean| Centroid {
                    mean,
                    weight: DEFAULT_WEIGHT,
                }));
                (merge_buffer, 0, self_unmerged_weight)
            }
            TDigestBuffer::Centroids {
                mut centroids,
                unmerged_tail_len,
            } => {
                let existing_prefix_len = centroids.len() - unmerged_tail_len;
                centroids.reserve(other.buffer.len());
                (centroids, existing_prefix_len, unmerged_tail_len as u64)
            }
        };
        match &other.buffer {
            TDigestBuffer::Staging(values) => {
                merge_buffer.extend(values.iter().copied().map(|mean| Centroid {
                    mean,
                    weight: DEFAULT_WEIGHT,
                }));
            }
            TDigestBuffer::Centroids {
                centroids,
                unmerged_tail_len,
            } => {
                let other_prefix_len = centroids.len() - unmerged_tail_len;
                merge_buffer.extend_from_slice(&centroids[other_prefix_len..]);
                merge_buffer.extend_from_slice(&centroids[..other_prefix_len]);
            }
        }
        // Preserve the original insertion order for equal means because t-digest requires a stable
        // sort: this digest's buffered/unmerged values, the other digest's buffered/unmerged values
        // and compressed centroids, then this digest's compressed centroids.
        merge_buffer.rotate_left(existing_prefix_len);
        self.compress_centroids(merge_buffer, self_unmerged_weight + other.total_weight())
    }

    /// Converts this mutable t-digest into an immutable one.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
    /// sketch.update(1.0);
    /// let bytes = sketch.serialize();
    /// let decoded = TDigestMut::deserialize(&bytes, false).unwrap();
    /// assert_eq!(decoded.max_value(), Some(1.0));
    /// ```
    pub fn serialize(&mut self) -> Vec<u8> {
        self.compress();
        let centroids = self.buffer.compressed_centroids();

        let mut total_size = 0;
        if self.is_empty() || self.is_single_value() {
            // 1 byte preamble
            // + 1 byte serial version
            // + 1 byte family
            // + 2 bytes k
            // + 1 byte flags
            // + 2 bytes unused
            total_size += size_of::<u64>();
        } else {
            // all of the above
            // + 4 bytes num centroids
            // + 4 bytes num buffered
            total_size += size_of::<u64>() * 2;
        }
        if self.is_empty() {
            // nothing more
        } else if self.is_single_value() {
            // + 8 bytes single value
            total_size += size_of::<f64>();
        } else {
            // + 8 bytes min
            // + 8 bytes max
            total_size += size_of::<f64>() * 2;
            // + (8+8) bytes per centroid
            total_size += centroids.len() * (size_of::<f64>() + size_of::<u64>());
        }

        let mut bytes = SketchBytes::with_capacity(total_size);
        bytes.write_u8(match self.total_weight() {
            0 => PREAMBLE_LONGS_EMPTY_OR_SINGLE,
            1 => PREAMBLE_LONGS_EMPTY_OR_SINGLE,
            _ => PREAMBLE_LONGS_MULTIPLE,
        });
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::TDIGEST.id);
        bytes.write_u16_le(self.k);
        bytes.write_u8({
            let mut flags = 0;
            if self.is_empty() {
                flags |= FLAGS_IS_EMPTY;
            }
            if self.is_single_value() {
                flags |= FLAGS_IS_SINGLE_VALUE;
            }
            if self.reverse_merge {
                flags |= FLAGS_REVERSE_MERGE;
            }
            flags
        });
        bytes.write_u16_le(0); // unused
        if self.is_empty() {
            return bytes.into_bytes();
        }
        if self.is_single_value() {
            bytes.write_f64_le(self.min);
            return bytes.into_bytes();
        }
        bytes.write_u32_le(centroids.len() as u32);
        bytes.write_u32_le(0); // unused
        bytes.write_f64_le(self.min);
        bytes.write_f64_le(self.max);
        for centroid in centroids {
            bytes.write_f64_le(centroid.mean);
            bytes.write_u64_le(centroid.weight.get());
        }
        bytes.into_bytes()
    }

    /// Deserializes a mutable t-digest from bytes.
    ///
    /// Supports reading compact format with (float, int) centroids as opposed to (double, long) to
    /// represent (mean, weight). [^1]
    ///
    /// Supports reading format of the reference implementation (auto-detected) [^2].
    ///
    /// [^1]: This is to support reading the `tdigest<float>` format from the C++ implementation.
    /// [^2]: <https://github.com/tdunning/t-digest>
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tdigest::TDigestMut;
    ///
    /// let mut sketch = TDigestMut::new(100);
    /// sketch.update(1.0);
    /// sketch.update(2.0);
    /// let bytes = sketch.serialize();
    /// let decoded = TDigestMut::deserialize(&bytes, false).unwrap();
    /// assert_eq!(decoded.max_value(), Some(2.0));
    /// ```
    pub fn deserialize(bytes: &[u8], is_f32: bool) -> Result<Self, Error> {
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
            return Ok(TDigestMut::new(k));
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
                TDigestBuffer::Centroids {
                    centroids: vec![Centroid {
                        mean: value,
                        weight: DEFAULT_WEIGHT,
                    }],
                    unmerged_tail_len: 0,
                },
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
        let required_payload_bytes = num_centroids
            .checked_mul(centroid_bytes)
            .and_then(|bytes| {
                num_buffered
                    .checked_mul(buffered_value_bytes)
                    .and_then(|buffered_bytes| bytes.checked_add(buffered_bytes))
            })
            .ok_or_else(|| Error::deserial("TDigest payload size exceeds the supported size"))?;
        if cursor.remaining().len() < required_payload_bytes {
            return Err(Error::insufficient_data(format!(
                "TDigest payload requires {required_payload_bytes} bytes, got {}",
                cursor.remaining().len()
            )));
        }
        if num_centroids == 0 {
            checked_weight_sum(0, num_buffered as u64)?;
            let mut initial_buffer = Vec::with_capacity(num_buffered);
            for _ in 0..num_buffered {
                let value = if is_f32 {
                    cursor
                        .read_f32_le()
                        .map_err(insufficient_data("buffered_value"))? as f64
                } else {
                    cursor
                        .read_f64_le()
                        .map_err(insufficient_data("buffered_value"))?
                };
                check_non_nan(value, "buffered_value mean")?;
                check_finite(value, "buffered_value mean")?;
                initial_buffer.push(value);
            }
            return Ok(TDigestMut::make(
                k,
                reverse_merge,
                min,
                max,
                TDigestBuffer::Staging(initial_buffer),
                0,
            ));
        }
        let stored_centroids = num_centroids.checked_add(num_buffered).ok_or_else(|| {
            Error::deserial("num_centroids and num_buffered exceed the supported size")
        })?;
        let mut centroids = Vec::with_capacity(stored_centroids);
        let mut compressed_weight = 0u64;
        for _ in 0..num_centroids {
            let (mean, weight) = if is_f32 {
                (
                    cursor.read_f32_le().map_err(insufficient_data("mean"))? as f64,
                    cursor.read_u32_le().map_err(insufficient_data("weight"))? as u64,
                )
            } else {
                (
                    cursor.read_f64_le().map_err(insufficient_data("mean"))?,
                    cursor.read_u64_le().map_err(insufficient_data("weight"))?,
                )
            };
            check_non_nan(mean, "centroid mean")?;
            check_finite(mean, "centroid")?;
            let weight = check_nonzero(weight, "centroid weight")?;
            compressed_weight = checked_weight_sum(compressed_weight, weight.get())?;
            centroids.push(Centroid { mean, weight });
        }
        checked_weight_sum(compressed_weight, num_buffered as u64)?;
        for _ in 0..num_buffered {
            let value = if is_f32 {
                cursor
                    .read_f32_le()
                    .map_err(insufficient_data("buffered_value"))? as f64
            } else {
                cursor
                    .read_f64_le()
                    .map_err(insufficient_data("buffered_value"))?
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
            TDigestBuffer::Centroids {
                centroids,
                unmerged_tail_len: num_buffered,
            },
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
                    TDigestBuffer::Centroids {
                        centroids,
                        unmerged_tail_len: 0,
                    },
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
                    TDigestBuffer::Centroids {
                        centroids,
                        unmerged_tail_len: 0,
                    },
                    total_weight,
                ))
            }
            ty => Err(Error::deserial(format!("unknown TDigest compat type {ty}"))),
        }
    }

    fn is_single_value(&self) -> bool {
        self.total_weight() == 1
    }

    /// Processes unmerged values and merges centroids if needed.
    fn compress(&mut self) {
        let buffer = std::mem::take(&mut self.buffer);
        match buffer {
            TDigestBuffer::Staging(values) if values.is_empty() => {
                self.buffer = TDigestBuffer::Staging(values);
            }
            TDigestBuffer::Staging(values) => {
                debug_assert_eq!(self.compressed_weight, 0);
                let weight = values.len() as u64;
                let mut centroids = Vec::with_capacity(values.len());
                centroids.extend(values.into_iter().map(|mean| Centroid {
                    mean,
                    weight: DEFAULT_WEIGHT,
                }));
                self.compress_centroids(centroids, weight);
            }
            TDigestBuffer::Centroids {
                centroids,
                unmerged_tail_len: 0,
            } => {
                // Preserve compact deserialized images verbatim, including images with more
                // centroids than this implementation would normally produce.
                self.buffer = TDigestBuffer::Centroids {
                    centroids,
                    unmerged_tail_len: 0,
                };
            }
            TDigestBuffer::Centroids {
                mut centroids,
                unmerged_tail_len,
            } => {
                let compressed_prefix_len = centroids.len() - unmerged_tail_len;
                // Preserve the original insertion order for equal means because t-digest requires
                // a stable sort: unmerged values before existing centroids.
                centroids.rotate_left(compressed_prefix_len);
                self.compress_centroids(centroids, unmerged_tail_len as u64);
            }
        }
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
    /// * `self.buffer` becomes centroid-backed with no unmerged values before returning.
    fn compress_centroids(&mut self, mut centroids: Vec<Centroid>, additional_weight: u64) {
        debug_assert!(!centroids.is_empty());
        centroids.sort_by(centroid_cmp);
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
        self.buffer = TDigestBuffer::Centroids {
            centroids,
            unmerged_tail_len: 0,
        };
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
    /// is always 1.
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
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
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
    /// let mut sketch = TDigestMut::new(100);
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
            TDigestBuffer::Centroids {
                centroids: self.centroids,
                unmerged_tail_len: 0,
            },
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
                    1. + (((value - self.min) / (first_mean - self.min))
                        * ((self.centroids[0].weight() / 2.) - 1.))
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
            return Some(
                self.max
                    + (((centroids_weight - weight - 1.) / ((last_weight / 2.) - 1.))
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
                let w1 = weight - weight_so_far - left_weight;
                let w2 = weight_so_far + dw - weight - right_weight;
                return Some(weighted_average(
                    self.centroids[i].mean,
                    w1,
                    self.centroids[i + 1].mean,
                    w2,
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
    let len = split_points.len();
    if len == 1 && split_points[0].is_nan() {
        panic!("split_points must not contain NaN values: {split_points:?}");
    }
    for i in 0..len - 1 {
        if split_points[i] < split_points[i + 1] {
            // we must use this positive condition because NaN comparisons are always false
            continue;
        }
        panic!("split_points must be unique and monotonically increasing: {split_points:?}");
    }
}

fn centroid_cmp(a: &Centroid, b: &Centroid) -> Ordering {
    match a.mean.partial_cmp(&b.mean) {
        Some(order) => order,
        None => unreachable!("NaN values should never be present in centroids"),
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
    pub(super) fn max(q: f64, normalizer: f64) -> f64 {
        q * (1. - q) / normalizer
    }

    pub(super) fn normalizer(compression: f64, n: f64) -> f64 {
        compression / z(compression, n)
    }

    pub(super) fn z(compression: f64, n: f64) -> f64 {
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
