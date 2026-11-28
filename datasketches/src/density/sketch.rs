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

use crate::common::random::random_below;
use crate::common::random::random_bit;
use crate::density::MAX_LEVELS;
use crate::density::MIN_K;
use crate::density::iter::DensitySketchIter;
use crate::density::kernel::GaussianKernel;
use crate::density::kernel::Kernel;
use crate::density::serialization::deserialize_sketch;
use crate::density::serialization::serialize_sketch;
use crate::density::value::DensityValue;
use crate::error::Error;

#[derive(Clone)]
pub struct Level<T> {
    pub coords: Vec<T>,
}

impl<T> Default for Level<T> {
    fn default() -> Self {
        Self { coords: Vec::new() }
    }
}

/// Streaming kernel density estimation sketch over `dim`-dimensional vectors.
///
/// See the [density module documentation](crate::density) for background and usage.
#[derive(Clone)]
pub struct DensitySketch<T, K = GaussianKernel> {
    k: u16,
    dim: u32,
    kernel: K,
    num_retained: usize,
    n: u64,
    levels: Vec<Level<T>>,
}

impl<T, K: fmt::Debug> fmt::Debug for DensitySketch<T, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let is_estimation_mode = self.levels.len() > 1
            || u64::try_from(self.num_retained).is_ok_and(|retained| retained < self.n);
        f.debug_struct("DensitySketch")
            .field("k", &self.k)
            .field("dim", &self.dim)
            .field("total_weight", &self.n)
            .field("is_empty", &(self.n == 0))
            .field("is_estimation_mode", &is_estimation_mode)
            .field("num_levels", &self.levels.len())
            .field("num_retained", &self.num_retained)
            .field("kernel", &self.kernel)
            .finish()
    }
}

impl<T: DensityValue> DensitySketch<T, GaussianKernel> {
    /// Creates an empty sketch using [`GaussianKernel`].
    ///
    /// Larger `k` retains more coreset points and lowers estimation error.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `k < 2`,
    /// `dim == 0`, or `dim` cannot be represented as `usize`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::density::DensitySketch;
    ///
    /// let sketch = DensitySketch::<f64>::new(16, 3)?;
    /// assert_eq!(sketch.k(), 16);
    /// assert_eq!(sketch.dim(), 3);
    /// assert!(sketch.is_empty());
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn new(k: u16, dim: u32) -> Result<Self, Error> {
        Self::make_with_kernel(k, dim, GaussianKernel)
    }

    /// Deserializes a sketch from bytes using [`GaussianKernel`].
    ///
    /// Also accepts data serialized by other DataSketches implementations when both sides use the
    /// same coordinate type and [`GaussianKernel`]. Use [`Self::deserialize_with_kernel`] for
    /// sketches built with a custom [`Kernel`]. See the
    /// [module documentation](crate::density#cross-language-compatibility) for compatibility notes.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidData`](crate::error::ErrorKind::InvalidData) if the image is truncated,
    /// malformed, or contains non-finite coordinates.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::density::DensitySketch;
    ///
    /// let mut sketch = DensitySketch::<f64>::new(8, 2)?;
    /// sketch.update(&[1.0, 2.0])?;
    /// let bytes = sketch.serialize();
    /// let decoded = DensitySketch::<f64>::deserialize(&bytes)?;
    /// assert_eq!(decoded.total_weight(), 1);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        deserialize_sketch(bytes, GaussianKernel)
    }
}

impl<T: DensityValue, K> DensitySketch<T, K> {
    fn make_with_kernel(k: u16, dim: u32, kernel: K) -> Result<Self, Error> {
        if k < MIN_K {
            return Err(Error::invalid_argument(format!(
                "k must be at least {MIN_K}, got {k}"
            )));
        }
        if dim == 0 {
            return Err(Error::invalid_argument(
                "dimension must be at least 1, got 0",
            ));
        }
        if usize::try_from(dim).is_err() {
            return Err(Error::invalid_argument(format!(
                "dimension {dim} exceeds platform usize::MAX"
            )));
        }
        Ok(Self {
            k,
            dim,
            kernel,
            num_retained: 0,
            n: 0,
            levels: vec![Level::default()],
        })
    }

    /// Returns the configured `k` parameter.
    pub fn k(&self) -> u16 {
        self.k
    }

    /// Returns the configured point dimension.
    pub fn dim(&self) -> u32 {
        self.dim
    }

    /// Returns a reference to the configured kernel.
    pub fn kernel(&self) -> &K {
        &self.kernel
    }

    /// Returns the total stream weight.
    pub fn total_weight(&self) -> u64 {
        self.n
    }

    /// Returns `true` if the sketch has observed no points.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Returns `true` if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        // Compaction normally creates higher levels (`levels.len() > 1`), while `datasketches-cpp`
        // round-trips can strip trailing empty levels and leave a single compacted level with
        // `num_retained < n`.
        self.levels.len() > 1
            || u64::try_from(self.num_retained).is_ok_and(|retained| retained < self.n)
    }

    /// Returns the number of retained points.
    pub fn num_retained(&self) -> usize {
        self.num_retained
    }

    /// Returns an estimate of the sketch's memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.levels.capacity() * size_of::<Level<T>>()
            + self
                .levels
                .iter()
                .map(|level| level.coords.capacity() * size_of::<T>())
                .sum::<usize>()
    }

    /// Resets the sketch to its empty state while retaining `k`, `dim`, and `kernel`.
    pub fn reset(&mut self) {
        self.n = 0;
        self.num_retained = 0;
        self.levels.truncate(1);
        self.levels[0].coords.clear();
    }

    /// Returns an iterator over retained `(point, weight)` pairs.
    pub fn iter(&self) -> DensitySketchIter<'_, T> {
        DensitySketchIter::new(&self.levels, self.dim as usize, self.num_retained)
    }

    pub(super) fn levels(&self) -> &[Level<T>] {
        &self.levels
    }

    pub(super) fn from_parts(
        k: u16,
        dim: u32,
        kernel: K,
        num_retained: usize,
        n: u64,
        levels: Vec<Level<T>>,
    ) -> Self {
        Self {
            k,
            dim,
            kernel,
            num_retained,
            n,
            levels,
        }
    }

    fn validate_point(&self, point: &[T]) -> Result<(), Error> {
        if point.len() != self.dim as usize {
            return Err(Error::invalid_argument(format!(
                "point dimension mismatch: expected {}, got {}",
                self.dim,
                point.len()
            )));
        }
        if !point.iter().all(|coord| coord.is_finite_val()) {
            return Err(Error::invalid_argument("point coordinates must be finite"));
        }
        Ok(())
    }
}

impl<'a, T: DensityValue, K> IntoIterator for &'a DensitySketch<T, K> {
    type Item = (&'a [T], u64);
    type IntoIter = DensitySketchIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: DensityValue, K: Kernel<T>> DensitySketch<T, K> {
    /// Creates an empty sketch using a custom `kernel`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `k < 2`,
    /// `dim == 0`, or `dim` cannot be represented as `usize`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::density::DensitySketch;
    /// use datasketches::density::GaussianKernel;
    ///
    /// let sketch = DensitySketch::<f64, _>::with_kernel(8, 2, GaussianKernel)?;
    /// assert_eq!(sketch.k(), 8);
    /// assert_eq!(sketch.dim(), 2);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn with_kernel(k: u16, dim: u32, kernel: K) -> Result<Self, Error> {
        Self::make_with_kernel(k, dim, kernel)
    }

    /// Updates the sketch with a `dim`-dimensional point.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) without modifying
    /// the sketch if `point.len() != self.dim() as usize` or any coordinate is non-finite.
    ///
    /// # Panics
    ///
    /// Panics without modifying the sketch if the total stream weight would exceed [`u64::MAX`], or
    /// if compaction reaches the maximum supported level count (`64`).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::density::DensitySketch;
    ///
    /// let mut sketch = DensitySketch::<f64>::new(8, 2)?;
    /// sketch.update(&[0.5, -0.5])?;
    /// assert_eq!(sketch.total_weight(), 1);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn update(&mut self, point: &[T]) -> Result<(), Error> {
        self.validate_point(point)?;
        assert!(self.n < u64::MAX, "total stream weight overflow");

        while self.num_retained >= usize::from(self.k) * self.levels.len() {
            self.compact();
        }
        self.levels[0].coords.extend_from_slice(point);
        self.num_retained += 1;
        self.n += 1;
        Ok(())
    }

    /// Merges another sketch into `self`.
    ///
    /// Retains `self.k()`, even if `other` uses a different value.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) without modifying
    /// `self` if `self.dim() != other.dim()`, if `self.kernel() != other.kernel()`, if the
    /// combined stream weight would exceed [`u64::MAX`], or if the combined retained count would
    /// exceed [`usize::MAX`].
    ///
    /// # Panics
    ///
    /// Panics if post-merge compaction reaches the maximum supported level count (`64`).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::density::DensitySketch;
    ///
    /// let mut left = DensitySketch::<f64>::new(8, 2)?;
    /// left.update(&[0.0, 0.0])?;
    /// let mut right = DensitySketch::<f64>::new(8, 2)?;
    /// right.update(&[1.0, 1.0])?;
    ///
    /// left.merge(&right)?;
    /// assert_eq!(left.total_weight(), 2);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn merge(&mut self, other: &Self) -> Result<(), Error> {
        if self.dim != other.dim {
            return Err(Error::invalid_argument(format!(
                "cannot merge density sketches with different dimensions: expected {}, got {}",
                self.dim, other.dim
            )));
        }
        if self.kernel != other.kernel {
            return Err(Error::invalid_argument(
                "cannot merge density sketches with different kernels",
            ));
        }
        if other.is_empty() {
            return Ok(());
        }

        let new_n = self.n.checked_add(other.n).ok_or_else(|| {
            Error::invalid_argument(format!(
                "combined stream weight exceeds {}: left {}, right {}",
                u64::MAX,
                self.n,
                other.n
            ))
        })?;
        let new_retained = self
            .num_retained
            .checked_add(other.num_retained)
            .ok_or_else(|| {
                Error::invalid_argument(format!(
                    "combined retained count exceeds {}: left {}, right {}",
                    usize::MAX,
                    self.num_retained,
                    other.num_retained
                ))
            })?;

        if self.levels.len() < other.levels.len() {
            self.levels.resize_with(other.levels.len(), Level::default);
        }
        for (dst, src) in self.levels.iter_mut().zip(&other.levels) {
            dst.coords.extend_from_slice(&src.coords);
        }
        self.n = new_n;
        self.num_retained = new_retained;

        while self.num_retained >= usize::from(self.k) * self.levels.len() {
            self.compact();
        }
        Ok(())
    }

    /// Estimates the normalized kernel density at `point`.
    ///
    /// Returns `Ok(None)` if the sketch is empty.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// `point.len() != self.dim() as usize` or any coordinate is non-finite, even if the sketch is
    /// empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::density::DensitySketch;
    ///
    /// let mut sketch = DensitySketch::<f64>::new(8, 2)?;
    /// assert_eq!(sketch.estimate(&[0.0, 0.0])?, None);
    /// sketch.update(&[0.0, 0.0])?;
    /// assert_eq!(sketch.estimate(&[0.0, 0.0])?, Some(1.0));
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn estimate(&self, point: &[T]) -> Result<Option<f64>, Error> {
        self.validate_point(point)?;
        if self.is_empty() {
            return Ok(None);
        }

        let dim = self.dim as usize;
        let mut weighted_sum = 0.0_f64;
        for (height, level) in self.levels.iter().enumerate() {
            if level.coords.is_empty() {
                continue;
            }
            let weight = (1_u64 << height) as f64;
            let level_sum: f64 = level
                .coords
                .chunks_exact(dim)
                .map(|retained| self.kernel.evaluate(retained, point))
                .sum();
            weighted_sum += weight * level_sum;
        }
        Ok(Some(weighted_sum / (self.n as f64)))
    }

    /// Serializes this sketch to bytes.
    ///
    /// The serialized data can be read by other DataSketches implementations when both sides use
    /// the same coordinate type and kernel. Kernel configuration is not stored in the image.
    pub fn serialize(&self) -> Vec<u8> {
        serialize_sketch(self)
    }

    /// Deserializes a sketch from bytes using `kernel`.
    ///
    /// Also accepts data serialized by other DataSketches implementations when both sides use the
    /// same coordinate type and an equivalent `kernel`. See the
    /// [module documentation](crate::density#cross-language-compatibility) for compatibility notes.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidData`](crate::error::ErrorKind::InvalidData) if the image is truncated,
    /// malformed, or contains non-finite coordinates.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::density::DensitySketch;
    /// use datasketches::density::GaussianKernel;
    ///
    /// let mut sketch = DensitySketch::<f64, _>::with_kernel(8, 2, GaussianKernel)?;
    /// sketch.update(&[0.0, 1.0])?;
    /// let bytes = sketch.serialize();
    /// let decoded = DensitySketch::<f64, _>::deserialize_with_kernel(&bytes, GaussianKernel)?;
    /// assert_eq!(decoded.total_weight(), 1);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn deserialize_with_kernel(bytes: &[u8], kernel: K) -> Result<Self, Error> {
        deserialize_sketch(bytes, kernel)
    }

    fn compact(&mut self) {
        let dim = self.dim as usize;
        let Some(height) = self
            .levels
            .iter()
            .position(|level| level.coords.len() / dim >= usize::from(self.k))
        else {
            unreachable!(
                "compaction invariant guarantees at least one level has at least k points"
            );
        };
        assert!(
            height + 1 < MAX_LEVELS,
            "density sketch exceeded {MAX_LEVELS} levels"
        );
        if height + 1 >= self.levels.len() {
            self.levels.push(Level::default());
        }
        self.compact_level(height);
    }

    fn compact_level(&mut self, height: usize) {
        let dim = self.dim as usize;
        let level_coords = &mut self.levels[height].coords;
        let m = level_coords.len() / dim;
        debug_assert!(m >= usize::from(self.k));

        for i in (1..m).rev() {
            let j = random_below(i + 1);
            if i != j {
                let (left, right) = level_coords.split_at_mut(i * dim);
                left[j * dim..j * dim + dim].swap_with_slice(&mut right[..dim]);
            }
        }

        let mut keep = vec![false; m];
        keep[0] = random_bit();
        for i in 1..m {
            let pi = &level_coords[i * dim..(i + 1) * dim];
            let mut delta = 0.0_f64;
            for j in 0..i {
                let pj = &level_coords[j * dim..(j + 1) * dim];
                let sign = if keep[j] { 1.0_f64 } else { -1.0_f64 };
                delta += sign * self.kernel.evaluate(pi, pj);
            }
            keep[i] = if delta == 0.0 {
                random_bit()
            } else {
                delta < 0.0
            };
        }
        // Keep compaction strictly reducing (1 <= kept <= m - 1) and unbiased when kernel
        // evaluations vanish (e.g., compact-support kernels or far-apart outliers where
        // exp(-d^2) underflows to 0.0). Because `level_coords` was shuffled above, flipping
        // index 0 preserves E[kept] = m / 2.
        if !keep.iter().any(|&b| b) {
            keep[0] = true;
        } else if keep.iter().all(|&b| b) {
            keep[0] = false;
        }

        let (head, tail) = self.levels.split_at_mut(height + 1);
        let level = &mut head[height];
        let next_level = &mut tail[0];

        let mut kept = 0usize;
        for (idx, chunk) in level.coords.chunks_exact(dim).enumerate() {
            if keep[idx] {
                next_level.coords.extend_from_slice(chunk);
                kept += 1;
            }
        }
        self.num_retained -= m - kept;
        level.coords.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "density sketch exceeded 64 levels")]
    fn compact_panics_when_levels_exceed_max_levels() {
        let mut levels = vec![Level::default(); MAX_LEVELS];
        levels[MAX_LEVELS - 1] = Level {
            coords: vec![1.0, 2.0],
        };
        let mut sketch = DensitySketch::from_parts(2, 1, GaussianKernel, 2, u64::MAX, levels);
        sketch.compact();
    }

    #[test]
    fn compact_is_strictly_reducing_and_unbiased_when_kernel_is_zero() {
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
        struct ZeroKernel;

        impl Kernel<f64> for ZeroKernel {
            fn evaluate(&self, _a: &[f64], _b: &[f64]) -> f64 {
                0.0
            }
        }

        // For m = 4 (k = 4), compact_level must always keep 1..=3 items with E[kept] = 2.0.
        const TRIALS: usize = 2_000;
        let mut total_kept = 0usize;
        for _ in 0..TRIALS {
            let levels = vec![Level {
                coords: vec![0.0, 1.0, 2.0, 3.0],
            }];
            let mut sketch = DensitySketch::from_parts(4, 1, ZeroKernel, 4, 4, levels);
            sketch.compact();
            let kept = sketch.levels[1].coords.len();
            assert!((1..=3).contains(&kept), "unexpected kept={kept}");
            assert_eq!(sketch.num_retained(), kept);
            total_kept += kept;
        }
        let mean_kept = total_kept as f64 / TRIALS as f64;
        assert!(
            (mean_kept - 2.0).abs() < 0.15,
            "mean kept {mean_kept} deviated from 2.0"
        );
    }
}
