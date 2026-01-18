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

use std::io::Write;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::common::RandomSource;
use crate::common::XorShift64;
use crate::density::serialization::DENSITY_FAMILY_ID;
use crate::density::serialization::FLAGS_IS_EMPTY;
use crate::density::serialization::PREAMBLE_INTS_LONG;
use crate::density::serialization::PREAMBLE_INTS_SHORT;
use crate::density::serialization::SERIAL_VERSION;
use crate::error::Error;
use crate::error::ErrorKind;

type SerializeValue<T> = fn(&mut SketchBytes, T);
type DeserializeValue<T> = fn(&mut SketchSlice<'_>) -> std::io::Result<T>;

/// Floating point types supported by the density sketch.
pub trait DensityValue: Copy + PartialOrd + 'static {
    /// Converts from f64.
    fn from_f64(value: f64) -> Self;
    /// Converts to f64 for accumulation.
    fn to_f64(self) -> f64;
}

impl DensityValue for f64 {
    fn from_f64(value: f64) -> Self {
        value
    }

    fn to_f64(self) -> f64 {
        self
    }
}

impl DensityValue for f32 {
    fn from_f64(value: f64) -> Self {
        value as f32
    }

    fn to_f64(self) -> f64 {
        self as f64
    }
}

/// Kernel used to compute density contributions between points.
pub trait DensityKernel {
    /// Returns the kernel evaluation for the two points.
    fn evaluate<T: DensityValue>(&self, left: &[T], right: &[T]) -> T;
}

/// Gaussian kernel based on squared Euclidean distance.
#[derive(Debug, Default, Clone, Copy)]
pub struct GaussianKernel;

impl DensityKernel for GaussianKernel {
    fn evaluate<T: DensityValue>(&self, left: &[T], right: &[T]) -> T {
        let mut sum = 0.0f64;
        for (a, b) in left.iter().zip(right.iter()) {
            let diff = a.to_f64() - b.to_f64();
            sum += diff * diff;
        }
        T::from_f64((-sum).exp())
    }
}

/// Density sketch for streaming density estimation.
pub struct DensitySketch<
    T: DensityValue,
    K: DensityKernel = GaussianKernel,
    R: RandomSource = XorShift64,
> {
    kernel: K,
    rng: R,
    k: u16,
    dim: u32,
    num_retained: u32,
    n: u64,
    levels: Vec<Vec<Vec<T>>>,
}

impl<T: DensityValue> DensitySketch<T, GaussianKernel, XorShift64> {
    /// Creates a new sketch using the Gaussian kernel.
    ///
    /// # Panics
    ///
    /// Panics if `k` is less than 2.
    pub fn new(k: u16, dim: u32) -> Self {
        Self::with_kernel(k, dim, GaussianKernel)
    }
}

impl DensitySketch<f32, GaussianKernel, XorShift64> {
    /// Deserializes a sketch using the Gaussian kernel.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_kernel(bytes, GaussianKernel)
    }
}

impl DensitySketch<f64, GaussianKernel, XorShift64> {
    /// Deserializes a sketch using the Gaussian kernel.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_kernel(bytes, GaussianKernel)
    }
}

impl<K: DensityKernel> DensitySketch<f32, K, XorShift64> {
    /// Deserializes a sketch using the provided kernel.
    pub fn deserialize_with_kernel(bytes: &[u8], kernel: K) -> Result<Self, Error> {
        Self::deserialize_with_kernel_and_rng(bytes, kernel, XorShift64::default())
    }
}

impl<K: DensityKernel> DensitySketch<f64, K, XorShift64> {
    /// Deserializes a sketch using the provided kernel.
    pub fn deserialize_with_kernel(bytes: &[u8], kernel: K) -> Result<Self, Error> {
        Self::deserialize_with_kernel_and_rng(bytes, kernel, XorShift64::default())
    }
}

impl<K: DensityKernel, R: RandomSource> DensitySketch<f32, K, R> {
    /// Deserializes a sketch using the provided kernel and random source.
    pub fn deserialize_with_kernel_and_rng(bytes: &[u8], kernel: K, rng: R) -> Result<Self, Error> {
        deserialize_inner(bytes, kernel, rng, read_f32_value)
    }

    /// Serializes the sketch to a byte vector.
    pub fn serialize(&self) -> Vec<u8> {
        serialize_inner(self, 4, write_f32_value)
    }

    /// Serializes the sketch to a writer.
    pub fn serialize_to_writer(&self, writer: &mut dyn Write) -> std::io::Result<()> {
        writer.write_all(&self.serialize())
    }
}

impl<K: DensityKernel, R: RandomSource> DensitySketch<f64, K, R> {
    /// Deserializes a sketch using the provided kernel and random source.
    pub fn deserialize_with_kernel_and_rng(bytes: &[u8], kernel: K, rng: R) -> Result<Self, Error> {
        deserialize_inner(bytes, kernel, rng, read_f64_value)
    }

    /// Serializes the sketch to a byte vector.
    pub fn serialize(&self) -> Vec<u8> {
        serialize_inner(self, 8, write_f64_value)
    }

    /// Serializes the sketch to a writer.
    pub fn serialize_to_writer(&self, writer: &mut dyn Write) -> std::io::Result<()> {
        writer.write_all(&self.serialize())
    }
}

impl<T: DensityValue, K: DensityKernel> DensitySketch<T, K, XorShift64> {
    /// Creates a new sketch with a custom kernel.
    ///
    /// # Panics
    ///
    /// Panics if `k` is less than 2.
    pub fn with_kernel(k: u16, dim: u32, kernel: K) -> Self {
        Self::with_kernel_and_rng(k, dim, kernel, XorShift64::default())
    }
}

impl<T: DensityValue, K: DensityKernel, R: RandomSource> DensitySketch<T, K, R> {
    /// Creates a new sketch with a custom kernel and random source.
    ///
    /// # Panics
    ///
    /// Panics if `k` is less than 2.
    pub fn with_kernel_and_rng(k: u16, dim: u32, kernel: K, rng: R) -> Self {
        assert!(k >= 2, "k must be > 1. Found: {k}");
        Self {
            kernel,
            rng,
            k,
            dim,
            num_retained: 0,
            n: 0,
            levels: vec![Vec::new()],
        }
    }

    /// Returns the configured parameter k.
    pub fn k(&self) -> u16 {
        self.k
    }

    /// Returns the configured dimension.
    pub fn dim(&self) -> u32 {
        self.dim
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_retained == 0
    }

    /// Returns the number of points observed by this sketch.
    pub fn n(&self) -> u64 {
        self.n
    }

    /// Returns the number of retained points.
    pub fn num_retained(&self) -> u32 {
        self.num_retained
    }

    /// Returns true if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        self.levels.len() > 1
    }

    /// Updates this sketch with a given point.
    ///
    /// # Panics
    ///
    /// Panics if the point dimension does not match this sketch.
    pub fn update(&mut self, point: Vec<T>) {
        if point.len() != self.dim as usize {
            panic!("dimension mismatch");
        }
        while self.num_retained >= self.k as u32 * self.levels.len() as u32 {
            self.compact();
        }
        self.levels[0].push(point);
        self.num_retained += 1;
        self.n += 1;
    }

    /// Updates this sketch with a slice, copying the point into the sketch.
    ///
    /// # Panics
    ///
    /// Panics if the point dimension does not match this sketch.
    pub fn update_slice(&mut self, point: &[T]) {
        self.update(point.to_vec());
    }

    /// Merges another sketch into this one.
    ///
    /// # Panics
    ///
    /// Panics if dimensions do not match.
    pub fn merge(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        if other.dim != self.dim {
            panic!("dimension mismatch");
        }
        while self.levels.len() < other.levels.len() {
            self.levels.push(Vec::new());
        }
        for (height, level) in other.levels.iter().enumerate() {
            self.levels[height].extend(level.iter().cloned());
        }
        self.num_retained += other.num_retained;
        self.n += other.n;
        while self.num_retained >= self.k as u32 * self.levels.len() as u32 {
            self.compact();
        }
    }

    /// Returns a density estimate at a given point.
    ///
    /// # Panics
    ///
    /// Panics if the sketch is empty.
    pub fn estimate(&self, point: &[T]) -> T {
        if self.is_empty() {
            panic!("operation is undefined for an empty sketch");
        }
        let n = self.n as f64;
        let mut density = 0.0f64;
        for (height, level) in self.levels.iter().enumerate() {
            let height_weight = weight_for_level(height) as f64;
            for p in level {
                density += height_weight * self.kernel.evaluate(p, point).to_f64() / n;
            }
        }
        T::from_f64(density)
    }

    /// Returns an iterator over retained points with their weights.
    pub fn iter(&self) -> DensityIter<'_, T> {
        DensityIter {
            levels: &self.levels,
            level_index: 0,
            item_index: 0,
        }
    }

    fn compact(&mut self) {
        for height in 0..self.levels.len() {
            if self.levels[height].len() >= self.k as usize {
                if height + 1 >= self.levels.len() {
                    self.levels.push(Vec::new());
                }
                self.compact_level(height);
                break;
            }
        }
    }

    fn compact_level(&mut self, height: usize) {
        let level_len = self.levels[height].len();
        if level_len == 0 {
            return;
        }
        let mut bits = vec![false; level_len];
        {
            let rng = &mut self.rng;
            let level = &mut self.levels[height];
            let kernel = &self.kernel;
            shuffle_with_rng(rng, level);
            bits[0] = random_bit(rng);
            for i in 1..level_len {
                let mut delta = 0.0f64;
                for (j, bit) in bits.iter().enumerate().take(i) {
                    let weight = if *bit { 1.0 } else { -1.0 };
                    delta += weight * kernel.evaluate(&level[i], &level[j]).to_f64();
                }
                bits[i] = delta < 0.0;
            }
        }
        let old_level = std::mem::take(&mut self.levels[height]);
        for (index, point) in old_level.into_iter().enumerate() {
            if bits[index] {
                self.levels[height + 1].push(point);
            } else {
                self.num_retained -= 1;
            }
        }
    }
}

/// Borrowed view of a retained point and its weight.
pub struct DensityItem<'a, T> {
    /// The retained point.
    point: &'a [T],
    /// The weight associated with the point.
    weight: u64,
}

impl<'a, T> DensityItem<'a, T> {
    /// Returns the retained point.
    pub fn point(&self) -> &'a [T] {
        self.point
    }

    /// Returns the weight associated with the point.
    pub fn weight(&self) -> u64 {
        self.weight
    }
}

/// Iterator over retained points and their weights.
pub struct DensityIter<'a, T> {
    levels: &'a [Vec<Vec<T>>],
    level_index: usize,
    item_index: usize,
}

impl<'a, T> Iterator for DensityIter<'a, T> {
    type Item = DensityItem<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.level_index < self.levels.len() {
            let level = &self.levels[self.level_index];
            if self.item_index < level.len() {
                let weight = weight_for_level(self.level_index);
                let item = DensityItem {
                    point: &level[self.item_index],
                    weight,
                };
                self.item_index += 1;
                return Some(item);
            }
            self.level_index += 1;
            self.item_index = 0;
        }
        None
    }
}

impl<'a, T: DensityValue, K: DensityKernel, R: RandomSource> IntoIterator
    for &'a DensitySketch<T, K, R>
{
    type Item = DensityItem<'a, T>;
    type IntoIter = DensityIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

fn weight_for_level(level: usize) -> u64 {
    match level {
        0..=63 => 1u64 << level,
        _ => panic!("level height too large"),
    }
}

fn random_bit<R: RandomSource>(rng: &mut R) -> bool {
    rng.next_bool()
}

fn shuffle_with_rng<R: RandomSource, T>(rng: &mut R, slice: &mut [T]) {
    if slice.len() <= 1 {
        return;
    }
    for i in (1..slice.len()).rev() {
        let j = (rng.next_u64() % (i as u64 + 1)) as usize;
        slice.swap(i, j);
    }
}

fn write_f32_value(bytes: &mut SketchBytes, value: f32) {
    bytes.write_f32_le(value);
}

fn write_f64_value(bytes: &mut SketchBytes, value: f64) {
    bytes.write_f64_le(value);
}

fn read_f32_value(cursor: &mut SketchSlice<'_>) -> std::io::Result<f32> {
    cursor.read_f32_le()
}

fn read_f64_value(cursor: &mut SketchSlice<'_>) -> std::io::Result<f64> {
    cursor.read_f64_le()
}

fn serialize_inner<T: DensityValue, K: DensityKernel, R: RandomSource>(
    sketch: &DensitySketch<T, K, R>,
    value_size: usize,
    write_value: SerializeValue<T>,
) -> Vec<u8> {
    let preamble_ints = if sketch.is_empty() {
        PREAMBLE_INTS_SHORT
    } else {
        PREAMBLE_INTS_LONG
    };
    let mut size_bytes = preamble_ints as usize * 4;
    if !sketch.is_empty() {
        for level in &sketch.levels {
            size_bytes += 4 + (level.len() * sketch.dim as usize * value_size);
        }
    }
    let mut bytes = SketchBytes::with_capacity(size_bytes);
    bytes.write_u8(preamble_ints);
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(DENSITY_FAMILY_ID);
    let flags = if sketch.is_empty() { FLAGS_IS_EMPTY } else { 0 };
    bytes.write_u8(flags);
    bytes.write_u16_le(sketch.k);
    bytes.write_u16_le(0);
    bytes.write_u32_le(sketch.dim);

    if sketch.is_empty() {
        return bytes.into_bytes();
    }

    bytes.write_u32_le(sketch.num_retained);
    bytes.write_u64_le(sketch.n);
    for level in &sketch.levels {
        bytes.write_u32_le(level.len() as u32);
        for point in level {
            for value in point {
                write_value(&mut bytes, *value);
            }
        }
    }
    bytes.into_bytes()
}

fn deserialize_inner<T: DensityValue, K: DensityKernel, R: RandomSource>(
    bytes: &[u8],
    kernel: K,
    rng: R,
    read_value: DeserializeValue<T>,
) -> Result<DensitySketch<T, K, R>, Error> {
    fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
        move |_| Error::insufficient_data(tag)
    }

    let mut cursor = SketchSlice::new(bytes);
    let preamble_ints = cursor.read_u8().map_err(make_error("preamble_ints"))?;
    let serial_version = cursor.read_u8().map_err(make_error("serial_version"))?;
    let family_id = cursor.read_u8().map_err(make_error("family_id"))?;
    let flags = cursor.read_u8().map_err(make_error("flags"))?;
    let k = cursor.read_u16_le().map_err(make_error("k"))?;
    cursor.read_u16_le().map_err(make_error("unused"))?;
    let dim = cursor.read_u32_le().map_err(make_error("dim"))?;

    if family_id != DENSITY_FAMILY_ID {
        return Err(Error::invalid_family(
            DENSITY_FAMILY_ID,
            family_id,
            "DensitySketch",
        ));
    }
    if serial_version != SERIAL_VERSION {
        return Err(Error::unsupported_serial_version(
            SERIAL_VERSION,
            serial_version,
        ));
    }
    if k < 2 {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("k must be > 1. Found: {k}"),
        ));
    }

    let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
    let expected_preamble = if is_empty {
        PREAMBLE_INTS_SHORT
    } else {
        PREAMBLE_INTS_LONG
    };
    if preamble_ints != expected_preamble {
        return Err(Error::invalid_preamble_longs(
            expected_preamble,
            preamble_ints,
        ));
    }
    if is_empty {
        return Ok(DensitySketch::with_kernel_and_rng(k, dim, kernel, rng));
    }

    let num_retained = cursor.read_u32_le().map_err(make_error("num_retained"))?;
    let n = cursor.read_u64_le().map_err(make_error("n"))?;

    let mut levels = Vec::new();
    let mut remaining = num_retained as i64;
    while remaining > 0 {
        let level_size = cursor.read_u32_le().map_err(make_error("level_size"))?;
        let mut level = Vec::with_capacity(level_size as usize);
        for _ in 0..level_size {
            let mut point = Vec::with_capacity(dim as usize);
            for _ in 0..dim {
                point.push(read_value(&mut cursor).map_err(make_error("point"))?);
            }
            level.push(point);
        }
        remaining -= level_size as i64;
        levels.push(level);
    }
    if remaining != 0 {
        return Err(Error::deserial(
            "invalid number of retained points while decoding density sketch",
        ));
    }

    Ok(DensitySketch {
        kernel,
        rng,
        k,
        dim,
        num_retained,
        n,
        levels,
    })
}
