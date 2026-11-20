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

use super::MAX_DENSE_BINS;
use super::mapping::IndexMapping;
use super::mapping::MappingKind;
use super::sketch::DdSketch;
use super::store::Store;
use super::store::StoreKind;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

const FLAG_TYPE_MASK: u8 = 0b0000_0011;
const FLAG_TYPE_SKETCH_FEATURES: u8 = 0b00;
const FLAG_TYPE_POSITIVE_STORE: u8 = 0b01;
const FLAG_TYPE_INDEX_MAPPING: u8 = 0b10;
const FLAG_TYPE_NEGATIVE_STORE: u8 = 0b11;

const SUBFLAG_ZERO_COUNT: u8 = 1 << 2;
const SUBFLAG_EXACT_SUM: u8 = 0x21 << 2;
const SUBFLAG_EXACT_MIN: u8 = 0x22 << 2;
const SUBFLAG_EXACT_MAX: u8 = 0x23 << 2;
const SUBFLAG_EXACT_COUNT: u8 = 0x28 << 2;

const SUBFLAG_MAPPING_LOGARITHMIC: u8 = 0 << 2;
const SUBFLAG_MAPPING_LINEAR: u8 = 1 << 2;
const SUBFLAG_MAPPING_CUBIC: u8 = 3 << 2;

const SUBFLAG_BINS_DELTAS_AND_COUNTS: u8 = 1 << 2;
const SUBFLAG_BINS_INDEX_DELTAS: u8 = 2 << 2;
const SUBFLAG_BINS_CONTIGUOUS_COUNTS: u8 = 3 << 2;

const MAX_VAR_LEN_64: usize = 9;
const VARFLOAT_ROTATE: u32 = 6;
const ONE_F64_BITS: u64 = 0x3ff0_0000_0000_0000;
// 2^64 as f64; any f64 >= TWO_POW_64 overflows u64.
const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;
// Largest f64 strictly below 2^64 (2^64 - 2048), used when serializing counts near u64::MAX.
const MAX_ENCODABLE_U64_F64: f64 = f64::from_bits(0x43f0_0000_0000_0000 - 1);

pub fn serialize_sketch(sketch: &DdSketch) -> Vec<u8> {
    let mapping = sketch.mapping();
    let zero_weight = sketch.zero_weight();
    let pos_size = encoded_store_size(sketch.positive_store());
    let neg_size = encoded_store_size(sketch.negative_store());
    let zero_size = if zero_weight == 0 {
        0
    } else {
        1 + varfloat64_size(u64_to_encodable_f64(zero_weight))
    };

    let mut out = SketchBytes::with_capacity(17 + zero_size + pos_size + neg_size);

    let mapping_subflag = match mapping.kind() {
        MappingKind::Logarithmic => SUBFLAG_MAPPING_LOGARITHMIC,
        MappingKind::Linear => SUBFLAG_MAPPING_LINEAR,
        MappingKind::Cubic => SUBFLAG_MAPPING_CUBIC,
    };
    out.write_u8(FLAG_TYPE_INDEX_MAPPING | mapping_subflag);
    out.write_f64_le(mapping.gamma());
    out.write_f64_le(mapping.index_offset());

    if zero_weight != 0 {
        out.write_u8(FLAG_TYPE_SKETCH_FEATURES | SUBFLAG_ZERO_COUNT);
        encode_varfloat64(&mut out, u64_to_encodable_f64(zero_weight));
    }

    encode_store(&mut out, sketch.positive_store(), FLAG_TYPE_POSITIVE_STORE);
    encode_store(&mut out, sketch.negative_store(), FLAG_TYPE_NEGATIVE_STORE);

    out.into_bytes()
}

pub fn deserialize_sketch(bytes: &[u8], store_kind: StoreKind) -> Result<DdSketch, Error> {
    let mut positive = Store::new(store_kind)?;
    let mut negative = Store::new(store_kind)?;
    let mut pos_range = IndexRangeTracker::new(matches!(store_kind, StoreKind::UnboundedDense));
    let mut neg_range = IndexRangeTracker::new(matches!(store_kind, StoreKind::UnboundedDense));
    let mut mapping: Option<IndexMapping> = None;
    let mut zero_weight = 0_u64;
    let mut total_weight = 0_u64;

    let mut cursor = SketchSlice::new(bytes);
    while !cursor.remaining().is_empty() {
        let flag = cursor.read_u8().map_err(insufficient_data("flag"))?;
        let flag_type = flag & FLAG_TYPE_MASK;
        let subflag = flag & !FLAG_TYPE_MASK;

        match flag_type {
            FLAG_TYPE_INDEX_MAPPING => {
                let kind = match subflag {
                    SUBFLAG_MAPPING_LOGARITHMIC => MappingKind::Logarithmic,
                    SUBFLAG_MAPPING_LINEAR => MappingKind::Linear,
                    SUBFLAG_MAPPING_CUBIC => MappingKind::Cubic,
                    _ => {
                        return Err(Error::deserial(format!(
                            "unsupported DDSketch index mapping flag: {flag:#04x}"
                        )));
                    }
                };
                let gamma = cursor.read_f64_le().map_err(insufficient_data("gamma"))?;
                let index_offset = cursor
                    .read_f64_le()
                    .map_err(insufficient_data("index_offset"))?;
                let decoded = IndexMapping::from_gamma_offset(kind, gamma, index_offset)?;
                if let Some(existing) = mapping {
                    if !existing.is_compatible(&decoded) {
                        return Err(Error::deserial(
                            "conflicting index mapping blocks in serialized DDSketch",
                        ));
                    }
                } else {
                    mapping = Some(decoded);
                }
            }
            FLAG_TYPE_POSITIVE_STORE => {
                decode_store_block(
                    &mut cursor,
                    subflag,
                    &mut positive,
                    &mut pos_range,
                    &mut total_weight,
                )?;
            }
            FLAG_TYPE_NEGATIVE_STORE => {
                decode_store_block(
                    &mut cursor,
                    subflag,
                    &mut negative,
                    &mut neg_range,
                    &mut total_weight,
                )?;
            }
            FLAG_TYPE_SKETCH_FEATURES => match subflag {
                SUBFLAG_ZERO_COUNT => {
                    let count = decode_count(&mut cursor, "zero_count")?;
                    zero_weight = zero_weight
                        .checked_add(count)
                        .ok_or_else(|| Error::deserial("zero_count overflows u64"))?;
                    total_weight = total_weight
                        .checked_add(count)
                        .ok_or_else(|| Error::deserial("total_weight overflows u64"))?;
                }
                SUBFLAG_EXACT_COUNT => {
                    // Optional summary statistic emitted by DDSketchWithExactSummaryStatistics.
                    let raw = decode_varfloat64(&mut cursor, "exact_count")?;
                    if !raw.is_finite() || raw < 0.0 {
                        return Err(Error::deserial(format!(
                            "invalid exact_count summary statistic: {raw}"
                        )));
                    }
                }
                SUBFLAG_EXACT_SUM | SUBFLAG_EXACT_MIN | SUBFLAG_EXACT_MAX => {
                    let _ = cursor
                        .read_f64_le()
                        .map_err(insufficient_data("summary_statistic"))?;
                }
                _ => {
                    return Err(Error::deserial(format!(
                        "unsupported DDSketch feature flag: {flag:#04x}"
                    )));
                }
            },
            _ => unreachable!("2-bit mask only produces 0..=3"),
        }
    }

    let Some(mapping) = mapping else {
        return Err(Error::deserial(
            "missing index mapping in serialized DDSketch",
        ));
    };

    Ok(DdSketch::from_parts(
        mapping,
        negative,
        positive,
        zero_weight,
        total_weight,
    ))
}

#[derive(Clone, Copy)]
enum BinEncodingChoice {
    Empty,
    ContiguousCounts { num_bins: u64, min_index: i32 },
    IndexDeltas { num_bins: u64 },
    IndexDeltasAndCounts { num_bins: u64 },
}

fn choose_bin_encoding(store: &Store) -> (BinEncodingChoice, usize) {
    if store.is_empty() {
        return (BinEncodingChoice::Empty, 0);
    }

    let num_non_empty = store.num_occupied() as u64;
    let mut sparse_deltas_bytes = 1 + uvarint64_size(num_non_empty);
    let mut sparse_counts_bytes = 0_usize;
    let mut all_unit_weights = true;
    let mut prev_index = 0_i64;

    for (idx, weight) in store.iter_asc() {
        let idx64 = i64::from(idx);
        sparse_deltas_bytes += varint64_size(idx64 - prev_index);
        sparse_counts_bytes += varfloat64_size(u64_to_encodable_f64(weight));
        if weight != 1 {
            all_unit_weights = false;
        }
        prev_index = idx64;
    }

    let (best_sparse_choice, best_sparse_bytes) = if all_unit_weights {
        (
            BinEncodingChoice::IndexDeltas {
                num_bins: num_non_empty,
            },
            sparse_deltas_bytes,
        )
    } else {
        (
            BinEncodingChoice::IndexDeltasAndCounts {
                num_bins: num_non_empty,
            },
            sparse_deltas_bytes + sparse_counts_bytes,
        )
    };

    if let Some((min_index, contiguous)) = store.dense_contiguous_slice() {
        let num_bins = contiguous.len() as u64;
        let num_zero_bins = contiguous.len() - store.num_occupied();
        // Zero count encodes as single byte 0x00 in varfloat64.
        let dense_bytes = 1
            + uvarint64_size(num_bins)
            + varint64_size(i64::from(min_index))
            + varint64_size(1)
            + sparse_counts_bytes
            + num_zero_bins;
        if dense_bytes <= best_sparse_bytes {
            return (
                BinEncodingChoice::ContiguousCounts {
                    num_bins,
                    min_index,
                },
                dense_bytes,
            );
        }
    }

    (best_sparse_choice, best_sparse_bytes)
}

fn encoded_store_size(store: &Store) -> usize {
    choose_bin_encoding(store).1
}

fn encode_store(out: &mut SketchBytes, store: &Store, flag_type: u8) {
    let (choice, _) = choose_bin_encoding(store);
    match choice {
        BinEncodingChoice::Empty => {}
        BinEncodingChoice::ContiguousCounts {
            num_bins,
            min_index,
        } => {
            let (_, slice) = store
                .dense_contiguous_slice()
                .expect("contiguous encoding only selected for non-empty dense store");
            out.write_u8(flag_type | SUBFLAG_BINS_CONTIGUOUS_COUNTS);
            encode_uvarint64(out, num_bins);
            encode_varint64(out, i64::from(min_index));
            encode_varint64(out, 1);
            for &weight in slice {
                encode_varfloat64(out, u64_to_encodable_f64(weight));
            }
        }
        BinEncodingChoice::IndexDeltas { num_bins } => {
            out.write_u8(flag_type | SUBFLAG_BINS_INDEX_DELTAS);
            encode_uvarint64(out, num_bins);
            let mut prev = 0_i64;
            for (idx, _) in store.iter_asc() {
                let idx64 = i64::from(idx);
                encode_varint64(out, idx64 - prev);
                prev = idx64;
            }
        }
        BinEncodingChoice::IndexDeltasAndCounts { num_bins } => {
            out.write_u8(flag_type | SUBFLAG_BINS_DELTAS_AND_COUNTS);
            encode_uvarint64(out, num_bins);
            let mut prev = 0_i64;
            for (idx, weight) in store.iter_asc() {
                let idx64 = i64::from(idx);
                encode_varint64(out, idx64 - prev);
                encode_varfloat64(out, u64_to_encodable_f64(weight));
                prev = idx64;
            }
        }
    }
}

struct IndexRangeTracker {
    enforce_span_limit: bool,
    min_index: i64,
    max_index: i64,
    seeded: bool,
}

impl IndexRangeTracker {
    const fn new(enforce_span_limit: bool) -> Self {
        Self {
            enforce_span_limit,
            min_index: 0,
            max_index: 0,
            seeded: false,
        }
    }

    fn accept(&mut self, index: i64) -> Result<i32, Error> {
        let idx32 = i32::try_from(index).map_err(|_| {
            Error::deserial(format!("DDSketch bin index {index} overflows 32-bit range"))
        })?;
        if self.enforce_span_limit {
            if !self.seeded {
                self.min_index = index;
                self.max_index = index;
                self.seeded = true;
            } else {
                self.min_index = self.min_index.min(index);
                self.max_index = self.max_index.max(index);
            }
            let span = (self.max_index - self.min_index) as u64;
            if span >= MAX_DENSE_BINS as u64 {
                return Err(Error::deserial(format!(
                    "decoded DDSketch index span ({}) exceeds maximum dense limit ({MAX_DENSE_BINS})",
                    span + 1
                )));
            }
        }
        Ok(idx32)
    }
}

fn decode_store_block(
    cursor: &mut SketchSlice<'_>,
    subflag: u8,
    store: &mut Store,
    tracker: &mut IndexRangeTracker,
    total_weight: &mut u64,
) -> Result<(), Error> {
    match subflag {
        SUBFLAG_BINS_DELTAS_AND_COUNTS => {
            let num_bins = decode_uvarint64(cursor, "num_bins")?;
            // Each entry encodes at least 1 byte of index delta + 1 byte of count.
            if num_bins > (cursor.remaining().len() / 2) as u64 {
                return Err(Error::insufficient_data_of(
                    "store_bins",
                    format!(
                        "declared {num_bins} bins exceeds remaining {} bytes",
                        cursor.remaining().len()
                    ),
                ));
            }
            let mut index = 0_i64;
            for _ in 0..num_bins {
                let delta = decode_varint64(cursor, "index_delta")?;
                let count = decode_count(cursor, "bin_count")?;
                index = index
                    .checked_add(delta)
                    .ok_or_else(|| Error::deserial("DDSketch bin index delta overflow"))?;
                let idx32 = i32::try_from(index).map_err(|_| {
                    Error::deserial(format!("DDSketch bin index {index} overflows 32-bit range"))
                })?;
                if count != 0 {
                    tracker.accept(index)?;
                    *total_weight = total_weight
                        .checked_add(count)
                        .ok_or_else(|| Error::deserial("total_weight overflows u64"))?;
                    store.add(idx32, count);
                }
            }
        }
        SUBFLAG_BINS_INDEX_DELTAS => {
            let num_bins = decode_uvarint64(cursor, "num_bins")?;
            if num_bins > cursor.remaining().len() as u64 {
                return Err(Error::insufficient_data_of(
                    "store_bins",
                    format!(
                        "declared {num_bins} bins exceeds remaining {} bytes",
                        cursor.remaining().len()
                    ),
                ));
            }
            let mut index = 0_i64;
            for _ in 0..num_bins {
                let delta = decode_varint64(cursor, "index_delta")?;
                index = index
                    .checked_add(delta)
                    .ok_or_else(|| Error::deserial("DDSketch bin index delta overflow"))?;
                let idx32 = tracker.accept(index)?;
                *total_weight = total_weight
                    .checked_add(1)
                    .ok_or_else(|| Error::deserial("total_weight overflows u64"))?;
                store.add(idx32, 1);
            }
        }
        SUBFLAG_BINS_CONTIGUOUS_COUNTS => {
            let num_bins = decode_uvarint64(cursor, "num_bins")?;
            let first_index = decode_varint64(cursor, "first_index")?;
            let index_delta = decode_varint64(cursor, "index_delta")?;
            if num_bins > cursor.remaining().len() as u64 {
                return Err(Error::insufficient_data_of(
                    "contiguous_bins",
                    format!(
                        "declared {num_bins} bins exceeds remaining {} bytes",
                        cursor.remaining().len()
                    ),
                ));
            }
            if num_bins > 0 {
                validate_contiguous_bounds(first_index, num_bins, index_delta)?;
            }
            let mut index = first_index;
            for step in 0..num_bins {
                let count = decode_count(cursor, "bin_count")?;
                if count != 0 {
                    let idx32 = tracker.accept(index)?;
                    *total_weight = total_weight
                        .checked_add(count)
                        .ok_or_else(|| Error::deserial("total_weight overflows u64"))?;
                    store.add(idx32, count);
                }
                if step + 1 < num_bins {
                    index += index_delta;
                }
            }
        }
        _ => {
            return Err(Error::deserial(format!(
                "unsupported DDSketch bin encoding mode: {:#04x}",
                subflag >> 2
            )));
        }
    }
    Ok(())
}

fn validate_contiguous_bounds(
    first_index: i64,
    num_bins: u64,
    index_delta: i64,
) -> Result<(), Error> {
    if i32::try_from(first_index).is_err() {
        return Err(Error::deserial(format!(
            "DDSketch contiguous first_index {first_index} overflows 32-bit range"
        )));
    }
    let steps = num_bins - 1;
    if steps == 0 {
        return Ok(());
    }
    let max_i32_span = (i64::from(i32::MAX) - i64::from(i32::MIN)) as u64;
    let abs_delta = index_delta.unsigned_abs();
    if abs_delta > max_i32_span / steps {
        return Err(Error::deserial(
            "DDSketch contiguous block exceeds 32-bit index bounds",
        ));
    }
    let last_index = first_index + (steps as i64) * index_delta;
    if i32::try_from(last_index).is_err() {
        return Err(Error::deserial(format!(
            "DDSketch contiguous last_index {last_index} overflows 32-bit range"
        )));
    }
    Ok(())
}

#[inline]
fn u64_to_encodable_f64(v: u64) -> f64 {
    let as_f64 = v as f64;
    if as_f64 >= TWO_POW_64 {
        MAX_ENCODABLE_U64_F64
    } else {
        as_f64
    }
}

#[inline]
fn decode_count(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<u64, Error> {
    let raw = decode_varfloat64(cursor, tag)?;
    if !raw.is_finite() || raw < 0.0 || raw.fract() != 0.0 || raw >= TWO_POW_64 {
        return Err(Error::deserial(format!(
            "invalid DDSketch {tag}: expected non-negative integer < 2^64, got {raw}"
        )));
    }
    Ok(raw as u64)
}

#[inline]
pub fn encode_uvarint64(out: &mut SketchBytes, mut v: u64) {
    for _ in 0..(MAX_VAR_LEN_64 - 1) {
        if v < 0x80 {
            break;
        }
        out.write_u8((v as u8) | 0x80);
        v >>= 7;
    }
    out.write_u8(v as u8);
}

#[inline]
pub fn decode_uvarint64(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<u64, Error> {
    let mut x = 0_u64;
    let mut shift = 0_u32;
    for i in 0..MAX_VAR_LEN_64 {
        let n = cursor.read_u8().map_err(insufficient_data(tag))?;
        if n < 0x80 || i == MAX_VAR_LEN_64 - 1 {
            return Ok(x | (u64::from(n) << shift));
        }
        x |= u64::from(n & 0x7f) << shift;
        shift += 7;
    }
    unreachable!()
}

#[inline]
pub const fn uvarint64_size(v: u64) -> usize {
    if v == 0 {
        return 1;
    }
    let sig_bits = 64 - v.leading_zeros() as usize;
    if sig_bits == 64 {
        MAX_VAR_LEN_64
    } else {
        sig_bits.div_ceil(7)
    }
}

#[inline]
pub fn encode_varint64(out: &mut SketchBytes, v: i64) {
    let zigzag = ((v << 1) ^ (v >> 63)) as u64;
    encode_uvarint64(out, zigzag);
}

#[inline]
pub fn decode_varint64(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<i64, Error> {
    let u = decode_uvarint64(cursor, tag)?;
    Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
}

#[inline]
pub const fn varint64_size(v: i64) -> usize {
    let zigzag = ((v << 1) ^ (v >> 63)) as u64;
    uvarint64_size(zigzag)
}

#[inline]
pub fn encode_varfloat64(out: &mut SketchBytes, v: f64) {
    let mut x = (v + 1.0)
        .to_bits()
        .wrapping_sub(ONE_F64_BITS)
        .rotate_left(VARFLOAT_ROTATE);
    for _ in 0..(MAX_VAR_LEN_64 - 1) {
        let n = (x >> 57) as u8;
        x <<= 7;
        if x == 0 {
            out.write_u8(n);
            return;
        }
        out.write_u8(n | 0x80);
    }
    out.write_u8((x >> 56) as u8);
}

#[inline]
pub fn decode_varfloat64(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<f64, Error> {
    let mut x = 0_u64;
    let mut shift = 57_u32;
    for i in 0..MAX_VAR_LEN_64 {
        let n = cursor.read_u8().map_err(insufficient_data(tag))?;
        if i == MAX_VAR_LEN_64 - 1 {
            x |= u64::from(n);
            break;
        }
        if n < 0x80 {
            x |= u64::from(n) << shift;
            break;
        }
        x |= u64::from(n & 0x7f) << shift;
        shift -= 7;
    }
    let bits = x.rotate_right(VARFLOAT_ROTATE).wrapping_add(ONE_F64_BITS);
    Ok(f64::from_bits(bits) - 1.0)
}

#[inline]
pub fn varfloat64_size(v: f64) -> usize {
    let x = (v + 1.0)
        .to_bits()
        .wrapping_sub(ONE_F64_BITS)
        .rotate_left(VARFLOAT_ROTATE);
    if x == 0 {
        return 1;
    }
    let active_msb_bits = 64 - x.trailing_zeros() as usize;
    if active_msb_bits > 56 {
        MAX_VAR_LEN_64
    } else {
        active_msb_bits.div_ceil(7)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datadog_uvarint64_reference_vectors_match() {
        let cases: &[(u64, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (129, &[0x81, 0x01]),
            (255, &[0xff, 0x01]),
            (256, &[0x80, 0x02]),
            (16_383, &[0xff, 0x7f]),
            (16_384, &[0x80, 0x80, 0x01]),
            (16_385, &[0x81, 0x80, 0x01]),
            (
                u64::MAX - 1,
                &[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ),
            (
                u64::MAX,
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ),
        ];
        for &(val, expected) in cases {
            let mut buf = SketchBytes::default();
            encode_uvarint64(&mut buf, val);
            let bytes = buf.into_bytes();
            assert_eq!(bytes, expected, "uvarint64({val})");
            assert_eq!(uvarint64_size(val), expected.len(), "size({val})");
            let mut slice = SketchSlice::new(&bytes);
            assert_eq!(decode_uvarint64(&mut slice, "test").unwrap(), val);
            assert!(slice.remaining().is_empty());
        }
    }

    #[test]
    fn datadog_varint64_reference_vectors_match() {
        let cases: &[(i64, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x02]),
            (63, &[0x7e]),
            (64, &[0x80, 0x01]),
            (-1, &[0x01]),
            (-63, &[0x7d]),
            (-64, &[0x7f]),
            (-65, &[0x81, 0x01]),
            (
                i64::MAX,
                &[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ),
            (
                i64::MIN,
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ),
        ];
        for &(val, expected) in cases {
            let mut buf = SketchBytes::default();
            encode_varint64(&mut buf, val);
            let bytes = buf.into_bytes();
            assert_eq!(bytes, expected, "varint64({val})");
            assert_eq!(varint64_size(val), expected.len(), "size({val})");
            let mut slice = SketchSlice::new(&bytes);
            assert_eq!(decode_varint64(&mut slice, "test").unwrap(), val);
            assert!(slice.remaining().is_empty());
        }
    }

    #[test]
    fn datadog_varfloat64_reference_vectors_match() {
        let cases: &[(f64, &[u8])] = &[
            (0.0, &[0x00]),
            (1.0, &[0x02]),
            (2.0, &[0x03]),
            (3.0, &[0x04]),
            (4.0, &[0x84, 0x40]),
            (5.0, &[0x05]),
            (6.0, &[0x85, 0x40]),
            (7.0, &[0x06]),
            (8.0, &[0x86, 0x20]),
            (9.0, &[0x86, 0x40]),
            (
                ((1_u64 << 52) - 2) as f64,
                &[0xe7, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x80],
            ),
            (((1_u64 << 52) - 1) as f64, &[0x68]),
            (
                (1_u64 << 52) as f64,
                &[0xe8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40],
            ),
            (
                ((1_u64 << 53) - 2) as f64,
                &[0xe9, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xc0],
            ),
            (((1_u64 << 53) - 1) as f64, &[0x6a]),
            (
                -1.0,
                &[0x82, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x30],
            ),
            (
                -0.5,
                &[0xfe, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x3f],
            ),
        ];
        for &(val, expected) in cases {
            let mut buf = SketchBytes::default();
            encode_varfloat64(&mut buf, val);
            let bytes = buf.into_bytes();
            assert_eq!(bytes, expected, "varfloat64({val})");
            assert_eq!(varfloat64_size(val), expected.len(), "size({val})");
            let mut slice = SketchSlice::new(&bytes);
            assert_eq!(decode_varfloat64(&mut slice, "test").unwrap(), val);
            assert!(slice.remaining().is_empty());
        }
    }
}
