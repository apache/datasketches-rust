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
use super::sketch::SummaryState;
use super::store::MAX_DENSE_LEN;
use super::store::Store;
use super::store::StoreKind;
use super::store::span_u64;
use super::weight::CONFIG_FLOAT_WEIGHTS;
use super::weight::DdSketchWeight;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;

// Native DDSKETCH preamble constants (3 longs = 24 bytes).
// `PREAMBLE_LONGS = 3` (`0x03`) is also disjoint from every valid leading flag byte of the
// reference DDSketch format (`flag_type = 0b11` defines no `subflag = 0`), ensuring cross-format
// inputs fail cleanly on the first header check.
const PREAMBLE_LONGS: u8 = 3;
const SERIAL_VERSION: u8 = 1;
const PREAMBLE_BYTES: usize = (PREAMBLE_LONGS as usize) * size_of::<u64>();

const CONFIG_MAX_BINS_MASK: u32 = (1 << 27) - 1;
const CONFIG_RESERVED_MASK: u32 = !(CONFIG_FLOAT_WEIGHTS | CONFIG_MAX_BINS_MASK);

const _: () = assert!(MAX_DENSE_BINS <= CONFIG_MAX_BINS_MASK as usize);

const MAPPING_MASK: u8 = 0b0000_0011;
const MAPPING_TAG_LOGARITHMIC: u8 = 0;
const MAPPING_TAG_LINEAR: u8 = 1;
const MAPPING_TAG_CUBIC: u8 = 2;

const STORE_SHIFT: u8 = 2;
const STORE_MASK: u8 = 0b0000_0011;
const STORE_TAG_COLLAPSING_LOWEST: u8 = 0;
const STORE_TAG_COLLAPSING_HIGHEST: u8 = 1;
const STORE_TAG_UNBOUNDED_DENSE: u8 = 2;
const STORE_TAG_SPARSE: u8 = 3;

const FLAG_IS_EMPTY: u8 = 1 << 4;
const FLAG_NEG_COLLAPSED: u8 = 1 << 5;
const FLAG_POS_COLLAPSED: u8 = 1 << 6;
const FLAG_HAS_EXACT_SUMMARY: u8 = 1 << 7;

const NATIVE_STORE_EMPTY: u8 = 0;
const NATIVE_STORE_CONTIGUOUS_COUNTS: u8 = 1;
const NATIVE_STORE_INDEX_DELTAS: u8 = 2;
const NATIVE_STORE_DELTAS_AND_COUNTS: u8 = 3;
// Dense stores wider than 65,536 bins (~512 KiB per sign) always serialize as contiguous counts so
// input byte length naturally bounds dense allocation on decode without rejecting valid wide
// `UnboundedDense` round-trips.
const MAX_SPARSE_ENCODED_DENSE_SPAN: usize = 65_536;

// Reference (Datadog) block-stream wire constants.
const DD_FLAG_TYPE_MASK: u8 = 0b0000_0011;
const DD_FLAG_TYPE_SKETCH_FEATURES: u8 = 0b00;
const DD_FLAG_TYPE_POSITIVE_STORE: u8 = 0b01;
const DD_FLAG_TYPE_INDEX_MAPPING: u8 = 0b10;
const DD_FLAG_TYPE_NEGATIVE_STORE: u8 = 0b11;

const DD_SUBFLAG_ZERO_COUNT: u8 = 1 << 2;
const DD_SUBFLAG_EXACT_SUM: u8 = 0x21 << 2;
const DD_SUBFLAG_EXACT_MIN: u8 = 0x22 << 2;
const DD_SUBFLAG_EXACT_MAX: u8 = 0x23 << 2;
const DD_SUBFLAG_EXACT_COUNT: u8 = 0x28 << 2;

const DD_SUBFLAG_MAPPING_LOGARITHMIC: u8 = 0 << 2;
const DD_SUBFLAG_MAPPING_LINEAR: u8 = 1 << 2;
const DD_SUBFLAG_MAPPING_CUBIC: u8 = 3 << 2;

const DD_SUBFLAG_BINS_DELTAS_AND_COUNTS: u8 = 1 << 2;
const DD_SUBFLAG_BINS_INDEX_DELTAS: u8 = 2 << 2;
const DD_SUBFLAG_BINS_CONTIGUOUS_COUNTS: u8 = 3 << 2;

const MAX_VAR_LEN_64: usize = 9;
const VARFLOAT_ROTATE: u32 = 6;
const ONE_F64_BITS: u64 = 0x3ff0_0000_0000_0000;

#[inline]
fn canonical_zero(v: f64) -> f64 {
    if v == 0.0 { 0.0 } else { v }
}

pub fn serialize_sketch<W: DdSketchWeight>(sketch: &DdSketch<W>) -> Vec<u8> {
    let mapping = sketch.mapping();
    let is_empty = sketch.is_empty();
    let summary = sketch.summary_state();
    let (store_tag, max_bins_u32) = encode_store_kind(sketch.store_kind());
    let config_word = max_bins_u32 | W::NATIVE_CONFIG_FLAG;
    let mapping_tag = match mapping.kind() {
        MappingKind::Logarithmic => MAPPING_TAG_LOGARITHMIC,
        MappingKind::Linear => MAPPING_TAG_LINEAR,
        MappingKind::Cubic => MAPPING_TAG_CUBIC,
    };

    let mut flags = mapping_tag | (store_tag << STORE_SHIFT);
    if is_empty {
        flags |= FLAG_IS_EMPTY;
    } else {
        if sketch.negative_store().is_collapsed() {
            flags |= FLAG_NEG_COLLAPSED;
        }
        if sketch.positive_store().is_collapsed() {
            flags |= FLAG_POS_COLLAPSED;
        }
        if matches!(summary, SummaryState::Active { .. }) {
            flags |= FLAG_HAS_EXACT_SUMMARY;
        }
    }

    let (neg_choice, neg_bytes) = if is_empty {
        (NativeBinChoice::Empty, 0)
    } else {
        choose_native_bin_encoding(sketch.negative_store())
    };
    let (pos_choice, pos_bytes) = if is_empty {
        (NativeBinChoice::Empty, 0)
    } else {
        choose_native_bin_encoding(sketch.positive_store())
    };
    let summary_bytes = if matches!(summary, SummaryState::Active { .. }) {
        3 * size_of::<f64>()
    } else {
        0
    };
    let payload_bytes = if is_empty {
        0
    } else {
        summary_bytes + sketch.zero_weight().native_size() + neg_bytes + pos_bytes
    };

    let mut out = SketchBytes::with_capacity(PREAMBLE_BYTES + payload_bytes);
    out.write_u8(PREAMBLE_LONGS);
    out.write_u8(SERIAL_VERSION);
    out.write_u8(Family::DDSKETCH.id);
    out.write_u8(flags);
    out.write_u32_le(config_word);
    out.write_f64_le(mapping.gamma());
    out.write_f64_le(mapping.index_offset());

    if is_empty {
        return out.into_bytes();
    }

    if let SummaryState::Active { min, max, sum } = summary {
        out.write_f64_le(min);
        out.write_f64_le(max);
        out.write_f64_le(sum);
    }
    sketch.zero_weight().encode_native(&mut out);
    encode_native_store(&mut out, sketch.negative_store(), neg_choice);
    encode_native_store(&mut out, sketch.positive_store(), pos_choice);

    out.into_bytes()
}

pub fn deserialize_sketch<W: DdSketchWeight>(bytes: &[u8]) -> Result<DdSketch<W>, Error> {
    deserialize_native(bytes)
}

pub fn serialize_compat_sketch<W: DdSketchWeight>(sketch: &DdSketch<W>) -> Result<Vec<u8>, Error> {
    let mapping = sketch.mapping();
    let mapping_subflag = match mapping.kind() {
        MappingKind::Logarithmic => DD_SUBFLAG_MAPPING_LOGARITHMIC,
        MappingKind::Linear => DD_SUBFLAG_MAPPING_LINEAR,
        MappingKind::Cubic => DD_SUBFLAG_MAPPING_CUBIC,
    };

    let total_weight = sketch.total_weight();
    let summary = sketch.summary_state();
    let summary_bytes = match summary {
        SummaryState::Active { sum, .. } => {
            total_weight.check_compat_encode("total_weight")?;
            let count_bytes = 1 + total_weight.compat_size();
            let min_max_bytes = 2 * (1 + size_of::<f64>());
            let sum_bytes = if sum != 0.0 { 1 + size_of::<f64>() } else { 0 };
            count_bytes + min_max_bytes + sum_bytes
        }
        SummaryState::Empty | SummaryState::Missing => 0,
    };

    let zero_weight = sketch.zero_weight();
    zero_weight.check_compat_encode("zero_weight")?;
    let zero_bytes = if zero_weight.is_zero() {
        0
    } else {
        1 + zero_weight.compat_size()
    };
    let neg_plan = choose_compat_bin_encoding(sketch.negative_store())?;
    let pos_plan = choose_compat_bin_encoding(sketch.positive_store())?;
    let has_nonzero_compat_weight = zero_weight.compat_encodes_nonzero()
        || neg_plan.has_nonzero_weight
        || pos_plan.has_nonzero_weight;
    if !sketch.is_empty() && !has_nonzero_compat_weight {
        return Err(Error::invalid_argument(format!(
            "reference DDSketch compatibility format rounds all non-empty {} weights to zero",
            W::TYPE_NAME
        )));
    }

    let mut out = SketchBytes::with_capacity(
        summary_bytes + 17 + zero_bytes + neg_plan.bytes + pos_plan.bytes,
    );
    if let SummaryState::Active { min, max, sum } = summary {
        out.write_u8(DD_SUBFLAG_EXACT_COUNT | DD_FLAG_TYPE_SKETCH_FEATURES);
        total_weight.encode_compat(&mut out);
        out.write_u8(DD_SUBFLAG_EXACT_MIN | DD_FLAG_TYPE_SKETCH_FEATURES);
        out.write_f64_le(min);
        out.write_u8(DD_SUBFLAG_EXACT_MAX | DD_FLAG_TYPE_SKETCH_FEATURES);
        out.write_f64_le(max);
        if sum != 0.0 {
            out.write_u8(DD_SUBFLAG_EXACT_SUM | DD_FLAG_TYPE_SKETCH_FEATURES);
            out.write_f64_le(sum);
        }
    }
    out.write_u8(mapping_subflag | DD_FLAG_TYPE_INDEX_MAPPING);
    out.write_f64_le(mapping.gamma());
    out.write_f64_le(mapping.index_offset());

    if !zero_weight.is_zero() {
        out.write_u8(DD_SUBFLAG_ZERO_COUNT | DD_FLAG_TYPE_SKETCH_FEATURES);
        zero_weight.encode_compat(&mut out);
    }
    if let Some(choice) = neg_plan.choice {
        encode_compat_store(
            &mut out,
            sketch.negative_store(),
            DD_FLAG_TYPE_NEGATIVE_STORE,
            choice,
        );
    }
    if let Some(choice) = pos_plan.choice {
        encode_compat_store(
            &mut out,
            sketch.positive_store(),
            DD_FLAG_TYPE_POSITIVE_STORE,
            choice,
        );
    }

    Ok(out.into_bytes())
}

pub fn deserialize_compat_sketch<W: DdSketchWeight>(
    bytes: &[u8],
    store_kind: StoreKind,
) -> Result<DdSketch<W>, Error> {
    deserialize_datadog(bytes, store_kind)
}

fn encode_store_kind(kind: StoreKind) -> (u8, u32) {
    match kind {
        StoreKind::CollapsingLowestDense { max_num_bins } => (
            STORE_TAG_COLLAPSING_LOWEST,
            u32::try_from(max_num_bins).expect("validated max_num_bins fits in u32"),
        ),
        StoreKind::CollapsingHighestDense { max_num_bins } => (
            STORE_TAG_COLLAPSING_HIGHEST,
            u32::try_from(max_num_bins).expect("validated max_num_bins fits in u32"),
        ),
        StoreKind::UnboundedDense => (STORE_TAG_UNBOUNDED_DENSE, 0),
        StoreKind::Sparse => (STORE_TAG_SPARSE, 0),
    }
}

fn decode_store_kind(store_tag: u8, max_bins_u32: u32) -> Result<StoreKind, Error> {
    match store_tag {
        STORE_TAG_COLLAPSING_LOWEST | STORE_TAG_COLLAPSING_HIGHEST => {
            let max_num_bins = max_bins_u32 as usize;
            if !(1..=MAX_DENSE_BINS).contains(&max_num_bins) {
                return Err(Error::deserial(format!(
                    "DDSketch max_num_bins must be in [1, {MAX_DENSE_BINS}], got {max_bins_u32}"
                )));
            }
            if store_tag == STORE_TAG_COLLAPSING_LOWEST {
                Ok(StoreKind::CollapsingLowestDense { max_num_bins })
            } else {
                Ok(StoreKind::CollapsingHighestDense { max_num_bins })
            }
        }
        STORE_TAG_UNBOUNDED_DENSE => {
            if max_bins_u32 != 0 {
                return Err(Error::deserial(format!(
                    "expected max_num_bins = 0 for UnboundedDense, got {max_bins_u32}"
                )));
            }
            Ok(StoreKind::UnboundedDense)
        }
        STORE_TAG_SPARSE => {
            if max_bins_u32 != 0 {
                return Err(Error::deserial(format!(
                    "expected max_num_bins = 0 for Sparse, got {max_bins_u32}"
                )));
            }
            Ok(StoreKind::Sparse)
        }
        _ => unreachable!("2-bit store tag only produces 0..=3"),
    }
}

fn deserialize_native<W: DdSketchWeight>(bytes: &[u8]) -> Result<DdSketch<W>, Error> {
    let mut cursor = SketchSlice::new(bytes);
    let preamble_longs = cursor
        .read_u8()
        .map_err(insufficient_data("preamble_longs"))?;
    let serial_version = cursor
        .read_u8()
        .map_err(insufficient_data("serial_version"))?;
    let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;

    Family::DDSKETCH.validate_id(family_id)?;
    ensure_serial_version_is(SERIAL_VERSION, serial_version)?;
    ensure_preamble_longs_in(&[PREAMBLE_LONGS], preamble_longs)?;

    let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
    let config_word = cursor
        .read_u32_le()
        .map_err(insufficient_data("max_num_bins"))?;
    let gamma = cursor.read_f64_le().map_err(insufficient_data("gamma"))?;
    let index_offset = cursor
        .read_f64_le()
        .map_err(insufficient_data("index_offset"))?;

    if (config_word & CONFIG_RESERVED_MASK) != 0 {
        return Err(Error::deserial(format!(
            "invalid DDSketch header reserved config bits: {config_word:#010x}"
        )));
    }
    let encoded_weight_flag = config_word & CONFIG_FLOAT_WEIGHTS;
    if encoded_weight_flag != W::NATIVE_CONFIG_FLAG {
        let encoded_name = if encoded_weight_flag == CONFIG_FLOAT_WEIGHTS {
            "f64"
        } else {
            "u64"
        };
        return Err(Error::deserial(format!(
            "DDSketch native image encodes {encoded_name} weights, cannot deserialize into DdSketch<{}>",
            W::TYPE_NAME
        )));
    }
    let max_bins_u32 = config_word & CONFIG_MAX_BINS_MASK;

    let mapping_kind = match flags & MAPPING_MASK {
        MAPPING_TAG_LOGARITHMIC => MappingKind::Logarithmic,
        MAPPING_TAG_LINEAR => MappingKind::Linear,
        MAPPING_TAG_CUBIC => MappingKind::Cubic,
        other => {
            return Err(Error::deserial(format!(
                "invalid DDSketch mapping tag: {other}"
            )));
        }
    };
    let encoded_store_kind = decode_store_kind((flags >> STORE_SHIFT) & STORE_MASK, max_bins_u32)?;
    let mapping = IndexMapping::from_gamma_offset(mapping_kind, gamma, index_offset)?;

    let is_empty = (flags & FLAG_IS_EMPTY) != 0;
    let neg_collapsed = (flags & FLAG_NEG_COLLAPSED) != 0;
    let pos_collapsed = (flags & FLAG_POS_COLLAPSED) != 0;
    let has_exact_summary = (flags & FLAG_HAS_EXACT_SUMMARY) != 0;
    let encoded_max_bins = match encoded_store_kind {
        StoreKind::CollapsingLowestDense { max_num_bins }
        | StoreKind::CollapsingHighestDense { max_num_bins } => Some(max_num_bins),
        StoreKind::UnboundedDense | StoreKind::Sparse => None,
    };
    if encoded_max_bins.is_none() && (neg_collapsed || pos_collapsed) {
        return Err(Error::deserial(
            "collapsed flags cannot be set for non-collapsing DDSketch store",
        ));
    }

    let mut negative = Store::<W>::new(encoded_store_kind)?;
    let mut positive = Store::<W>::new(encoded_store_kind)?;

    if is_empty {
        if neg_collapsed || pos_collapsed || has_exact_summary {
            return Err(Error::deserial(
                "collapsed or summary flags cannot be set on an empty DDSketch",
            ));
        }
        let trailing = cursor.remaining().len();
        if trailing != 0 {
            return Err(Error::deserial(format!(
                "expected end of empty DDSketch image, found {trailing} trailing bytes"
            )));
        }
        return Ok(DdSketch::from_parts(
            mapping,
            negative,
            positive,
            W::ZERO,
            SummaryState::Empty,
        ));
    }

    let summary = if has_exact_summary {
        let min = cursor
            .read_f64_le()
            .map_err(insufficient_data("exact_min"))?;
        let max = cursor
            .read_f64_le()
            .map_err(insufficient_data("exact_max"))?;
        let sum = cursor
            .read_f64_le()
            .map_err(insufficient_data("exact_sum"))?;
        if !min.is_finite() || !max.is_finite() || min > max {
            return Err(Error::deserial(format!(
                "invalid DDSketch exact summary bounds: min={min}, max={max}"
            )));
        }
        SummaryState::Active {
            min: canonical_zero(min),
            max: canonical_zero(max),
            sum: canonical_zero(sum),
        }
    } else {
        SummaryState::Missing
    };

    let is_dense_target = !matches!(encoded_store_kind, StoreKind::Sparse);
    let zero_weight = W::decode_native(&mut cursor, "zero_weight")?;
    let (neg_weight, neg_span) = decode_native_store(
        &mut cursor,
        &mut negative,
        &mapping,
        is_dense_target,
        encoded_max_bins,
    )?;
    let (pos_weight, pos_span) = decode_native_store(
        &mut cursor,
        &mut positive,
        &mapping,
        is_dense_target,
        encoded_max_bins,
    )?;

    validate_encoded_sign_store(neg_weight, neg_span, neg_collapsed, encoded_max_bins)?;
    validate_encoded_sign_store(pos_weight, pos_span, pos_collapsed, encoded_max_bins)?;

    let total_weight = neg_weight
        .checked_add(zero_weight)
        .and_then(|acc| acc.checked_add(pos_weight))
        .ok_or_else(|| Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME)))?;
    if total_weight.is_zero() {
        return Err(Error::deserial(
            "non-empty DDSketch image must have positive total_weight",
        ));
    }
    validate_summary_against_stores::<W>(
        summary,
        !neg_weight.is_zero(),
        !zero_weight.is_zero(),
        !pos_weight.is_zero(),
        false,
    )?;

    let trailing = cursor.remaining().len();
    if trailing != 0 {
        return Err(Error::deserial(format!(
            "unexpected {trailing} trailing bytes after DDSketch payload"
        )));
    }

    if neg_collapsed {
        negative.set_collapsed(true);
    }
    if pos_collapsed {
        positive.set_collapsed(true);
    }

    Ok(DdSketch::from_parts(
        mapping,
        negative,
        positive,
        zero_weight,
        summary,
    ))
}

fn validate_summary_against_stores<W: DdSketchWeight>(
    summary: SummaryState,
    neg_non_empty: bool,
    zero_non_empty: bool,
    pos_non_empty: bool,
    allow_underflowed_stores: bool,
) -> Result<(), Error> {
    let SummaryState::Active { min, max, sum } = summary else {
        return Ok(());
    };
    if neg_non_empty && min >= 0.0 {
        return Err(Error::deserial(format!(
            "DDSketch exact_min ({min}) must be negative when negative store is non-empty"
        )));
    }
    if pos_non_empty && max <= 0.0 {
        return Err(Error::deserial(format!(
            "DDSketch exact_max ({max}) must be positive when positive store is non-empty"
        )));
    }
    // In `varfloat64` compatibility images with `f64` weights, positive weights `<= 2^-53` round to
    // `0.0` while exact summary extrema remain finite. Only enforce empty-sign bound exclusion
    // when weights cannot underflow to zero on the wire.
    if !allow_underflowed_stores {
        if !neg_non_empty && !zero_non_empty && min <= 0.0 {
            return Err(Error::deserial(format!(
                "DDSketch exact_min ({min}) must be positive when only positive store is non-empty"
            )));
        }
        if !pos_non_empty && !zero_non_empty && max >= 0.0 {
            return Err(Error::deserial(format!(
                "DDSketch exact_max ({max}) must be negative when only negative store is non-empty"
            )));
        }
    }
    if sum.is_finite() {
        let sum_consistent = if min == 0.0 && max == 0.0 {
            sum == 0.0
        } else if min >= 0.0 {
            if W::ALLOWS_SUBUNIT_SUM_UNDERFLOW {
                sum >= 0.0
            } else {
                sum > 0.0
            }
        } else if max <= 0.0 {
            if W::ALLOWS_SUBUNIT_SUM_UNDERFLOW {
                sum <= 0.0
            } else {
                sum < 0.0
            }
        } else {
            true
        };
        if !sum_consistent {
            return Err(Error::deserial(format!(
                "DDSketch exact_sum ({sum}) is inconsistent with exact bounds [{min}, {max}]"
            )));
        }
    }
    Ok(())
}

fn validate_encoded_sign_store<W: DdSketchWeight>(
    weight: W,
    wire_span: u64,
    collapsed: bool,
    encoded_max_bins: Option<usize>,
) -> Result<(), Error> {
    if collapsed && weight.is_zero() {
        return Err(Error::deserial(
            "collapsed flag set on an empty DDSketch sign store",
        ));
    }
    if let Some(max_bins) = encoded_max_bins {
        let max_bins_u64 = max_bins as u64;
        if wire_span > max_bins_u64 {
            return Err(Error::deserial(format!(
                "DDSketch sign store span ({wire_span}) exceeds encoded max_num_bins ({max_bins})"
            )));
        }
        if collapsed && wire_span != max_bins_u64 {
            return Err(Error::deserial(format!(
                "collapsed DDSketch sign store span ({wire_span}) must equal max_num_bins ({max_bins})"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum NativeBinChoice {
    Empty,
    ContiguousCounts { num_bins: u64, min_index: i32 },
    IndexDeltas { num_bins: u64 },
    IndexDeltasAndCounts { num_bins: u64 },
}

fn choose_native_bin_encoding<W: DdSketchWeight>(store: &Store<W>) -> (NativeBinChoice, usize) {
    if store.is_empty() {
        return (NativeBinChoice::Empty, 1);
    }

    let num_non_empty = store.num_occupied() as u64;
    let mut sparse_deltas_bytes = 1 + uvarint64_size(num_non_empty);
    let mut sparse_counts_bytes = 0_usize;
    let mut all_unit_weights = true;
    let mut prev_index = 0_i64;

    for (idx, weight) in store.iter_asc() {
        let idx64 = i64::from(idx);
        sparse_deltas_bytes += varint64_size(idx64 - prev_index);
        sparse_counts_bytes += weight.native_size();
        if !weight.is_one() {
            all_unit_weights = false;
        }
        prev_index = idx64;
    }

    let (best_sparse, best_sparse_bytes) = if all_unit_weights {
        (
            NativeBinChoice::IndexDeltas {
                num_bins: num_non_empty,
            },
            sparse_deltas_bytes,
        )
    } else {
        (
            NativeBinChoice::IndexDeltasAndCounts {
                num_bins: num_non_empty,
            },
            sparse_deltas_bytes + sparse_counts_bytes,
        )
    };

    if let Some((min_index, contiguous)) = store.dense_contiguous_slice() {
        let num_bins = contiguous.len() as u64;
        let num_zero_bins = contiguous.len() - store.num_occupied();
        let dense_bytes = 1
            + uvarint64_size(num_bins)
            + varint64_size(i64::from(min_index))
            + sparse_counts_bytes
            + num_zero_bins * W::NATIVE_ZERO_BYTES;
        if contiguous.len() > MAX_SPARSE_ENCODED_DENSE_SPAN || dense_bytes <= best_sparse_bytes {
            return (
                NativeBinChoice::ContiguousCounts {
                    num_bins,
                    min_index,
                },
                dense_bytes,
            );
        }
    }

    (best_sparse, best_sparse_bytes)
}

fn encode_native_store<W: DdSketchWeight>(
    out: &mut SketchBytes,
    store: &Store<W>,
    choice: NativeBinChoice,
) {
    match choice {
        NativeBinChoice::Empty => out.write_u8(NATIVE_STORE_EMPTY),
        NativeBinChoice::ContiguousCounts {
            num_bins,
            min_index,
        } => {
            let (_, slice) = store
                .dense_contiguous_slice()
                .expect("contiguous encoding only selected for non-empty dense store");
            out.write_u8(NATIVE_STORE_CONTIGUOUS_COUNTS);
            encode_uvarint64(out, num_bins);
            encode_varint64(out, i64::from(min_index));
            for &weight in slice {
                weight.encode_native(out);
            }
        }
        NativeBinChoice::IndexDeltas { num_bins } => {
            out.write_u8(NATIVE_STORE_INDEX_DELTAS);
            encode_uvarint64(out, num_bins);
            let mut prev = 0_i64;
            for (idx, _) in store.iter_asc() {
                let idx64 = i64::from(idx);
                encode_varint64(out, idx64 - prev);
                prev = idx64;
            }
        }
        NativeBinChoice::IndexDeltasAndCounts { num_bins } => {
            out.write_u8(NATIVE_STORE_DELTAS_AND_COUNTS);
            encode_uvarint64(out, num_bins);
            let mut prev = 0_i64;
            for (idx, weight) in store.iter_asc() {
                let idx64 = i64::from(idx);
                encode_varint64(out, idx64 - prev);
                weight.encode_native(out);
                prev = idx64;
            }
        }
    }
}

fn validate_mapping_bounds(mapping: &IndexMapping, lo: i32, hi: i32) -> Result<(), Error> {
    if !mapping.contains_valid_index(lo) || !mapping.contains_valid_index(hi) {
        return Err(Error::deserial(format!(
            "DDSketch bucket index range [{lo}, {hi}] is outside valid domain for mapping"
        )));
    }
    Ok(())
}

fn decode_native_store<W: DdSketchWeight>(
    cursor: &mut SketchSlice<'_>,
    store: &mut Store<W>,
    mapping: &IndexMapping,
    is_dense_target: bool,
    encoded_max_bins: Option<usize>,
) -> Result<(W, u64), Error> {
    let mode = cursor.read_u8().map_err(insufficient_data("store_mode"))?;
    let max_delta_dense_span = encoded_max_bins.map_or(MAX_SPARSE_ENCODED_DENSE_SPAN, |m| {
        m.min(MAX_SPARSE_ENCODED_DENSE_SPAN)
    }) as u64;
    match mode {
        NATIVE_STORE_EMPTY => Ok((W::ZERO, 0)),
        NATIVE_STORE_CONTIGUOUS_COUNTS => {
            let num_bins = decode_uvarint64(cursor, "num_bins")?;
            if num_bins == 0 {
                return Err(Error::deserial(
                    "contiguous DDSketch store block must have num_bins >= 1",
                ));
            }
            let first_index = decode_varint64(cursor, "first_index")?;
            if num_bins > (cursor.remaining().len() / W::NATIVE_MIN_BYTES) as u64 {
                return Err(Error::insufficient_data_of(
                    "contiguous_bins",
                    format!(
                        "declared {num_bins} bins exceeds remaining {} bytes",
                        cursor.remaining().len()
                    ),
                ));
            }
            validate_contiguous_bounds(first_index, num_bins, 1)?;
            let last_index = first_index + (num_bins as i64) - 1;
            validate_mapping_bounds(mapping, first_index as i32, last_index as i32)?;
            if let Some(max_bins) = encoded_max_bins {
                if num_bins > max_bins as u64 {
                    return Err(Error::deserial(format!(
                        "DDSketch contiguous store length ({num_bins}) exceeds encoded max_num_bins ({max_bins})"
                    )));
                }
            }
            if is_dense_target && num_bins > MAX_DENSE_LEN as u64 {
                return Err(Error::deserial(format!(
                    "DDSketch contiguous dense store length ({num_bins}) exceeds maximum dense capacity ({MAX_DENSE_LEN})"
                )));
            }
            let (sum, bounds) =
                store.decode_contiguous_unit_step(first_index as i32, num_bins as usize, || {
                    W::decode_native(cursor, "bin_weight")
                })?;
            let Some((lo, hi)) = bounds else {
                return Err(Error::deserial(
                    "non-empty DDSketch contiguous store block must contain positive weight",
                ));
            };
            Ok((sum, span_u64(lo, hi)))
        }
        NATIVE_STORE_INDEX_DELTAS => {
            let num_bins = decode_uvarint64(cursor, "num_bins")?;
            if num_bins == 0 {
                return Err(Error::deserial(
                    "DDSketch delta store block must have num_bins >= 1",
                ));
            }
            if num_bins > cursor.remaining().len() as u64 {
                return Err(Error::insufficient_data_of(
                    "store_bins",
                    format!(
                        "declared {num_bins} bins exceeds remaining {} bytes",
                        cursor.remaining().len()
                    ),
                ));
            }
            let max_valid_index = mapping.max_indexable_index();
            let mut first_index = 0_i64;
            let mut index = 0_i64;
            let mut sum = W::ZERO;
            for step in 0..num_bins {
                let delta = decode_varint64(cursor, "index_delta")?;
                if step > 0 && delta <= 0 {
                    return Err(Error::deserial(
                        "DDSketch delta store indices must be strictly increasing",
                    ));
                }
                index = index
                    .checked_add(delta)
                    .ok_or_else(|| Error::deserial("DDSketch bin index delta overflow"))?;
                let idx32 = i32::try_from(index).map_err(|_| {
                    Error::deserial(format!("DDSketch bin index {index} overflows 32-bit range"))
                })?;
                if step == 0 {
                    if !mapping.contains_valid_index(idx32) {
                        return Err(Error::deserial(format!(
                            "DDSketch bin index {idx32} is outside valid domain for mapping"
                        )));
                    }
                    first_index = index;
                } else if idx32 > max_valid_index {
                    return Err(Error::deserial(format!(
                        "DDSketch bin index {idx32} exceeds maximum indexable domain ({max_valid_index})"
                    )));
                }
                let wire_span = (index - first_index) as u64 + 1;
                if is_dense_target && wire_span > max_delta_dense_span {
                    return Err(Error::deserial(format!(
                        "DDSketch delta-encoded dense store span ({wire_span}) exceeds dense limit ({max_delta_dense_span})"
                    )));
                }
                sum = sum.checked_add(W::ONE).ok_or_else(|| {
                    Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME))
                })?;
                store.add(idx32, W::ONE);
            }
            let wire_span = (index - first_index) as u64 + 1;
            Ok((sum, wire_span))
        }
        NATIVE_STORE_DELTAS_AND_COUNTS => {
            let num_bins = decode_uvarint64(cursor, "num_bins")?;
            if num_bins == 0 {
                return Err(Error::deserial(
                    "DDSketch delta-and-count store block must have num_bins >= 1",
                ));
            }
            let min_entry_bytes = 1 + W::NATIVE_MIN_BYTES;
            if num_bins > (cursor.remaining().len() / min_entry_bytes) as u64 {
                return Err(Error::insufficient_data_of(
                    "store_bins",
                    format!(
                        "declared {num_bins} bins exceeds remaining {} bytes",
                        cursor.remaining().len()
                    ),
                ));
            }
            let max_valid_index = mapping.max_indexable_index();
            let mut first_index = 0_i64;
            let mut index = 0_i64;
            let mut sum = W::ZERO;
            for step in 0..num_bins {
                let delta = decode_varint64(cursor, "index_delta")?;
                if step > 0 && delta <= 0 {
                    return Err(Error::deserial(
                        "DDSketch delta store indices must be strictly increasing",
                    ));
                }
                let weight = W::decode_native(cursor, "bin_weight")?;
                if weight.is_zero() {
                    return Err(Error::deserial(
                        "DDSketch delta-and-count store block cannot contain zero weight",
                    ));
                }
                index = index
                    .checked_add(delta)
                    .ok_or_else(|| Error::deserial("DDSketch bin index delta overflow"))?;
                let idx32 = i32::try_from(index).map_err(|_| {
                    Error::deserial(format!("DDSketch bin index {index} overflows 32-bit range"))
                })?;
                if step == 0 {
                    if !mapping.contains_valid_index(idx32) {
                        return Err(Error::deserial(format!(
                            "DDSketch bin index {idx32} is outside valid domain for mapping"
                        )));
                    }
                    first_index = index;
                } else if idx32 > max_valid_index {
                    return Err(Error::deserial(format!(
                        "DDSketch bin index {idx32} exceeds maximum indexable domain ({max_valid_index})"
                    )));
                }
                let wire_span = (index - first_index) as u64 + 1;
                if is_dense_target && wire_span > max_delta_dense_span {
                    return Err(Error::deserial(format!(
                        "DDSketch delta-encoded dense store span ({wire_span}) exceeds dense limit ({max_delta_dense_span})"
                    )));
                }
                sum = sum.checked_add(weight).ok_or_else(|| {
                    Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME))
                })?;
                store.add(idx32, weight);
            }
            let wire_span = (index - first_index) as u64 + 1;
            Ok((sum, wire_span))
        }
        _ => Err(Error::deserial(format!(
            "unsupported DDSketch store encoding mode: {mode:#04x}"
        ))),
    }
}

#[derive(Clone, Copy)]
enum CompatBinChoice {
    ContiguousCounts { num_bins: u64, min_index: i32 },
    IndexDeltas { num_bins: u64 },
    IndexDeltasAndCounts { num_bins: u64 },
}

struct CompatBinPlan {
    choice: Option<CompatBinChoice>,
    bytes: usize,
    has_nonzero_weight: bool,
}

fn choose_compat_bin_encoding<W: DdSketchWeight>(store: &Store<W>) -> Result<CompatBinPlan, Error> {
    if store.is_empty() {
        return Ok(CompatBinPlan {
            choice: None,
            bytes: 0,
            has_nonzero_weight: false,
        });
    }

    let num_non_empty = store.num_occupied() as u64;
    let mut sparse_deltas_bytes = 1 + uvarint64_size(num_non_empty);
    let mut sparse_counts_bytes = 0_usize;
    let mut all_unit_weights = true;
    let mut has_nonzero_weight = false;
    let mut prev_index = 0_i64;

    for (idx, weight) in store.iter_asc() {
        weight.check_compat_encode("bucket weight")?;
        if weight.compat_encodes_nonzero() {
            has_nonzero_weight = true;
        }
        let idx64 = i64::from(idx);
        sparse_deltas_bytes += varint64_size(idx64 - prev_index);
        sparse_counts_bytes += weight.compat_size();
        if !weight.is_one() {
            all_unit_weights = false;
        }
        prev_index = idx64;
    }

    let (best_sparse, best_sparse_bytes) = if all_unit_weights {
        (
            CompatBinChoice::IndexDeltas {
                num_bins: num_non_empty,
            },
            sparse_deltas_bytes,
        )
    } else {
        (
            CompatBinChoice::IndexDeltasAndCounts {
                num_bins: num_non_empty,
            },
            sparse_deltas_bytes + sparse_counts_bytes,
        )
    };

    if let Some((min_index, contiguous)) = store.dense_contiguous_slice() {
        let num_bins = contiguous.len() as u64;
        let num_zero_bins = contiguous.len() - store.num_occupied();
        // `index_delta = 1` encodes as single byte `0x02`, and `0.0` encodes as single byte `0x00`.
        let dense_bytes = 1
            + uvarint64_size(num_bins)
            + varint64_size(i64::from(min_index))
            + 1
            + sparse_counts_bytes
            + num_zero_bins;
        if dense_bytes <= best_sparse_bytes {
            return Ok(CompatBinPlan {
                choice: Some(CompatBinChoice::ContiguousCounts {
                    num_bins,
                    min_index,
                }),
                bytes: dense_bytes,
                has_nonzero_weight,
            });
        }
    }

    Ok(CompatBinPlan {
        choice: Some(best_sparse),
        bytes: best_sparse_bytes,
        has_nonzero_weight,
    })
}

fn encode_compat_store<W: DdSketchWeight>(
    out: &mut SketchBytes,
    store: &Store<W>,
    flag_type: u8,
    choice: CompatBinChoice,
) {
    match choice {
        CompatBinChoice::ContiguousCounts {
            num_bins,
            min_index,
        } => {
            let (_, slice) = store
                .dense_contiguous_slice()
                .expect("contiguous encoding only selected for non-empty dense store");
            out.write_u8(DD_SUBFLAG_BINS_CONTIGUOUS_COUNTS | flag_type);
            encode_uvarint64(out, num_bins);
            encode_varint64(out, i64::from(min_index));
            encode_varint64(out, 1);
            for &weight in slice {
                weight.encode_compat(out);
            }
        }
        CompatBinChoice::IndexDeltas { num_bins } => {
            out.write_u8(DD_SUBFLAG_BINS_INDEX_DELTAS | flag_type);
            encode_uvarint64(out, num_bins);
            let mut prev = 0_i64;
            for (idx, _) in store.iter_asc() {
                let idx64 = i64::from(idx);
                encode_varint64(out, idx64 - prev);
                prev = idx64;
            }
        }
        CompatBinChoice::IndexDeltasAndCounts { num_bins } => {
            out.write_u8(DD_SUBFLAG_BINS_DELTAS_AND_COUNTS | flag_type);
            encode_uvarint64(out, num_bins);
            let mut prev = 0_i64;
            for (idx, weight) in store.iter_asc() {
                let idx64 = i64::from(idx);
                encode_varint64(out, idx64 - prev);
                weight.encode_compat(out);
                prev = idx64;
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Sign {
    Negative,
    Positive,
}

struct DatadogScan<W> {
    mapping: IndexMapping,
    zero_weight: W,
    summary: SummaryState,
    neg_bounds: Option<(i32, i32)>,
    pos_bounds: Option<(i32, i32)>,
}

fn deserialize_datadog<W: DdSketchWeight>(
    bytes: &[u8],
    store_kind: StoreKind,
) -> Result<DdSketch<W>, Error> {
    let enforce_span_limit = matches!(store_kind, StoreKind::UnboundedDense);
    let scan = scan_datadog_stream::<W>(bytes, enforce_span_limit, |_, _, _| {})?;

    let mut negative = Store::<W>::with_wire_bounds(store_kind, scan.neg_bounds)?;
    let mut positive = Store::<W>::with_wire_bounds(store_kind, scan.pos_bounds)?;
    if scan.neg_bounds.is_some() || scan.pos_bounds.is_some() {
        scan_datadog_stream::<W>(bytes, false, |sign, idx, count| match sign {
            Sign::Negative => negative.add_in_bounds(idx, count),
            Sign::Positive => positive.add_in_bounds(idx, count),
        })?;
    }

    // Re-check combined weight after bin accumulation so `f64` store-order rounding near
    // `f64::MAX` can never leave `total_weight()` non-finite.
    negative
        .total_weight()
        .checked_add(scan.zero_weight)
        .and_then(|acc| acc.checked_add(positive.total_weight()))
        .ok_or_else(|| Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME)))?;

    Ok(DdSketch::from_parts(
        scan.mapping,
        negative,
        positive,
        scan.zero_weight,
        scan.summary,
    ))
}

fn scan_datadog_stream<W: DdSketchWeight>(
    bytes: &[u8],
    enforce_span_limit: bool,
    mut on_bin: impl FnMut(Sign, i32, W),
) -> Result<DatadogScan<W>, Error> {
    let mut pos_range = IndexRangeTracker::new(enforce_span_limit);
    let mut neg_range = IndexRangeTracker::new(enforce_span_limit);
    let mut mapping: Option<IndexMapping> = None;
    let mut zero_weight = W::ZERO;
    let mut total_weight = W::ZERO;
    let mut saw_count = false;
    let mut saw_sum = false;
    let mut saw_min = false;
    let mut saw_max = false;
    let mut summary_count = W::ZERO;
    let mut summary_sum = 0.0_f64;
    let mut summary_min = f64::INFINITY;
    let mut summary_max = f64::NEG_INFINITY;

    let mut cursor = SketchSlice::new(bytes);
    while !cursor.remaining().is_empty() {
        let flag = cursor.read_u8().map_err(insufficient_data("flag"))?;
        let flag_type = flag & DD_FLAG_TYPE_MASK;
        let subflag = flag & !DD_FLAG_TYPE_MASK;

        match flag_type {
            DD_FLAG_TYPE_INDEX_MAPPING => {
                let kind = match subflag {
                    DD_SUBFLAG_MAPPING_LOGARITHMIC => MappingKind::Logarithmic,
                    DD_SUBFLAG_MAPPING_LINEAR => MappingKind::Linear,
                    DD_SUBFLAG_MAPPING_CUBIC => MappingKind::Cubic,
                    _ => {
                        return Err(Error::deserial(format!(
                            "unsupported reference DDSketch index mapping flag: {flag:#04x}"
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
                            "conflicting index mapping blocks in reference DDSketch",
                        ));
                    }
                } else {
                    mapping = Some(decoded);
                }
            }
            DD_FLAG_TYPE_POSITIVE_STORE => {
                scan_datadog_store_block(
                    &mut cursor,
                    subflag,
                    Sign::Positive,
                    &mut pos_range,
                    &mut total_weight,
                    &mut on_bin,
                )?;
            }
            DD_FLAG_TYPE_NEGATIVE_STORE => {
                scan_datadog_store_block(
                    &mut cursor,
                    subflag,
                    Sign::Negative,
                    &mut neg_range,
                    &mut total_weight,
                    &mut on_bin,
                )?;
            }
            DD_FLAG_TYPE_SKETCH_FEATURES => match subflag {
                DD_SUBFLAG_ZERO_COUNT => {
                    let count = W::decode_compat(&mut cursor, "zero_count")?;
                    zero_weight = zero_weight.checked_add(count).ok_or_else(|| {
                        Error::deserial(format!("zero_count overflows {}", W::TYPE_NAME))
                    })?;
                    total_weight = total_weight.checked_add(count).ok_or_else(|| {
                        Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME))
                    })?;
                }
                DD_SUBFLAG_EXACT_COUNT => {
                    let count = W::decode_compat(&mut cursor, "exact_count")?;
                    summary_count = summary_count.checked_add(count).ok_or_else(|| {
                        Error::deserial(format!("exact_count overflows {}", W::TYPE_NAME))
                    })?;
                    saw_count = true;
                }
                DD_SUBFLAG_EXACT_SUM => {
                    let v = cursor
                        .read_f64_le()
                        .map_err(insufficient_data("exact_sum"))?;
                    summary_sum += v;
                    saw_sum = true;
                }
                DD_SUBFLAG_EXACT_MIN => {
                    let v = cursor
                        .read_f64_le()
                        .map_err(insufficient_data("exact_min"))?;
                    if !v.is_finite() {
                        return Err(Error::deserial(format!(
                            "invalid exact_min summary statistic: {v}"
                        )));
                    }
                    summary_min = summary_min.min(canonical_zero(v));
                    saw_min = true;
                }
                DD_SUBFLAG_EXACT_MAX => {
                    let v = cursor
                        .read_f64_le()
                        .map_err(insufficient_data("exact_max"))?;
                    if !v.is_finite() {
                        return Err(Error::deserial(format!(
                            "invalid exact_max summary statistic: {v}"
                        )));
                    }
                    summary_max = summary_max.max(canonical_zero(v));
                    saw_max = true;
                }
                _ => {
                    return Err(Error::deserial(format!(
                        "unsupported reference DDSketch feature flag: {flag:#04x}"
                    )));
                }
            },
            _ => unreachable!("2-bit mask only produces 0..=3"),
        }
    }

    let Some(mapping) = mapping else {
        return Err(Error::deserial(
            "missing index mapping in reference DDSketch",
        ));
    };

    if let Some((lo, hi)) = neg_range.bounds {
        validate_mapping_bounds(&mapping, lo, hi)?;
    }
    if let Some((lo, hi)) = pos_range.bounds {
        validate_mapping_bounds(&mapping, lo, hi)?;
    }
    if enforce_span_limit {
        let neg_span = neg_range.bounds.map_or(0, |(lo, hi)| span_u64(lo, hi));
        let pos_span = pos_range.bounds.map_or(0, |(lo, hi)| span_u64(lo, hi));
        let combined_span = neg_span + pos_span;
        if combined_span > MAX_DENSE_BINS as u64 {
            return Err(Error::deserial(format!(
                "decoded DDSketch combined index span ({combined_span}) exceeds maximum dense limit ({MAX_DENSE_BINS})"
            )));
        }
    }

    let saw_any_summary = saw_count || saw_sum || saw_min || saw_max;
    let summary = if total_weight.is_zero() {
        if !summary_count.is_zero() || saw_min || saw_max || (saw_sum && summary_sum != 0.0) {
            return Err(Error::deserial(
                "non-empty exact summary statistics on an empty reference DDSketch",
            ));
        }
        SummaryState::Empty
    } else if !saw_any_summary {
        SummaryState::Missing
    } else {
        W::validate_compat_summary_count(saw_count, summary_count, total_weight)?;
        if !saw_min || !saw_max || summary_min > summary_max {
            return Err(Error::deserial(format!(
                "invalid reference DDSketch exact min/max summary statistics: min={summary_min}, max={summary_max}"
            )));
        }
        let active = SummaryState::Active {
            min: summary_min,
            max: summary_max,
            sum: canonical_zero(summary_sum),
        };
        validate_summary_against_stores::<W>(
            active,
            neg_range.bounds.is_some(),
            !zero_weight.is_zero(),
            pos_range.bounds.is_some(),
            W::COMPAT_WEIGHTS_CAN_UNDERFLOW,
        )?;
        active
    };

    Ok(DatadogScan {
        mapping,
        zero_weight,
        summary,
        neg_bounds: neg_range.bounds,
        pos_bounds: pos_range.bounds,
    })
}

struct IndexRangeTracker {
    enforce_span_limit: bool,
    bounds: Option<(i32, i32)>,
}

impl IndexRangeTracker {
    const fn new(enforce_span_limit: bool) -> Self {
        Self {
            enforce_span_limit,
            bounds: None,
        }
    }

    fn accept(&mut self, index: i64) -> Result<i32, Error> {
        let idx32 = i32::try_from(index).map_err(|_| {
            Error::deserial(format!("DDSketch bin index {index} overflows 32-bit range"))
        })?;
        let (lo, hi) = match self.bounds {
            None => (idx32, idx32),
            Some((lo, hi)) => (lo.min(idx32), hi.max(idx32)),
        };
        if self.enforce_span_limit {
            let span = span_u64(lo, hi);
            if span > MAX_DENSE_BINS as u64 {
                return Err(Error::deserial(format!(
                    "decoded DDSketch index span ({span}) exceeds maximum dense limit ({MAX_DENSE_BINS})"
                )));
            }
        }
        self.bounds = Some((lo, hi));
        Ok(idx32)
    }
}

fn scan_datadog_store_block<W: DdSketchWeight>(
    cursor: &mut SketchSlice<'_>,
    subflag: u8,
    sign: Sign,
    tracker: &mut IndexRangeTracker,
    total_weight: &mut W,
    on_bin: &mut impl FnMut(Sign, i32, W),
) -> Result<(), Error> {
    match subflag {
        DD_SUBFLAG_BINS_DELTAS_AND_COUNTS => {
            let num_bins = decode_uvarint64(cursor, "num_bins")?;
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
                let count = W::decode_compat(cursor, "bin_count")?;
                index = index
                    .checked_add(delta)
                    .ok_or_else(|| Error::deserial("DDSketch bin index delta overflow"))?;
                let idx32 = i32::try_from(index).map_err(|_| {
                    Error::deserial(format!("DDSketch bin index {index} overflows 32-bit range"))
                })?;
                if !count.is_zero() {
                    tracker.accept(index)?;
                    *total_weight = total_weight.checked_add(count).ok_or_else(|| {
                        Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME))
                    })?;
                    on_bin(sign, idx32, count);
                }
            }
        }
        DD_SUBFLAG_BINS_INDEX_DELTAS => {
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
                *total_weight = total_weight.checked_add(W::ONE).ok_or_else(|| {
                    Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME))
                })?;
                on_bin(sign, idx32, W::ONE);
            }
        }
        DD_SUBFLAG_BINS_CONTIGUOUS_COUNTS => {
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
                let count = W::decode_compat(cursor, "bin_count")?;
                if !count.is_zero() {
                    let idx32 = tracker.accept(index)?;
                    *total_weight = total_weight.checked_add(count).ok_or_else(|| {
                        Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME))
                    })?;
                    on_bin(sign, idx32, count);
                }
                if step + 1 < num_bins {
                    index += index_delta;
                }
            }
        }
        _ => {
            return Err(Error::deserial(format!(
                "unsupported reference DDSketch bin encoding mode: {:#04x}",
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
    let bits = (v + 1.0).to_bits().wrapping_sub(ONE_F64_BITS);
    let mut x = bits.rotate_left(VARFLOAT_ROTATE);
    for _ in 0..(MAX_VAR_LEN_64 - 1) {
        let byte = (x >> 57) as u8;
        x <<= 7;
        if x == 0 {
            out.write_u8(byte);
            return;
        }
        out.write_u8(byte | 0x80);
    }
    out.write_u8((x >> 56) as u8);
}

#[inline]
pub fn varfloat64_size(v: f64) -> usize {
    let bits = (v + 1.0).to_bits().wrapping_sub(ONE_F64_BITS);
    let x = bits.rotate_left(VARFLOAT_ROTATE);
    if x == 0 {
        return 1;
    }
    let tz = x.trailing_zeros() as usize;
    if tz < 8 {
        MAX_VAR_LEN_64
    } else {
        (64 - tz).div_ceil(7)
    }
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
    fn datadog_varfloat64_reference_vectors_decode() {
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
