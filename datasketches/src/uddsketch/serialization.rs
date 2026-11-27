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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::uddsketch::MAX_COLLAPSES;
use crate::uddsketch::MAX_MAX_BUCKETS;
use crate::uddsketch::MIN_ALPHA;
use crate::uddsketch::MIN_MAX_BUCKETS;
use crate::uddsketch::mapping::Mapping;
use crate::uddsketch::sketch::UddSketch;
use crate::uddsketch::store::Bucket;
use crate::uddsketch::store::BucketStore;

const SERIAL_VERSION: u8 = 1;
const PREAMBLE_LONGS_EMPTY: u8 = 2;
const PREAMBLE_LONGS_SINGLE: u8 = 4;
const PREAMBLE_LONGS_NON_EMPTY: u8 = 7;
const FLAGS_IS_EMPTY: u8 = 1 << 2;
const FLAGS_SINGLE_VALUE: u8 = 1 << 3;
pub(super) const BUCKET_BYTES: usize = size_of::<i64>() + size_of::<u64>();

pub(super) struct ValidatedSketchImage<'a> {
    pub(super) max_buckets: u32,
    pub(super) mapping: Mapping,
    pub(super) num_collapses: u8,
    pub(super) total_weight: u64,
    pub(super) zero_weight: u64,
    pub(super) min: f64,
    pub(super) max: f64,
    pub(super) neg_bytes: &'a [u8],
    pub(super) pos_bytes: &'a [u8],
    pub(super) neg_weight: u64,
    pub(super) pos_weight: u64,
}

#[inline]
pub(super) fn decode_bucket(chunk: &[u8]) -> Bucket {
    debug_assert_eq!(chunk.len(), BUCKET_BYTES);
    Bucket {
        key: i64::from_le_bytes(chunk[..8].try_into().unwrap()),
        count: u64::from_le_bytes(chunk[8..16].try_into().unwrap()),
    }
}

pub fn serialize_sketch(sketch: &UddSketch) -> Vec<u8> {
    let is_empty = sketch.is_empty();
    let is_single = !is_empty && sketch.min_value() == sketch.max_value();
    let preamble_longs = if is_empty {
        PREAMBLE_LONGS_EMPTY
    } else if is_single {
        PREAMBLE_LONGS_SINGLE
    } else {
        PREAMBLE_LONGS_NON_EMPTY
    };
    let header_bytes = usize::from(preamble_longs) * size_of::<u64>();
    let payload_bytes = if is_empty || is_single {
        0
    } else {
        sketch.num_retained() * BUCKET_BYTES
    };
    let flags = if is_empty {
        FLAGS_IS_EMPTY
    } else if is_single {
        FLAGS_SINGLE_VALUE
    } else {
        0
    };

    let mut bytes = SketchBytes::with_capacity(header_bytes + payload_bytes);
    bytes.write_u8(preamble_longs);
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(Family::UDDSKETCH.id);
    bytes.write_u8(flags);
    bytes.write_u32_le(sketch.max_buckets());
    bytes.write_f64_le(sketch.initial_alpha());

    if is_empty {
        return bytes.into_bytes();
    }
    if is_single {
        bytes.write_u64_le(sketch.total_weight());
        bytes.write_f64_le(sketch.min_value().unwrap());
        return bytes.into_bytes();
    }

    let num_neg =
        u32::try_from(sketch.neg_store().len()).expect("negative bucket count exceeds u32::MAX");
    let num_pos =
        u32::try_from(sketch.pos_store().len()).expect("positive bucket count exceeds u32::MAX");

    bytes.write_u8(sketch.num_collapses());
    bytes.write(&[0u8; 7]);
    bytes.write_u32_le(num_neg);
    bytes.write_u32_le(num_pos);
    bytes.write_u64_le(sketch.zero_weight());
    bytes.write_f64_le(sketch.min_value().unwrap());
    bytes.write_f64_le(sketch.max_value().unwrap());

    for bucket in sketch.neg_store().sorted_buckets() {
        bytes.write_i64_le(bucket.key);
        bytes.write_u64_le(bucket.count);
    }
    for bucket in sketch.pos_store().sorted_buckets() {
        bytes.write_i64_le(bucket.key);
        bytes.write_u64_le(bucket.count);
    }

    bytes.into_bytes()
}

pub fn deserialize_sketch(bytes: &[u8]) -> Result<UddSketch, Error> {
    let image = validate_sketch_image(bytes)?;
    if image.total_weight == 0 {
        return Ok(UddSketch::from_parts(
            image.max_buckets,
            image.mapping,
            0,
            0,
            0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            BucketStore::default(),
            BucketStore::default(),
        ));
    }

    if image.min == image.max {
        let mut neg = BucketStore::default();
        let mut pos = BucketStore::default();
        if image.zero_weight == 0 {
            let bucket = Bucket {
                key: image.mapping.key_at_level(image.min.abs(), 0),
                count: image.total_weight,
            };
            let store = if image.min < 0.0 { &mut neg } else { &mut pos };
            *store = BucketStore::from_sorted_unchecked(vec![bucket], image.total_weight);
        }
        return Ok(UddSketch::from_parts(
            image.max_buckets,
            image.mapping,
            0,
            image.total_weight,
            image.zero_weight,
            image.min,
            image.max,
            neg,
            pos,
        ));
    }

    let neg_entries: Vec<Bucket> = image
        .neg_bytes
        .chunks_exact(BUCKET_BYTES)
        .map(decode_bucket)
        .collect();
    let pos_entries: Vec<Bucket> = image
        .pos_bytes
        .chunks_exact(BUCKET_BYTES)
        .map(decode_bucket)
        .collect();

    Ok(UddSketch::from_parts(
        image.max_buckets,
        image.mapping,
        image.num_collapses,
        image.total_weight,
        image.zero_weight,
        image.min,
        image.max,
        BucketStore::from_sorted_unchecked(neg_entries, image.neg_weight),
        BucketStore::from_sorted_unchecked(pos_entries, image.pos_weight),
    ))
}

pub(super) fn validate_sketch_image(bytes: &[u8]) -> Result<ValidatedSketchImage<'_>, Error> {
    let mut cursor = SketchSlice::new(bytes);

    let preamble_longs = cursor
        .read_u8()
        .map_err(insufficient_data("preamble_longs"))?;
    let serial_version = cursor
        .read_u8()
        .map_err(insufficient_data("serial_version"))?;
    let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
    let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
    let max_buckets = cursor
        .read_u32_le()
        .map_err(insufficient_data("max_buckets"))?;
    let initial_alpha = cursor
        .read_f64_le()
        .map_err(insufficient_data("initial_alpha"))?;

    Family::UDDSKETCH.validate_id(family_id)?;
    ensure_serial_version_is(SERIAL_VERSION, serial_version)?;

    if (flags & !(FLAGS_IS_EMPTY | FLAGS_SINGLE_VALUE)) != 0 {
        return Err(Error::deserial(format!(
            "unsupported UDDSketch flags: {flags:#x}"
        )));
    }
    let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
    let is_single = (flags & FLAGS_SINGLE_VALUE) != 0;
    if is_empty && is_single {
        return Err(Error::deserial(
            "conflicting UDDSketch flags: empty and single-value",
        ));
    }
    let expected_preamble_longs = if is_empty {
        PREAMBLE_LONGS_EMPTY
    } else if is_single {
        PREAMBLE_LONGS_SINGLE
    } else {
        PREAMBLE_LONGS_NON_EMPTY
    };
    if preamble_longs != expected_preamble_longs {
        return Err(Error::invalid_preamble_longs(
            &[expected_preamble_longs],
            preamble_longs,
        ));
    }

    if !(MIN_MAX_BUCKETS..=MAX_MAX_BUCKETS).contains(&max_buckets) {
        return Err(Error::deserial(format!(
            "max_buckets must be in [{MIN_MAX_BUCKETS}, {MAX_MAX_BUCKETS}], got {max_buckets}"
        )));
    }
    if !(MIN_ALPHA..1.0).contains(&initial_alpha) {
        return Err(Error::deserial(format!(
            "initial_alpha must be in [{MIN_ALPHA}, 1.0), got {initial_alpha}"
        )));
    }

    let mapping = Mapping::new(initial_alpha);

    if is_empty {
        let trailing = cursor.remaining().len();
        if trailing != 0 {
            return Err(Error::deserial(format!(
                "expected end of empty UDDSketch image, found {trailing} trailing bytes"
            )));
        }
        return Ok(ValidatedSketchImage {
            max_buckets,
            mapping,
            num_collapses: 0,
            total_weight: 0,
            zero_weight: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            neg_bytes: &[],
            pos_bytes: &[],
            neg_weight: 0,
            pos_weight: 0,
        });
    }

    if is_single {
        let total_weight = cursor
            .read_u64_le()
            .map_err(insufficient_data("total_weight"))?;
        let value = cursor.read_f64_le().map_err(insufficient_data("value"))?;
        let trailing = cursor.remaining().len();
        if trailing != 0 {
            return Err(Error::deserial(format!(
                "expected end of single-value UDDSketch image, found {trailing} trailing bytes"
            )));
        }
        if total_weight == 0 {
            return Err(Error::deserial(
                "single-value UDDSketch must have total_weight > 0",
            ));
        }
        if !value.is_finite() || value.to_bits() == (-0.0f64).to_bits() {
            return Err(Error::deserial("invalid UDDSketch single value"));
        }
        return Ok(ValidatedSketchImage {
            max_buckets,
            mapping,
            num_collapses: 0,
            total_weight,
            zero_weight: if value == 0.0 { total_weight } else { 0 },
            min: value,
            max: value,
            neg_bytes: &[],
            pos_bytes: &[],
            neg_weight: if value < 0.0 { total_weight } else { 0 },
            pos_weight: if value > 0.0 { total_weight } else { 0 },
        });
    }

    let num_collapses = cursor
        .read_u8()
        .map_err(insufficient_data("num_collapses"))?;
    let mut reserved = [0u8; 7];
    cursor
        .read_exact(&mut reserved)
        .map_err(insufficient_data("reserved"))?;
    let num_neg = cursor
        .read_u32_le()
        .map_err(insufficient_data("num_neg_buckets"))?;
    let num_pos = cursor
        .read_u32_le()
        .map_err(insufficient_data("num_pos_buckets"))?;
    let zero_weight = cursor
        .read_u64_le()
        .map_err(insufficient_data("zero_weight"))?;
    let min_value = cursor
        .read_f64_le()
        .map_err(insufficient_data("min_value"))?;
    let max_value = cursor
        .read_f64_le()
        .map_err(insufficient_data("max_value"))?;

    if reserved != [0u8; 7] {
        return Err(Error::deserial(
            "non-zero reserved bytes in UDDSketch header",
        ));
    }
    if num_collapses > MAX_COLLAPSES {
        return Err(Error::deserial(format!(
            "num_collapses must be at most {MAX_COLLAPSES}, got {num_collapses}"
        )));
    }

    let total_buckets = u64::from(num_neg) + u64::from(num_pos);
    if total_buckets > u64::from(max_buckets) {
        return Err(Error::deserial(format!(
            "retained bucket count ({total_buckets}) exceeds max_buckets ({max_buckets})"
        )));
    }
    let min_collapsed_buckets = u64::from(max_buckets) / 2 + 1;
    if num_collapses > 0 && total_buckets < min_collapsed_buckets {
        return Err(Error::deserial(format!(
            "UDDSketch with num_collapses={num_collapses} must retain at least {min_collapsed_buckets} buckets, got {total_buckets}"
        )));
    }

    if !min_value.is_finite()
        || !max_value.is_finite()
        || min_value > max_value
        || min_value.to_bits() == (-0.0f64).to_bits()
        || max_value.to_bits() == (-0.0f64).to_bits()
    {
        return Err(Error::deserial("invalid UDDSketch extrema"));
    }
    if min_value == max_value {
        return Err(Error::deserial(
            "non-canonical UDDSketch: constant stream must use single-value encoding",
        ));
    }

    let total_buckets_usize = usize::try_from(total_buckets)
        .map_err(|_| Error::deserial("bucket count exceeds platform usize::MAX"))?;
    let expected_payload_bytes = total_buckets_usize
        .checked_mul(BUCKET_BYTES)
        .ok_or_else(|| Error::deserial("bucket payload size overflows usize"))?;
    let remaining_bytes = cursor.remaining().len();
    if remaining_bytes < expected_payload_bytes {
        return Err(Error::insufficient_data_of(
            "UDDSketch buckets",
            format_args!("expected {expected_payload_bytes} bytes, got {remaining_bytes}"),
        ));
    }
    if remaining_bytes > expected_payload_bytes {
        return Err(Error::deserial(format!(
            "expected {expected_payload_bytes} bucket bytes, found {} trailing bytes",
            remaining_bytes - expected_payload_bytes
        )));
    }

    let payload = &bytes[bytes.len() - remaining_bytes..];
    let (neg_bytes, pos_bytes) = payload.split_at(num_neg as usize * BUCKET_BYTES);
    let (min_key, max_key) = mapping.key_bounds(num_collapses);
    let neg_weight = validate_bucket_run(neg_bytes, min_key, max_key)?;
    let pos_weight = validate_bucket_run(pos_bytes, min_key, max_key)?;

    let total_weight = neg_weight
        .checked_add(zero_weight)
        .and_then(|acc| acc.checked_add(pos_weight))
        .ok_or_else(|| Error::deserial("UDDSketch total weight overflow"))?;
    if total_weight == 0 {
        return Err(Error::deserial(
            "non-empty UDDSketch must have total_weight > 0",
        ));
    }
    if total_weight == 1 && min_value != max_value {
        return Err(Error::deserial(
            "single-item UDDSketch must have min_value == max_value",
        ));
    }

    // Validate that stream extrema fall in the outermost occupied buckets or zero bucket.
    if let Some(largest_neg) = neg_bytes.last_chunk::<BUCKET_BYTES>() {
        if !(min_value < 0.0
            && mapping.key_at_level(-min_value, num_collapses) == decode_bucket(largest_neg).key)
        {
            return Err(Error::deserial(
                "min_value does not match outermost negative bucket",
            ));
        }
    } else if zero_weight > 0 {
        if min_value != 0.0 {
            return Err(Error::deserial(
                "min_value must be 0.0 when zero is minimum",
            ));
        }
    } else if !(min_value > 0.0
        && mapping.key_at_level(min_value, num_collapses)
            == decode_bucket(&pos_bytes[..BUCKET_BYTES]).key)
    {
        return Err(Error::deserial(
            "min_value does not match innermost positive bucket",
        ));
    }

    if let Some(largest_pos) = pos_bytes.last_chunk::<BUCKET_BYTES>() {
        if !(max_value > 0.0
            && mapping.key_at_level(max_value, num_collapses) == decode_bucket(largest_pos).key)
        {
            return Err(Error::deserial(
                "max_value does not match outermost positive bucket",
            ));
        }
    } else if zero_weight > 0 {
        if max_value != 0.0 {
            return Err(Error::deserial(
                "max_value must be 0.0 when zero is maximum",
            ));
        }
    } else if !(max_value < 0.0
        && mapping.key_at_level(-max_value, num_collapses)
            == decode_bucket(&neg_bytes[..BUCKET_BYTES]).key)
    {
        return Err(Error::deserial(
            "max_value does not match innermost negative bucket",
        ));
    }

    if num_collapses > 0 {
        let prev_level = num_collapses - 1;
        let (prev_min_key, _) = mapping.key_bounds(prev_level);
        let neg_prev = if neg_bytes.is_empty() {
            0
        } else {
            let neg_lo = if max_value < 0.0 {
                mapping.key_at_level(-max_value, prev_level)
            } else {
                prev_min_key
            };
            let neg_hi = mapping.key_at_level(-min_value, prev_level);
            count_reachable_prev_buckets(neg_bytes, neg_lo, neg_hi)
        };
        let pos_prev = if pos_bytes.is_empty() {
            0
        } else {
            let pos_lo = if min_value > 0.0 {
                mapping.key_at_level(min_value, prev_level)
            } else {
                prev_min_key
            };
            let pos_hi = mapping.key_at_level(max_value, prev_level);
            count_reachable_prev_buckets(pos_bytes, pos_lo, pos_hi)
        };
        let max_prev_buckets = neg_prev + pos_prev;
        if max_prev_buckets <= u64::from(max_buckets) {
            return Err(Error::deserial(format!(
                "UDDSketch with num_collapses={num_collapses} could have occupied at most {max_prev_buckets} buckets before its last collapse, within max_buckets={max_buckets}"
            )));
        }
    }

    Ok(ValidatedSketchImage {
        max_buckets,
        mapping,
        num_collapses,
        total_weight,
        zero_weight,
        min: min_value,
        max: max_value,
        neg_bytes,
        pos_bytes,
        neg_weight,
        pos_weight,
    })
}

fn validate_bucket_run(bytes: &[u8], min_key: i64, max_key: i64) -> Result<u64, Error> {
    let mut total_weight = 0u64;
    let mut prev_key: Option<i64> = None;

    for chunk in bytes.chunks_exact(BUCKET_BYTES) {
        let Bucket { key, count } = decode_bucket(chunk);
        if count == 0 {
            return Err(Error::deserial("UDDSketch bucket count must be positive"));
        }
        if key < min_key || key > max_key {
            return Err(Error::deserial(format!(
                "UDDSketch bucket key {key} outside valid range [{min_key}, {max_key}]"
            )));
        }
        if prev_key.is_some_and(|prev| prev >= key) {
            return Err(Error::deserial(
                "UDDSketch bucket keys must be strictly ascending",
            ));
        }
        total_weight = total_weight
            .checked_add(count)
            .ok_or_else(|| Error::deserial("UDDSketch bucket weight overflow"))?;
        prev_key = Some(key);
    }

    Ok(total_weight)
}

fn count_reachable_prev_buckets(bytes: &[u8], prev_min_key: i64, prev_max_key: i64) -> u64 {
    bytes
        .chunks_exact(BUCKET_BYTES)
        .map(decode_bucket)
        .map(|bucket| {
            let hi_child = bucket.key.saturating_mul(2);
            let lo_child = hi_child - 1;
            let reachable = u64::from((prev_min_key..=prev_max_key).contains(&lo_child))
                + u64::from((prev_min_key..=prev_max_key).contains(&hi_child));
            bucket.count.min(reachable)
        })
        .sum()
}
