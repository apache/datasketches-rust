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
const PREAMBLE_LONGS_NON_EMPTY: u8 = 7;
const FLAGS_IS_EMPTY: u8 = 1 << 2;
const BUCKET_BYTES: usize = size_of::<i64>() + size_of::<u64>();

pub fn serialize_sketch(sketch: &UddSketch) -> Vec<u8> {
    let is_empty = sketch.is_empty();
    let preamble_longs = if is_empty {
        PREAMBLE_LONGS_EMPTY
    } else {
        PREAMBLE_LONGS_NON_EMPTY
    };
    let header_bytes = usize::from(preamble_longs) * size_of::<u64>();
    let payload_bytes = if is_empty {
        0
    } else {
        sketch.num_retained() * BUCKET_BYTES
    };

    let mut bytes = SketchBytes::with_capacity(header_bytes + payload_bytes);
    bytes.write_u8(preamble_longs);
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(Family::UDDSKETCH.id);
    bytes.write_u8(if is_empty { FLAGS_IS_EMPTY } else { 0 });
    bytes.write_u32_le(sketch.max_buckets());
    bytes.write_f64_le(sketch.initial_alpha());

    if is_empty {
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

    if (flags & !FLAGS_IS_EMPTY) != 0 {
        return Err(Error::deserial(format!(
            "unsupported UDDSketch flags: {flags:#x}"
        )));
    }
    let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
    let expected_preamble_longs = if is_empty {
        PREAMBLE_LONGS_EMPTY
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
        return Ok(UddSketch::from_parts(
            max_buckets,
            mapping,
            0,
            0,
            0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            BucketStore::default(),
            BucketStore::default(),
        ));
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

    let (min_key, max_key) = mapping.key_bounds(num_collapses);
    let (neg_entries, neg_weight) =
        decode_bucket_run(&mut cursor, num_neg as usize, min_key, max_key)?;
    let (pos_entries, pos_weight) =
        decode_bucket_run(&mut cursor, num_pos as usize, min_key, max_key)?;

    if num_collapses > 0 {
        let (prev_min_key, prev_max_key) = mapping.key_bounds(num_collapses - 1);
        let max_prev_buckets: u64 = neg_entries
            .iter()
            .chain(pos_entries.iter())
            .map(|bucket| {
                let hi_child = bucket.key.saturating_mul(2);
                let lo_child = hi_child - 1;
                let reachable = u64::from((prev_min_key..=prev_max_key).contains(&lo_child))
                    + u64::from((prev_min_key..=prev_max_key).contains(&hi_child));
                bucket.count.min(reachable)
            })
            .sum();
        if max_prev_buckets <= u64::from(max_buckets) {
            return Err(Error::deserial(format!(
                "UDDSketch with num_collapses={num_collapses} could have occupied at most {max_prev_buckets} buckets before its last collapse, within max_buckets={max_buckets}"
            )));
        }
    }

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
    if let Some(largest_neg) = neg_entries.last() {
        if !(min_value < 0.0 && mapping.key_at_level(-min_value, num_collapses) == largest_neg.key)
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
        && mapping.key_at_level(min_value, num_collapses) == pos_entries[0].key)
    {
        return Err(Error::deserial(
            "min_value does not match innermost positive bucket",
        ));
    }

    if let Some(largest_pos) = pos_entries.last() {
        if !(max_value > 0.0 && mapping.key_at_level(max_value, num_collapses) == largest_pos.key) {
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
        && mapping.key_at_level(-max_value, num_collapses) == neg_entries[0].key)
    {
        return Err(Error::deserial(
            "max_value does not match innermost negative bucket",
        ));
    }

    Ok(UddSketch::from_parts(
        max_buckets,
        mapping,
        num_collapses,
        total_weight,
        zero_weight,
        min_value,
        max_value,
        BucketStore::from_sorted_unchecked(neg_entries, neg_weight),
        BucketStore::from_sorted_unchecked(pos_entries, pos_weight),
    ))
}

fn decode_bucket_run(
    cursor: &mut SketchSlice<'_>,
    len: usize,
    min_key: i64,
    max_key: i64,
) -> Result<(Vec<Bucket>, u64), Error> {
    let mut entries = Vec::with_capacity(len);
    let mut total_weight = 0u64;
    let mut prev_key: Option<i64> = None;

    for _ in 0..len {
        let key = cursor
            .read_i64_le()
            .map_err(insufficient_data("bucket key"))?;
        let count = cursor
            .read_u64_le()
            .map_err(insufficient_data("bucket count"))?;

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
        entries.push(Bucket { key, count });
    }

    Ok((entries, total_weight))
}
