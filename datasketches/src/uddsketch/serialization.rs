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
use crate::codec::varint::MAX_VAR_LEN_64;
use crate::codec::varint::decode_uvarint64_slice;
use crate::codec::varint::decode_varint64_slice;
use crate::codec::varint::encode_uvarint64;
use crate::codec::varint::write_uvarint64_to_buf;
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
const PREAMBLE_LONGS_NON_EMPTY: u8 = 4;

const FLAG_HAS_COLLAPSES: u8 = 1 << 0;
const FLAG_HAS_ZERO: u8 = 1 << 1;
const FLAGS_IS_EMPTY: u8 = 1 << 2;
const FLAG_HAS_NEG: u8 = 1 << 3;
const FLAG_HAS_POS: u8 = 1 << 4;
const FLAG_NEG_UNIT_WEIGHT: u8 = 1 << 5;
const FLAG_POS_UNIT_WEIGHT: u8 = 1 << 6;
const SUPPORTED_FLAGS: u8 = FLAG_HAS_COLLAPSES
    | FLAG_HAS_ZERO
    | FLAGS_IS_EMPTY
    | FLAG_HAS_NEG
    | FLAG_HAS_POS
    | FLAG_NEG_UNIT_WEIGHT
    | FLAG_POS_UNIT_WEIGHT;

const RUN_CHUNK_BYTES: usize = 256;

/// Serializes `sketch` into the compact delta-varint `UDDSKETCH` binary wire format.
pub fn serialize_sketch(sketch: &UddSketch) -> Vec<u8> {
    if sketch.is_empty() {
        let mut bytes = SketchBytes::with_capacity(16);
        bytes.write_u8(PREAMBLE_LONGS_EMPTY);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::UDDSKETCH.id);
        bytes.write_u8(FLAGS_IS_EMPTY);
        bytes.write_u32_le(sketch.max_buckets());
        bytes.write_f64_le(sketch.initial_alpha());
        return bytes.into_bytes();
    }

    let neg = sketch.neg_store();
    let pos = sketch.pos_store();
    let neg_unit = !neg.is_empty() && neg.total_weight() == neg.len() as u64;
    let pos_unit = !pos.is_empty() && pos.total_weight() == pos.len() as u64;
    let mut flags = 0;
    if sketch.num_collapses() > 0 {
        flags |= FLAG_HAS_COLLAPSES;
    }
    if sketch.zero_weight() > 0 {
        flags |= FLAG_HAS_ZERO;
    }
    if !neg.is_empty() {
        flags |= FLAG_HAS_NEG;
    }
    if !pos.is_empty() {
        flags |= FLAG_HAS_POS;
    }
    if neg_unit {
        flags |= FLAG_NEG_UNIT_WEIGHT;
    }
    if pos_unit {
        flags |= FLAG_POS_UNIT_WEIGHT;
    }

    let mut bytes = SketchBytes::with_capacity(
        32 + 16
            + neg.len() * if neg_unit { 1 } else { 2 }
            + pos.len() * if pos_unit { 1 } else { 2 },
    );
    bytes.write_u8(PREAMBLE_LONGS_NON_EMPTY);
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(Family::UDDSKETCH.id);
    bytes.write_u8(flags);
    bytes.write_u32_le(sketch.max_buckets());
    bytes.write_f64_le(sketch.initial_alpha());
    bytes.write_f64_le(sketch.min_value().unwrap());
    bytes.write_f64_le(sketch.max_value().unwrap());

    if flags & FLAG_HAS_COLLAPSES != 0 {
        bytes.write_u8(sketch.num_collapses());
    }
    if flags & FLAG_HAS_ZERO != 0 {
        encode_uvarint64(&mut bytes, sketch.zero_weight());
    }
    if flags & FLAG_HAS_NEG != 0 {
        encode_uvarint64(&mut bytes, neg.len() as u64);
        if neg_unit {
            encode_bucket_run::<true>(&mut bytes, neg.sorted_buckets());
        } else {
            encode_bucket_run::<false>(&mut bytes, neg.sorted_buckets());
        }
    }
    if flags & FLAG_HAS_POS != 0 {
        encode_uvarint64(&mut bytes, pos.len() as u64);
        if pos_unit {
            encode_bucket_run::<true>(&mut bytes, pos.sorted_buckets());
        } else {
            encode_bucket_run::<false>(&mut bytes, pos.sorted_buckets());
        }
    }
    bytes.into_bytes()
}

/// Deserializes a `UDDSKETCH` image from `bytes`, enforcing header, flag, DoS budget, bucket
/// key/weight, extrema, and pre-collapse reachability invariants.
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

    if (flags & !SUPPORTED_FLAGS) != 0 {
        return Err(Error::deserial(format!(
            "unsupported UDDSketch flags: {flags:#x}"
        )));
    }
    let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
    if is_empty && flags != FLAGS_IS_EMPTY {
        return Err(Error::deserial("empty UDDSketch has non-empty flags"));
    }
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

    let has_collapses = flags & FLAG_HAS_COLLAPSES != 0;
    let has_zero = flags & FLAG_HAS_ZERO != 0;
    let has_neg = flags & FLAG_HAS_NEG != 0;
    let has_pos = flags & FLAG_HAS_POS != 0;
    let neg_unit = flags & FLAG_NEG_UNIT_WEIGHT != 0;
    let pos_unit = flags & FLAG_POS_UNIT_WEIGHT != 0;
    if (neg_unit && !has_neg) || (pos_unit && !has_pos) {
        return Err(Error::deserial("unit weight flag requires a bucket run"));
    }
    if !has_zero && !has_neg && !has_pos {
        return Err(Error::deserial(
            "non-empty UDDSketch has no occupied buckets",
        ));
    }

    let min_value = cursor
        .read_f64_le()
        .map_err(insufficient_data("min_value"))?;
    let max_value = cursor
        .read_f64_le()
        .map_err(insufficient_data("max_value"))?;

    if !min_value.is_finite()
        || !max_value.is_finite()
        || min_value > max_value
        || min_value.to_bits() == (-0.0f64).to_bits()
        || max_value.to_bits() == (-0.0f64).to_bits()
    {
        return Err(Error::deserial("invalid UDDSketch extrema"));
    }

    let mut rem = cursor.remaining();
    let num_collapses = if has_collapses {
        let (&level, rest) = rem
            .split_first()
            .ok_or_else(|| Error::insufficient_data_of("num_collapses", "expected one byte"))?;
        rem = rest;
        if level == 0 || level > MAX_COLLAPSES {
            return Err(Error::deserial(format!(
                "num_collapses must be in [1, {MAX_COLLAPSES}] when present, got {level}"
            )));
        }
        level
    } else {
        0
    };
    let zero_weight = if has_zero {
        let weight = decode_uvarint64_slice(&mut rem, "zero_weight")?;
        if weight == 0 {
            return Err(Error::deserial("zero_weight must be positive when present"));
        }
        weight
    } else {
        0
    };

    let (min_key, max_key) = mapping.key_bounds(num_collapses);
    let (neg_entries, neg_weight) = if has_neg {
        let num_neg = decode_uvarint64_slice(&mut rem, "num_neg_buckets")?;
        if num_neg == 0 {
            return Err(Error::deserial(
                "num_neg_buckets must be positive when present",
            ));
        }
        if num_neg > u64::from(max_buckets) {
            return Err(Error::deserial(format!(
                "retained bucket count ({num_neg}) exceeds max_buckets ({max_buckets})"
            )));
        }
        let len = usize::try_from(num_neg)
            .map_err(|_| Error::deserial("bucket count exceeds platform usize::MAX"))?;
        if neg_unit {
            decode_bucket_run::<true>(&mut rem, len, min_key, max_key)?
        } else {
            decode_bucket_run::<false>(&mut rem, len, min_key, max_key)?
        }
    } else {
        (Vec::new(), 0)
    };
    let (pos_entries, pos_weight) = if has_pos {
        let num_pos = decode_uvarint64_slice(&mut rem, "num_pos_buckets")?;
        if num_pos == 0 {
            return Err(Error::deserial(
                "num_pos_buckets must be positive when present",
            ));
        }
        let total_buckets = (neg_entries.len() as u64)
            .checked_add(num_pos)
            .ok_or_else(|| Error::deserial("UDDSketch bucket count overflow"))?;
        if total_buckets > u64::from(max_buckets) {
            return Err(Error::deserial(format!(
                "retained bucket count ({total_buckets}) exceeds max_buckets ({max_buckets})"
            )));
        }
        let len = usize::try_from(num_pos)
            .map_err(|_| Error::deserial("bucket count exceeds platform usize::MAX"))?;
        if pos_unit {
            decode_bucket_run::<true>(&mut rem, len, min_key, max_key)?
        } else {
            decode_bucket_run::<false>(&mut rem, len, min_key, max_key)?
        }
    } else {
        (Vec::new(), 0)
    };
    if !rem.is_empty() {
        return Err(Error::deserial(format!(
            "expected end of UDDSketch image, found {} trailing bytes",
            rem.len()
        )));
    }

    let total_buckets = (neg_entries.len() + pos_entries.len()) as u64;
    let min_collapsed_buckets = u64::from(max_buckets) / 2 + 1;
    if num_collapses > 0 && total_buckets < min_collapsed_buckets {
        return Err(Error::deserial(format!(
            "UDDSketch with num_collapses={num_collapses} must retain at least {min_collapsed_buckets} buckets, got {total_buckets}"
        )));
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

    if num_collapses > 0 {
        let prev_level = num_collapses - 1;
        let (prev_min_key, _) = mapping.key_bounds(prev_level);
        let neg_prev = if neg_entries.is_empty() {
            0
        } else {
            let neg_lo = if max_value < 0.0 {
                mapping.key_at_level(-max_value, prev_level)
            } else {
                prev_min_key
            };
            let neg_hi = mapping.key_at_level(-min_value, prev_level);
            count_reachable_prev_buckets(&neg_entries, neg_lo, neg_hi)
        };
        let pos_prev = if pos_entries.is_empty() {
            0
        } else {
            let pos_lo = if min_value > 0.0 {
                mapping.key_at_level(min_value, prev_level)
            } else {
                prev_min_key
            };
            let pos_hi = mapping.key_at_level(max_value, prev_level);
            count_reachable_prev_buckets(&pos_entries, pos_lo, pos_hi)
        };
        let max_prev_buckets = neg_prev + pos_prev;
        if max_prev_buckets <= u64::from(max_buckets) {
            return Err(Error::deserial(format!(
                "UDDSketch with num_collapses={num_collapses} could have occupied at most {max_prev_buckets} buckets before its last collapse, within max_buckets={max_buckets}"
            )));
        }
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

#[inline(always)]
fn push_uvarint64_chunked(chunk: &mut [u8; RUN_CHUNK_BYTES], pos: &mut usize, v: u64) {
    if v < 0x80 {
        chunk[*pos] = v as u8;
        *pos += 1;
    } else if v < 0x4000 {
        chunk[*pos] = (v as u8) | 0x80;
        chunk[*pos + 1] = (v >> 7) as u8;
        *pos += 2;
    } else {
        *pos += write_uvarint64_to_buf(&mut chunk[*pos..], v);
    }
}

/// Encodes a sorted bucket run into `out` using a `[u8; 256]` stack buffer (`UNIT_WEIGHT` omits
/// per-bucket `count - 1` varints).
fn encode_bucket_run<const UNIT_WEIGHT: bool>(out: &mut SketchBytes, buckets: &[Bucket]) {
    if buckets.is_empty() {
        return;
    }
    let mut chunk = [0u8; RUN_CHUNK_BYTES];
    let mut pos = 0usize;
    let first_key = buckets[0].key;
    let zigzag = ((first_key << 1) ^ (first_key >> 63)) as u64;
    push_uvarint64_chunked(&mut chunk, &mut pos, zigzag);
    if !UNIT_WEIGHT {
        push_uvarint64_chunked(&mut chunk, &mut pos, buckets[0].count - 1);
    }

    let mut prev_key = first_key;
    for bucket in &buckets[1..] {
        if pos > RUN_CHUNK_BYTES - MAX_VAR_LEN_64 * 2 {
            out.write(&chunk[..pos]);
            pos = 0;
        }
        let delta_m1 = (bucket.key as u64).wrapping_sub(prev_key as u64) - 1;
        push_uvarint64_chunked(&mut chunk, &mut pos, delta_m1);
        if !UNIT_WEIGHT {
            push_uvarint64_chunked(&mut chunk, &mut pos, bucket.count - 1);
        }
        prev_key = bucket.key;
    }
    if pos > 0 {
        out.write(&chunk[..pos]);
    }
}

/// Decodes `len` strictly ascending buckets in `[min_key, max_key]` from `src` after checking
/// minimum byte bounds to prevent oversized preallocation.
fn decode_bucket_run<const UNIT_WEIGHT: bool>(
    src: &mut &[u8],
    len: usize,
    min_key: i64,
    max_key: i64,
) -> Result<(Vec<Bucket>, u64), Error> {
    if len == 0 {
        return Ok((Vec::new(), 0));
    }
    let min_bytes = if UNIT_WEIGHT {
        len
    } else {
        len.checked_mul(2)
            .ok_or_else(|| Error::deserial("bucket payload size overflows usize"))?
    };
    if src.len() < min_bytes {
        return Err(Error::insufficient_data_of(
            "UDDSketch buckets",
            format_args!("expected at least {min_bytes} bytes, got {}", src.len()),
        ));
    }

    let mut entries = Vec::with_capacity(len);
    let first_key = decode_varint64_slice(src, "bucket key")?;
    if first_key < min_key || first_key > max_key {
        return Err(Error::deserial(format!(
            "UDDSketch bucket key {first_key} outside valid range [{min_key}, {max_key}]"
        )));
    }
    let first_count = if UNIT_WEIGHT {
        1
    } else {
        decode_uvarint64_slice(src, "bucket count")?
            .checked_add(1)
            .ok_or_else(|| Error::deserial("UDDSketch bucket count overflow"))?
    };
    entries.push(Bucket {
        key: first_key,
        count: first_count,
    });
    let mut total_weight = first_count;
    let mut prev_key = first_key;
    for _ in 1..len {
        let delta_m1 = decode_uvarint64_slice(src, "bucket key delta")?;
        if delta_m1 >= (max_key as u64).wrapping_sub(prev_key as u64) {
            return Err(Error::deserial(
                "UDDSketch bucket key delta exceeds valid range",
            ));
        }
        let key = (prev_key as u64).wrapping_add(delta_m1).wrapping_add(1) as i64;
        let count = if UNIT_WEIGHT {
            1
        } else {
            decode_uvarint64_slice(src, "bucket count")?
                .checked_add(1)
                .ok_or_else(|| Error::deserial("UDDSketch bucket count overflow"))?
        };
        total_weight = total_weight
            .checked_add(count)
            .ok_or_else(|| Error::deserial("UDDSketch bucket weight overflow"))?;
        entries.push(Bucket { key, count });
        prev_key = key;
    }
    Ok((entries, total_weight))
}

fn count_reachable_prev_buckets(entries: &[Bucket], prev_min_key: i64, prev_max_key: i64) -> u64 {
    entries
        .iter()
        .map(|bucket| {
            let hi_child = bucket.key.saturating_mul(2);
            let lo_child = hi_child - 1;
            let reachable = u64::from((prev_min_key..=prev_max_key).contains(&lo_child))
                + u64::from((prev_min_key..=prev_max_key).contains(&hi_child));
            bucket.count.min(reachable)
        })
        .sum()
}
