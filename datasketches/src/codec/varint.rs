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

//! Variable-length 64-bit integer codecs using DataDog's 9-byte `uvarint64` and ZigZag `varint64`
//! encodings.

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::insufficient_data;
use crate::error::Error;

/// Maximum encoded byte length of a 64-bit DataDog varint (9 bytes, where the 9th byte carries all
/// 8 top bits).
pub const MAX_VAR_LEN_64: usize = 9;

/// Encodes `v` as a 9-byte DataDog unsigned varint into `dst[..MAX_VAR_LEN_64]` and returns the
/// number of bytes written (`1..=MAX_VAR_LEN_64`).
///
/// # Panics
///
/// Panics if `dst.len() < MAX_VAR_LEN_64`.
#[inline(always)]
pub fn write_uvarint64_to_buf(dst: &mut [u8], v: u64) -> usize {
    let dst = &mut dst[..MAX_VAR_LEN_64];
    let mut v = v;
    let mut i = 0;
    while i < MAX_VAR_LEN_64 - 1 && v >= 0x80 {
        dst[i] = (v as u8) | 0x80;
        v >>= 7;
        i += 1;
    }
    dst[i] = v as u8;
    i + 1
}

/// Encodes `v` as a DataDog unsigned varint into `out`.
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

/// Decodes a DataDog unsigned varint from `cursor`, preserving lenient 9th-byte and overlong
/// semantics.
#[inline]
pub fn decode_uvarint64(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<u64, Error> {
    let mut x = 0;
    let mut shift = 0;
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

/// Decodes a DataDog unsigned varint directly from a byte slice with 1-byte and 2-byte fast paths,
/// advancing `*src` past the consumed bytes.
#[inline(always)]
pub fn decode_uvarint64_slice(src: &mut &[u8], tag: &'static str) -> Result<u64, Error> {
    let bytes = *src;
    if bytes.is_empty() {
        return Err(Error::insufficient_data_of(
            tag,
            "unexpected end of uvarint64",
        ));
    }
    if bytes[0] < 0x80 {
        *src = &bytes[1..];
        return Ok(u64::from(bytes[0]));
    }
    if bytes.len() >= 2 && bytes[1] < 0x80 {
        *src = &bytes[2..];
        return Ok(u64::from(bytes[0] & 0x7f) | (u64::from(bytes[1]) << 7));
    }

    let mut x = 0;
    let mut shift = 0;
    for i in 0..MAX_VAR_LEN_64 {
        if i == bytes.len() {
            return Err(Error::insufficient_data_of(
                tag,
                "unexpected end of uvarint64",
            ));
        }
        let n = bytes[i];
        if n < 0x80 || i == MAX_VAR_LEN_64 - 1 {
            *src = &bytes[i + 1..];
            return Ok(x | (u64::from(n) << shift));
        }
        x |= u64::from(n & 0x7f) << shift;
        shift += 7;
    }
    unreachable!()
}

/// Returns the encoded byte length (`1..=MAX_VAR_LEN_64`) of `v` as a DataDog unsigned varint.
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

/// Encodes `v` as a ZigZag-mapped DataDog signed varint into `out`.
#[inline]
pub fn encode_varint64(out: &mut SketchBytes, v: i64) {
    let zigzag = ((v << 1) ^ (v >> 63)) as u64;
    encode_uvarint64(out, zigzag);
}

/// Decodes a ZigZag-mapped DataDog signed varint from `cursor`.
#[inline]
pub fn decode_varint64(cursor: &mut SketchSlice<'_>, tag: &'static str) -> Result<i64, Error> {
    let u = decode_uvarint64(cursor, tag)?;
    Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
}

/// Decodes a ZigZag-mapped DataDog signed varint directly from a byte slice, advancing `*src` past
/// the consumed bytes.
#[inline(always)]
pub fn decode_varint64_slice(src: &mut &[u8], tag: &'static str) -> Result<i64, Error> {
    let u = decode_uvarint64_slice(src, tag)?;
    Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
}

/// Returns the encoded byte length (`1..=MAX_VAR_LEN_64`) of `v` as a ZigZag-mapped DataDog
/// signed varint.
#[inline]
pub const fn varint64_size(v: i64) -> usize {
    let zigzag = ((v << 1) ^ (v >> 63)) as u64;
    uvarint64_size(zigzag)
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
                1 << 56,
                &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
            ),
            (
                u64::MAX - 1,
                &[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ),
            (u64::MAX, &[0xff; 9]),
        ];
        for &(value, expected) in cases {
            let mut out = SketchBytes::default();
            encode_uvarint64(&mut out, value);
            let mut bytes = out.into_bytes();
            assert_eq!(bytes, expected);
            assert_eq!(uvarint64_size(value), expected.len());

            let mut buf = [0; MAX_VAR_LEN_64];
            let len = write_uvarint64_to_buf(&mut buf, value);
            assert_eq!(&buf[..len], expected);

            bytes.push(0x2a);
            let mut cursor = SketchSlice::new(&bytes);
            let mut src = bytes.as_slice();
            assert_eq!(decode_uvarint64(&mut cursor, "test").unwrap(), value);
            assert_eq!(decode_uvarint64_slice(&mut src, "test").unwrap(), value);
            assert_eq!(cursor.remaining(), &[0x2a]);
            assert_eq!(src, &[0x2a]);
        }

        for overlong in [&[0x81, 0x00][..], &[0x81, 0x80, 0x00]] {
            let mut cursor = SketchSlice::new(overlong);
            let mut src = overlong;
            assert_eq!(decode_uvarint64(&mut cursor, "test").unwrap(), 1);
            assert_eq!(decode_uvarint64_slice(&mut src, "test").unwrap(), 1);
            assert!(cursor.remaining().is_empty());
            assert!(src.is_empty());
        }

        let bytes = [0xff; MAX_VAR_LEN_64];
        for len in 0..bytes.len() {
            let truncated = &bytes[..len];
            let mut cursor = SketchSlice::new(truncated);
            let mut src = truncated;
            let error = decode_uvarint64(&mut cursor, "test").unwrap_err();
            assert_eq!(error.kind(), crate::error::ErrorKind::InvalidData);
            let error = decode_uvarint64_slice(&mut src, "test").unwrap_err();
            assert_eq!(error.kind(), crate::error::ErrorKind::InvalidData);
            assert_eq!(src, truncated);
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
            (i64::MIN, &[0xff; 9]),
        ];
        for &(value, expected) in cases {
            let mut out = SketchBytes::default();
            encode_varint64(&mut out, value);
            let bytes = out.into_bytes();
            assert_eq!(bytes, expected);
            assert_eq!(varint64_size(value), expected.len());

            let mut cursor = SketchSlice::new(&bytes);
            let mut src = bytes.as_slice();
            assert_eq!(decode_varint64(&mut cursor, "test").unwrap(), value);
            assert_eq!(decode_varint64_slice(&mut src, "test").unwrap(), value);
            assert!(cursor.remaining().is_empty());
            assert!(src.is_empty());
        }
    }
}
