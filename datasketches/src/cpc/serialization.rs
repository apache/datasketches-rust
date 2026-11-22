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
use crate::codec::family::Family;
use crate::cpc::compression::encode_window;
use crate::cpc::compression::estimated_encoded_pair_bytes;
use crate::cpc::compression::estimated_encoded_window_bytes;

pub const SERIAL_VERSION: u8 = 1;
pub const FLAG_COMPRESSED: u8 = 1;
pub const FLAG_HAS_HIP: u8 = 2;
pub const FLAG_HAS_TABLE: u8 = 3;
pub const FLAG_HAS_WINDOW: u8 = 4;

pub fn make_preamble_ints(
    num_coupons: u32,
    has_hip: bool,
    has_table: bool,
    has_window: bool,
) -> u8 {
    let mut preamble_ints = 2;
    if num_coupons > 0 {
        preamble_ints += 1; // number of coupons
        if has_hip {
            preamble_ints += 4; // HIP
        }
        if has_table {
            preamble_ints += 1; // table data length
            // number of values (if there is no window it is the same as number of coupons)
            if has_window {
                preamble_ints += 1;
            }
        }
        if has_window {
            preamble_ints += 1; // window length
        }
    }
    preamble_ints
}

pub fn serialize_compressed<W, F>(
    lg_k: u8,
    seed_hash: u16,
    first_interesting_column: u8,
    num_coupons: u32,
    hip: Option<(f64, f64)>,
    window: Option<W>,
    table_num_entries: u32,
    encode_table: F,
) -> Vec<u8>
where
    W: IntoIterator<Item = u8>,
    F: FnOnce(&mut SketchBytes) -> usize,
{
    let has_hip = hip.is_some();
    let has_window = window.is_some();
    let has_table = table_num_entries > 0;
    let preamble_ints = make_preamble_ints(num_coupons, has_hip, has_table, has_window);
    let header_bytes = usize::from(preamble_ints) * size_of::<u32>();
    let payload_bytes = if num_coupons == 0 {
        0
    } else {
        let window_bytes = if has_window {
            estimated_encoded_window_bytes(lg_k, num_coupons)
        } else {
            0
        };
        window_bytes + estimated_encoded_pair_bytes(table_num_entries, lg_k)
    };

    let mut bytes = SketchBytes::with_capacity(header_bytes + payload_bytes);
    bytes.write_u8(preamble_ints);
    bytes.write_u8(SERIAL_VERSION);
    bytes.write_u8(Family::CPC.id);
    bytes.write_u8(lg_k);
    bytes.write_u8(first_interesting_column);
    let flags = (1 << FLAG_COMPRESSED)
        | (u8::from(has_hip) << FLAG_HAS_HIP)
        | (u8::from(has_table) << FLAG_HAS_TABLE)
        | (u8::from(has_window) << FLAG_HAS_WINDOW);
    bytes.write_u8(flags);
    bytes.write_u16_le(seed_hash);

    if num_coupons == 0 {
        return bytes.into_bytes();
    }

    bytes.write_u32_le(num_coupons);
    let mut words_offset = 12;
    if has_table && has_window {
        // if there is no window it is the same as number of coupons
        bytes.write_u32_le(table_num_entries);
        words_offset += 4;
        // HIP values can be in two different places in the sequence of fields
        // this is the first HIP decision point
        if let Some((kxp, hip_est_accum)) = hip {
            bytes.write_f64_le(kxp);
            bytes.write_f64_le(hip_est_accum);
            words_offset += 16;
        }
    }
    let table_words_offset = words_offset;
    if has_table {
        bytes.write_u32_le(0);
        words_offset += 4;
    }
    let window_words_offset = words_offset;
    if has_window {
        bytes.write_u32_le(0);
    }
    // this is the second HIP decision point
    if !(has_table && has_window) {
        if let Some((kxp, hip_est_accum)) = hip {
            bytes.write_f64_le(kxp);
            bytes.write_f64_le(hip_est_accum);
        }
    }

    let window_words = window.map(|w| encode_window(w, lg_k, num_coupons, &mut bytes));
    let table_words = has_table.then(|| encode_table(&mut bytes));

    let mut out = bytes.into_bytes();
    if let Some(table_words) = table_words {
        debug_assert!(table_words <= u32::MAX as usize);
        out[table_words_offset..table_words_offset + 4]
            .copy_from_slice(&(table_words as u32).to_le_bytes());
    }
    if let Some(window_words) = window_words {
        debug_assert!(window_words <= u32::MAX as usize);
        out[window_words_offset..window_words_offset + 4]
            .copy_from_slice(&(window_words as u32).to_le_bytes());
    }
    out
}
