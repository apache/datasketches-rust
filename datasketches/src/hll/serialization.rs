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

//! HLL wire-format constants. Preamble lengths ending in `PREINTS` count 32-bit words;
//! those ending in `PREAMBLE_SIZE` count bytes.

pub const SERIAL_VERSION: u8 = 1;

pub const EMPTY_FLAG_MASK: u8 = 4;
/// Flag indicating compact coupon or HLL4 auxiliary storage.
///
/// HLL register arrays have the same layout in compact and updatable images.
pub const COMPACT_FLAG_MASK: u8 = 8;
/// Flag indicating that HIP history is unavailable and composite estimation is required.
pub const OUT_OF_ORDER_FLAG_MASK: u8 = 16;

pub const LIST_PREINTS: u8 = 2;
pub const HASH_SET_PREINTS: u8 = 3;
pub const HLL_PREINTS: u8 = 10;

pub const LIST_PREAMBLE_SIZE: usize = 8;
pub const SET_PREAMBLE_SIZE: usize = 12;
pub const HLL_PREAMBLE_SIZE: usize = 40;

#[inline]
pub fn extract_cur_mode(mode_byte: u8) -> u8 {
    mode_byte & 0x3
}

#[inline]
pub fn extract_tgt_hll_type(mode_byte: u8) -> u8 {
    (mode_byte >> 2) & 0x3
}

/// The mode byte stores `CUR_MODE_*` in bits 0–1 and `TGT_HLL*` in bits 2–3.
#[inline]
pub fn encode_mode_byte(cur_mode: u8, tgt_type: u8) -> u8 {
    (cur_mode & 0x3) | ((tgt_type & 0x3) << 2)
}

pub const CUR_MODE_LIST: u8 = 0;

pub const CUR_MODE_SET: u8 = 1;

pub const CUR_MODE_HLL: u8 = 2;

pub const TGT_HLL4: u8 = 0;

pub const TGT_HLL6: u8 = 1;

pub const TGT_HLL8: u8 = 2;

pub const COUPON_SIZE_BYTES: usize = 4;
