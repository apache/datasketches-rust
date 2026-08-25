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

//! REQ sketch wire format — constants and helpers shared by sketch + compactor serdes.

use crate::codec::assert::ensure_preamble_longs_in;
use crate::codec::assert::ensure_serial_version_is;
use crate::error::Error;

pub(super) const SERIAL_VERSION: u8 = 1;
pub(super) const PREAMBLE_INTS_EXACT: u8 = 2;
pub(super) const PREAMBLE_INTS_ESTIMATION: u8 = 4;
pub(super) const RAW_ITEMS_THRESHOLD: u64 = 4;
/// Initial number of sections in a new compactor. Matches C++/Java
/// `INIT_NUMBER_OF_SECTIONS`.
pub(super) const INIT_NUM_SECTIONS: u8 = 3;
/// Doubling in `ensure_enough_sections` proceeds only while `num_sections` is
/// at most this bound, so `1u64 << (num_sections - 1)` stays in range.
pub(super) const MAX_NUM_SECTIONS_FOR_DOUBLING: u8 = 64;

/// Flag bits — match the C++ enum order: RESERVED1, RESERVED2, IS_EMPTY, IS_HIGH_RANK, RAW_ITEMS,
/// IS_LEVEL_ZERO_SORTED.
pub(super) const FLAG_IS_EMPTY: u8 = 1 << 2;
pub(super) const FLAG_IS_HIGH_RANK: u8 = 1 << 3;
pub(super) const FLAG_RAW_ITEMS: u8 = 1 << 4;
pub(super) const FLAG_IS_LEVEL_ZERO_SORTED: u8 = 1 << 5;

pub(super) fn check_serial_version(actual: u8) -> Result<(), Error> {
    ensure_serial_version_is(SERIAL_VERSION, actual)
}

pub(super) fn check_preamble_ints(actual: u8, num_levels: u8) -> Result<(), Error> {
    let expected = if num_levels > 1 {
        PREAMBLE_INTS_ESTIMATION
    } else {
        PREAMBLE_INTS_EXACT
    };
    ensure_preamble_longs_in(&[expected], actual)
}
