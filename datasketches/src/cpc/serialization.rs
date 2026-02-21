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

pub(super) const SERIAL_VERSION: u8 = 1;
pub(super) const FLAG_COMPRESSED: u8 = 1;
pub(super) const FLAG_HAS_HIP: u8 = 2;
pub(super) const FLAG_HAS_TABLE: u8 = 3;
pub(super) const FLAG_HAS_WINDOW: u8 = 4;

pub(super) fn make_preamble_ints(
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
