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

//! Simple list for storing unique coupons in order
//!
//! Provides sequential storage with linear search for duplicates.
//! Efficient for small numbers of coupons before transitioning to HashSet.

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::family::Family;
use crate::error::Error;
use crate::hll::Coupon;
use crate::hll::HllType;
use crate::hll::container::Container;
use crate::hll::serialization::COMPACT_FLAG_MASK;
use crate::hll::serialization::CUR_MODE_LIST;
use crate::hll::serialization::EMPTY_FLAG_MASK;
use crate::hll::serialization::LIST_PREAMBLE_SIZE;
use crate::hll::serialization::LIST_PREINTS;
use crate::hll::serialization::SERIAL_VERSION;
use crate::hll::serialization::encode_mode_byte;

/// List for sequential coupon storage with duplicate detection
#[derive(Debug, Clone, PartialEq)]
pub struct List {
    container: Container,
}

impl Default for List {
    fn default() -> Self {
        const LG_INIT_LIST_SIZE: usize = 3;
        Self::new(LG_INIT_LIST_SIZE)
    }
}

impl List {
    pub fn new(lg_size: usize) -> Self {
        Self {
            container: Container::new(lg_size),
        }
    }

    /// Insert coupon into list, ignoring duplicates
    pub fn update(&mut self, coupon: Coupon) {
        for value in self.container.coupons.iter_mut() {
            if value.is_empty() {
                // Found empty slot, insert new coupon
                *value = coupon;
                self.container.len += 1;
                break;
            } else if *value == coupon {
                // Duplicate found, nothing to do
                break;
            }
        }
    }

    pub fn container(&self) -> &Container {
        &self.container
    }

    /// Deserialize a List from bytes
    pub fn deserialize(
        mut cursor: SketchSlice,
        lg_arr: usize,
        coupon_count: usize,
        empty: bool,
        compact: bool,
    ) -> Result<Self, Error> {
        // Always allocate the full-sized array (1 << lg_arr) so Coupon::EMPTY sentinel
        // slots are available for future update() calls. In compact format only
        // coupon_count values are stored on disk, but memory must hold the full capacity
        // so the linear scan in update() can find an empty slot to insert into.
        let array_size = 1 << lg_arr;
        let read_count = if compact { coupon_count } else { array_size };

        // Read coupons into the front of the full-sized array; remaining slots stay Coupon::EMPTY.
        let mut coupons = vec![Coupon::EMPTY; array_size];
        if !empty && coupon_count > 0 {
            for (i, coupon) in coupons.iter_mut().take(read_count).enumerate() {
                let raw = cursor.read_u32_le().map_err(|_| {
                    Error::insufficient_data(format!(
                        "expect {coupon_count} coupons, failed at index {i}"
                    ))
                })?;
                *coupon = Coupon(raw);
            }
        }

        Ok(Self {
            container: Container::from_coupons(lg_arr, coupons.into_boxed_slice(), coupon_count),
        })
    }

    /// Serialize a List to bytes
    pub fn serialize(&self, lg_config_k: u8, hll_type: HllType) -> Vec<u8> {
        let compact = true; // Always use compact format
        let empty = self.container.is_empty();
        let coupon_count = self.container.len();
        let lg_arr = self.container.lg_size();

        // Compute size
        let array_size = if compact { coupon_count } else { 1 << lg_arr };
        let total_size = LIST_PREAMBLE_SIZE + (array_size * 4);

        let mut bytes = SketchBytes::with_capacity(total_size);

        // Write preamble
        bytes.write_u8(LIST_PREINTS);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::HLL.id);
        bytes.write_u8(lg_config_k);
        bytes.write_u8(lg_arr as u8);

        // Write flags
        let mut flags = 0u8;
        if empty {
            flags |= EMPTY_FLAG_MASK;
        }
        if compact {
            flags |= COMPACT_FLAG_MASK;
        }
        bytes.write_u8(flags);

        // Write count
        bytes.write_u8(coupon_count as u8);

        // Write mode byte: LIST mode with target HLL type
        bytes.write_u8(encode_mode_byte(CUR_MODE_LIST, hll_type as u8));

        // Write coupons (only non-empty ones if compact)
        if !empty {
            let mut write_idx = 0;
            for coupon in self.container.coupons.iter().copied() {
                if compact && coupon.is_empty() {
                    continue; // Skip empty coupons in compact mode
                }
                bytes.write_u32_le(coupon.raw());
                write_idx += 1;
                if write_idx >= array_size {
                    break;
                }
            }
        }

        bytes.into_bytes()
    }
}
