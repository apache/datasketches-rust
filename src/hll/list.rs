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

use crate::error::{SerdeError, SerdeResult};
use crate::hll::HllType;
use crate::hll::container::{COUPON_EMPTY, Container};
use crate::hll::serialization::*;

/// List for sequential coupon storage with duplicate detection
#[derive(Debug, Clone)]
pub struct List {
    container: Container,
}

impl PartialEq for List {
    fn eq(&self, other: &Self) -> bool {
        self.container == other.container
    }
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
    pub fn update(&mut self, coupon: u32) {
        for value in self.container.coupons.iter_mut() {
            if value == &COUPON_EMPTY {
                // Found empty slot, insert new coupon
                *value = coupon;
                self.container.len += 1;
                break;
            } else if value == &coupon {
                // Duplicate found, nothing to do
                break;
            }
        }
    }

    pub fn container(&self) -> &Container {
        &self.container
    }

    /// Deserialize a List from bytes
    pub fn deserialize(bytes: &[u8], empty: bool, compact: bool) -> SerdeResult<Self> {
        // Read coupon count from byte 6
        let coupon_count = bytes[LIST_COUNT_BYTE] as usize;

        // Compute array size
        let lg_arr = bytes[LG_ARR_BYTE] as usize;
        let array_size = if compact { coupon_count } else { 1 << lg_arr };

        // Validate length
        let expected_len = LIST_INT_ARR_START + (array_size * 4);
        if bytes.len() < expected_len {
            return Err(SerdeError::InsufficientData(format!(
                "expected {}, got {}",
                expected_len,
                bytes.len()
            )));
        }

        // Read coupons
        let mut coupons = vec![0u32; array_size];
        if !empty && coupon_count > 0 {
            for (i, coupon) in coupons.iter_mut().enumerate() {
                let offset = LIST_INT_ARR_START + i * COUPON_SIZE_BYTES;
                *coupon = read_u32_le(bytes, offset);
            }
        }

        Ok(Self {
            container: Container::from_coupons(lg_arr, coupons.into_boxed_slice(), coupon_count),
        })
    }

    /// Serialize a List to bytes
    pub fn serialize(&self, lg_config_k: u8, hll_type: HllType) -> SerdeResult<Vec<u8>> {
        let compact = true; // Always use compact format
        let empty = self.container.len() == 0;
        let coupon_count = self.container.len();
        let lg_arr = self.container.lg_size();

        // Compute size
        let array_size = if compact { coupon_count } else { 1 << lg_arr };
        let total_size = LIST_INT_ARR_START + (array_size * 4);

        let mut bytes = vec![0u8; total_size];

        // Write preamble
        bytes[PREAMBLE_INTS_BYTE] = LIST_PREINTS;
        bytes[SER_VER_BYTE] = SER_VER;
        bytes[FAMILY_BYTE] = HLL_FAMILY_ID;
        bytes[LG_K_BYTE] = lg_config_k;
        bytes[LG_ARR_BYTE] = lg_arr as u8;

        // Write flags
        let mut flags = 0u8;
        if empty {
            flags |= EMPTY_FLAG_MASK;
        }
        if compact {
            flags |= COMPACT_FLAG_MASK;
        }
        bytes[FLAGS_BYTE] = flags;

        // Write count
        bytes[LIST_COUNT_BYTE] = coupon_count as u8;

        // Write mode byte: LIST mode with target HLL type
        bytes[MODE_BYTE] = encode_mode_byte(CUR_MODE_LIST, hll_type as u8);

        // Write coupons (only non-empty ones if compact)
        if !empty {
            let mut write_idx = 0;
            for coupon in &self.container.coupons {
                if compact && *coupon == 0 {
                    continue; // Skip empty coupons in compact mode
                }
                let offset = LIST_INT_ARR_START + write_idx * 4;
                write_u32_le(&mut bytes, offset, *coupon);
                write_idx += 1;
                if write_idx >= array_size {
                    break;
                }
            }
        }

        Ok(bytes)
    }
}
