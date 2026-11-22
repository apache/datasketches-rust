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

const LG_INIT_LIST_SIZE: u8 = 3;
const LIST_CAPACITY: usize = 1 << LG_INIT_LIST_SIZE;

#[derive(Debug, Clone)]
pub struct List {
    container: Container<[Coupon; LIST_CAPACITY]>,
}

impl Default for List {
    fn default() -> Self {
        Self {
            container: Container::from_coupons([Coupon::EMPTY; LIST_CAPACITY], 0),
        }
    }
}

impl List {
    pub fn update(&mut self, coupon: Coupon) {
        for value in self.container.coupons.iter_mut() {
            if value.is_empty() {
                *value = coupon;
                self.container.len += 1;
                break;
            } else if *value == coupon {
                break;
            }
        }
    }

    pub fn container(&self) -> &Container<[Coupon; LIST_CAPACITY]> {
        &self.container
    }

    pub fn deserialize(
        mut cursor: SketchSlice,
        lg_arr: u8,
        coupon_count: usize,
        empty: bool,
        compact: bool,
    ) -> Result<Self, Error> {
        if lg_arr != LG_INIT_LIST_SIZE {
            return Err(Error::deserial(format!(
                "LIST mode lg_arr: expected {LG_INIT_LIST_SIZE}, got {lg_arr}"
            )));
        }
        if coupon_count > LIST_CAPACITY {
            return Err(Error::deserial(format!(
                "LIST mode coupon count {coupon_count} exceeds capacity {LIST_CAPACITY}"
            )));
        }
        if empty != (coupon_count == 0) {
            return Err(Error::deserial(
                "LIST mode empty flag and coupon count disagree",
            ));
        }
        let read_count = if compact { coupon_count } else { LIST_CAPACITY };
        let required_bytes = read_count * size_of::<u32>();
        if !empty {
            let available_bytes = cursor.remaining().len();
            if available_bytes < required_bytes {
                return Err(Error::insufficient_data_of(
                    "HLL LIST mode coupons",
                    format_args!("expected {required_bytes} bytes, got {available_bytes}"),
                ));
            }
        }

        // Compact images omit empty slots. Restore the full capacity so future updates
        // can insert into an empty slot before the list promotes.
        let mut coupons = [Coupon::EMPTY; LIST_CAPACITY];
        if !empty && coupon_count > 0 {
            for (i, coupon) in coupons.iter_mut().take(read_count).enumerate() {
                let raw = cursor.read_u32_le().map_err(|error| {
                    Error::insufficient_data_of("HLL LIST mode coupon", error)
                        .with_context("index", i)
                })?;
                *coupon = Coupon(raw);
            }
        }

        Ok(Self {
            container: Container::from_coupons(coupons, coupon_count),
        })
    }

    /// Serializes occupied coupons in compact format.
    pub fn serialize(&self, lg_config_k: u8, hll_type: HllType) -> Vec<u8> {
        let empty = self.container.is_empty();
        let coupon_count = self.container.len();
        let total_size = LIST_PREAMBLE_SIZE + (coupon_count * size_of::<u32>());

        let mut bytes = SketchBytes::with_capacity(total_size);

        bytes.write_u8(LIST_PREINTS);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::HLL.id);
        bytes.write_u8(lg_config_k);
        bytes.write_u8(LG_INIT_LIST_SIZE);

        let mut flags = COMPACT_FLAG_MASK;
        if empty {
            flags |= EMPTY_FLAG_MASK;
        }
        bytes.write_u8(flags);

        bytes.write_u8(coupon_count as u8);

        bytes.write_u8(encode_mode_byte(CUR_MODE_LIST, hll_type as u8));

        for coupon in self.container.iter().take(coupon_count) {
            bytes.write_u32_le(coupon.raw());
        }

        bytes.into_bytes()
    }
}
