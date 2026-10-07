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
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::hll::Coupon;
use crate::hll::HllType;
use crate::hll::KEY_MASK_26;
use crate::hll::container::Container;
use crate::hll::serialization::COMPACT_FLAG_MASK;
use crate::hll::serialization::CUR_MODE_SET;
use crate::hll::serialization::HASH_SET_PREINTS;
use crate::hll::serialization::SERIAL_VERSION;
use crate::hll::serialization::SET_PREAMBLE_SIZE;
use crate::hll::serialization::encode_mode_byte;

#[derive(Debug, Clone, PartialEq)]
pub struct HashSet {
    container: Container,
}

impl Default for HashSet {
    fn default() -> Self {
        const LG_INIT_SET_SIZE: usize = 5;
        Self::new(LG_INIT_SET_SIZE)
    }
}

impl HashSet {
    pub fn new(lg_size: usize) -> Self {
        Self {
            container: Container::new(lg_size),
        }
    }

    pub fn update(&mut self, coupon: Coupon) {
        let mask = (1 << self.container.lg_size()) - 1;

        let mut probe = coupon.raw() & mask;
        let starting_position = probe;

        loop {
            let slot = &mut self.container.coupons[probe as usize];
            if slot.is_empty() {
                *slot = coupon;
                self.container.len += 1;
                break;
            } else if *slot == coupon {
                break;
            }

            // An odd stride visits every slot in a power-of-two table.
            let stride = ((coupon.raw() & KEY_MASK_26) >> self.container.lg_size()) | 1;
            probe = (probe + stride) & mask;
            if probe == starting_position {
                // HllSketch must grow or promote the set before it runs out of empty slots.
                unreachable!("HashSet full; no empty slots");
            }
        }
    }

    pub fn container(&self) -> &Container {
        &self.container
    }

    pub fn deserialize(
        mut cursor: SketchSlice,
        lg_arr: usize,
        compact: bool,
    ) -> Result<Self, Error> {
        // Read coupon count from bytes 8-11
        let coupon_count = cursor
            .read_u32_le()
            .map_err(insufficient_data("coupon_count"))?;
        let coupon_count = coupon_count as usize;
        let array_size = 1usize << lg_arr;
        if coupon_count >= array_size {
            return Err(Error::deserial(format!(
                "SET mode coupon count {coupon_count} must be below capacity {array_size}"
            )));
        }
        let read_count = if compact { coupon_count } else { array_size };
        let required_bytes = read_count * size_of::<u32>();
        let available_bytes = cursor.remaining().len();
        if available_bytes < required_bytes {
            return Err(Error::insufficient_data_of(
                "HLL SET mode coupons",
                format_args!("expected {required_bytes} bytes, got {available_bytes}"),
            ));
        }

        if compact {
            // Compact images omit empty slots, so probe positions must be rebuilt.
            let mut hash_set = HashSet::new(lg_arr);
            for i in 0..coupon_count {
                let coupon = cursor.read_u32_le().map_err(|error| {
                    Error::insufficient_data_of("HLL SET mode coupon", error)
                        .with_context("index", i)
                })?;
                hash_set.update(Coupon(coupon));
            }
            if hash_set.container.len() != coupon_count {
                return Err(Error::deserial("SET mode contains duplicate coupons"));
            }
            Ok(hash_set)
        } else {
            // Updatable images contain the probe positions; preserve their table layout.
            let mut coupons = vec![Coupon::EMPTY; array_size];
            for (i, coupon) in coupons.iter_mut().enumerate() {
                let raw = cursor.read_u32_le().map_err(|error| {
                    Error::insufficient_data_of("HLL SET mode coupon", error)
                        .with_context("index", i)
                })?;
                *coupon = Coupon(raw);
            }
            if coupons.iter().filter(|coupon| !coupon.is_empty()).count() != coupon_count {
                return Err(Error::deserial(
                    "SET mode coupon count does not match occupied slots",
                ));
            }

            Ok(Self {
                container: Container::from_coupons(
                    lg_arr,
                    coupons.into_boxed_slice(),
                    coupon_count,
                ),
            })
        }
    }

    /// Serializes occupied coupons in compact format.
    pub fn serialize(&self, lg_config_k: u8, hll_type: HllType) -> Vec<u8> {
        let coupon_count = self.container.len();
        let lg_arr = self.container.lg_size();
        let total_size = SET_PREAMBLE_SIZE + (coupon_count * size_of::<u32>());

        let mut bytes = SketchBytes::with_capacity(total_size);

        bytes.write_u8(HASH_SET_PREINTS);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::HLL.id);
        bytes.write_u8(lg_config_k);
        bytes.write_u8(lg_arr as u8);

        bytes.write_u8(COMPACT_FLAG_MASK);

        bytes.write_u8(0); // Unused state byte in Set mode.

        bytes.write_u8(encode_mode_byte(CUR_MODE_SET, hll_type as u8));

        bytes.write_u32_le(coupon_count as u32);

        for coupon in self.container.iter() {
            bytes.write_u32_le(coupon.raw());
        }

        bytes.into_bytes()
    }
}
