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

use crate::codec::SketchSlice;
use crate::codec::family::Family;
use crate::codec::utility::ensure_preamble_longs_in;
use crate::codec::utility::ensure_serial_version_is;
use crate::common::NumStdDev;
use crate::cpc::MAX_LG_K;
use crate::cpc::MIN_LG_K;
use crate::cpc::estimator::estimate;
use crate::cpc::estimator::lower_bound;
use crate::cpc::estimator::upper_bound;
use crate::cpc::serialization::FLAG_COMPRESSED;
use crate::cpc::serialization::FLAG_HAS_HIP;
use crate::cpc::serialization::FLAG_HAS_TABLE;
use crate::cpc::serialization::FLAG_HAS_WINDOW;
use crate::cpc::serialization::SERIAL_VERSION;
use crate::cpc::serialization::make_preamble_ints;
use crate::error::Error;
use crate::error::ErrorKind;

/// A read-only view of a serialized image of a CpcSketch.
#[derive(Debug, Clone)]
pub struct CpcWrapper {
    lg_k: u8,
    merge_flag: bool,
    num_coupons: u32,
    hip_est_accum: f64,
}

impl CpcWrapper {
    /// Creates a new `CpcWrapper` from the given byte slice without copying bytes.
    pub fn new(bytes: &[u8]) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }

        let mut cursor = SketchSlice::new(bytes);
        let preamble_ints = cursor.read_u8().map_err(make_error("preamble_ints"))?;
        let serial_version = cursor.read_u8().map_err(make_error("serial_version"))?;
        let family_id = cursor.read_u8().map_err(make_error("family_id"))?;
        Family::CPC.validate_id(family_id)?;
        ensure_serial_version_is(SERIAL_VERSION, serial_version)?;

        let lg_k = cursor.read_u8().map_err(make_error("lg_k"))?;
        let first_interesting_column = cursor
            .read_u8()
            .map_err(make_error("first_interesting_column"))?;
        if !(MIN_LG_K..=MAX_LG_K).contains(&lg_k) {
            return Err(Error::invalid_argument(format!(
                "lg_k out of range; got {}",
                lg_k
            )));
        }
        if first_interesting_column > 63 {
            return Err(Error::invalid_argument(format!(
                "first_interesting_column out of range; got {}",
                first_interesting_column
            )));
        }

        let flags = cursor.read_u8().map_err(make_error("flags"))?;
        let is_compressed = flags & (1 << FLAG_COMPRESSED) != 0;
        if !is_compressed {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "only compressed sketches are supported",
            ));
        }
        let has_hip = flags & (1 << FLAG_HAS_HIP) != 0;
        let has_table = flags & (1 << FLAG_HAS_TABLE) != 0;
        let has_window = flags & (1 << FLAG_HAS_WINDOW) != 0;

        cursor.read_u16_le().map_err(make_error("seed_hash"))?;

        let mut num_coupons = 0;
        let mut hip_est_accum = 0.0;

        if has_table || has_window {
            num_coupons = cursor.read_u32_le().map_err(make_error("num_coupons"))?;
            if has_table && has_window {
                cursor
                    .read_u32_le()
                    .map_err(make_error("table_num_entries"))?;
                if has_hip {
                    cursor.read_f64_le().map_err(make_error("kxp"))?;
                    hip_est_accum = cursor.read_f64_le().map_err(make_error("hip_est_accum"))?;
                }
            }
            if has_table {
                cursor
                    .read_u32_le()
                    .map_err(make_error("table_data_words"))?;
            }
            if has_window {
                cursor
                    .read_u32_le()
                    .map_err(make_error("window_data_words"))?;
            }
            if has_hip && !(has_table && has_window) {
                cursor.read_f64_le().map_err(make_error("kxp"))?;
                hip_est_accum = cursor.read_f64_le().map_err(make_error("hip_est_accum"))?;
            }
        }

        let expected_preamble_ints =
            make_preamble_ints(num_coupons, has_hip, has_table, has_window);
        ensure_preamble_longs_in(&[expected_preamble_ints], preamble_ints)?;
        Ok(CpcWrapper {
            lg_k,
            merge_flag: !has_hip,
            num_coupons,
            hip_est_accum,
        })
    }

    /// Return the parameter lg_k.
    pub fn lg_k(&self) -> u8 {
        self.lg_k
    }

    /// Returns the best estimate of the cardinality of the sketch.
    pub fn estimate(&self) -> f64 {
        estimate(
            self.merge_flag,
            self.hip_est_accum,
            self.lg_k,
            self.num_coupons,
        )
    }

    /// Returns the best estimate of the lower bound of the confidence interval given `kappa`.
    pub fn lower_bound(&self, kappa: NumStdDev) -> f64 {
        lower_bound(
            self.merge_flag,
            self.hip_est_accum,
            self.lg_k,
            self.num_coupons,
            kappa,
        )
    }

    /// Returns the best estimate of the upper bound of the confidence interval given `kappa`.
    pub fn upper_bound(&self, kappa: NumStdDev) -> f64 {
        upper_bound(
            self.merge_flag,
            self.hip_est_accum,
            self.lg_k,
            self.num_coupons,
            kappa,
        )
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_coupons == 0
    }
}
