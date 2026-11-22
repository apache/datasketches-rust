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

use crate::common::NumStdDev;
use crate::hll::COUPON_RSE;
use crate::hll::Coupon;
use crate::hll::coupon_mapping::X_ARR;
use crate::hll::coupon_mapping::Y_ARR;
use crate::hll::cubic_interpolation::using_x_and_y_tables;

#[derive(Debug, Clone)]
pub struct Container<S> {
    /// Empty slots contain `Coupon::EMPTY`.
    pub coupons: S,
    /// Number of non-empty coupons
    pub len: usize,
}

impl<S> Container<S> {
    pub fn from_coupons(coupons: S, len: usize) -> Self {
        Self { coupons, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn estimate(&self) -> f64 {
        let len = self.len as f64;
        let est = using_x_and_y_tables(&X_ARR, &Y_ARR, len);
        len.max(est)
    }

    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        let len = self.len as f64;
        let est = using_x_and_y_tables(&X_ARR, &Y_ARR, len);
        let rse = -(num_std_dev as u8 as f64) * COUPON_RSE;
        let bound = est / (1.0 + rse);
        len.max(bound)
    }

    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        let len = self.len as f64;
        let est = using_x_and_y_tables(&X_ARR, &Y_ARR, len);
        let rse = (num_std_dev as u8 as f64) * COUPON_RSE;
        let bound = est / (1.0 + rse);
        len.max(bound)
    }
}

impl<S: AsRef<[Coupon]>> Container<S> {
    pub fn lg_size(&self) -> usize {
        usize::try_from(self.coupons.as_ref().len().ilog2()).unwrap()
    }

    pub fn is_full(&self) -> bool {
        self.len == self.coupons.as_ref().len()
    }

    pub fn capacity(&self) -> usize {
        self.coupons.as_ref().len()
    }

    pub fn iter(&self) -> impl Iterator<Item = Coupon> + '_ {
        self.coupons
            .as_ref()
            .iter()
            .filter(|&&c| !c.is_empty())
            .copied()
    }
}

impl Container<Box<[Coupon]>> {
    pub fn new(lg_size: usize) -> Self {
        Self {
            coupons: vec![Coupon::EMPTY; 1 << lg_size].into_boxed_slice(),
            len: 0,
        }
    }

    /// Returns an estimate of memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.coupons.len() * size_of::<Coupon>()
    }
}
