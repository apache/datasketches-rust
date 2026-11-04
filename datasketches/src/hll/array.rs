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

use std::fmt::Debug;
use std::sync::OnceLock;

use crate::common::NumStdDev;
use crate::hll::Coupon;
use crate::hll::array8::Registers8;
use crate::hll::estimator::EstimateState;
use crate::hll::estimator::Kxq;
use crate::hll::estimator::RegisterStatistics;
use crate::hll::estimator::relative_error;

/// A dense sketch's registers and the estimation state derived from those same registers.
///
/// All register mutations pass through this owner, so a caller cannot query statistics for
/// a different register array or forget to invalidate statistics after a bulk merge.
#[derive(Debug, Clone)]
pub struct HllArray<R: Registers> {
    registers: R,
    state: EstimateState,
    statistics: R::Statistics,
}

/// Physical register storage and the statistics maintenance it supports.
pub trait Registers: Debug + Clone + PartialEq {
    type Statistics: StatisticsStorage;
    fn new(lg_config_k: u8) -> Self;
    fn lg_config_k(&self) -> u8;
    fn get(&self, slot: u32) -> u8;
    fn values(&self) -> impl Iterator<Item = u8>;
    fn num_zeros(&self) -> u32;
    fn is_empty(&self) -> bool;
    fn update(&mut self, coupon: Coupon) -> Option<(u8, u8)>;
}

/// Eager sums for packed storage, or deferred statistics for bulk-mergeable storage.
pub trait StatisticsStorage: Debug + Clone {
    fn from_serialized(kxq: Kxq, num_zeros: u32) -> Self;
    fn resolve(&self, registers: &impl Registers) -> RegisterStatistics;
    fn update(&mut self, state: &mut EstimateState, lg_config_k: u8, old: u8, new: u8);
    fn cached_num_zeros(&self) -> Option<u32> {
        None
    }
}

impl StatisticsStorage for Kxq {
    fn from_serialized(kxq: Kxq, _: u32) -> Self {
        kxq
    }

    fn resolve(&self, registers: &impl Registers) -> RegisterStatistics {
        RegisterStatistics {
            num_zeros: registers.num_zeros(),
            kxq: *self,
        }
    }

    #[inline]
    fn update(&mut self, state: &mut EstimateState, lg_config_k: u8, old: u8, new: u8) {
        state.update(lg_config_k, self, old, new);
    }
}

impl StatisticsStorage for OnceLock<RegisterStatistics> {
    fn from_serialized(kxq: Kxq, num_zeros: u32) -> Self {
        OnceLock::from(RegisterStatistics { num_zeros, kxq })
    }

    fn resolve(&self, registers: &impl Registers) -> RegisterStatistics {
        *self.get_or_init(|| RegisterStatistics::from_registers(registers.values()))
    }

    #[inline]
    fn update(&mut self, state: &mut EstimateState, lg_config_k: u8, old: u8, new: u8) {
        if let Some(statistics) = self.get_mut() {
            state.update(lg_config_k, &mut statistics.kxq, old, new);
            statistics.num_zeros -= u32::from(old == 0);
        } else {
            debug_assert_eq!(*state, EstimateState::Composite);
        }
    }

    fn cached_num_zeros(&self) -> Option<u32> {
        self.get().map(|statistics| statistics.num_zeros)
    }
}

impl<R: Registers> PartialEq for HllArray<R> {
    fn eq(&self, other: &Self) -> bool {
        self.registers == other.registers
            && self.state == other.state
            && self.statistics() == other.statistics()
    }
}

impl<R: Registers> HllArray<R> {
    pub fn new(lg_config_k: u8) -> Self {
        Self::from_serialized(
            R::new(lg_config_k),
            0.0,
            (1 << lg_config_k) as f64,
            0.0,
            1 << lg_config_k,
            false,
        )
    }

    pub fn from_serialized(
        registers: R,
        hip_accum: f64,
        kxq0: f64,
        kxq1: f64,
        num_zeros: u32,
        out_of_order: bool,
    ) -> Self {
        Self {
            registers,
            state: EstimateState::from_serialized(hip_accum, out_of_order),
            statistics: R::Statistics::from_serialized(
                Kxq {
                    low: kxq0,
                    high: kxq1,
                },
                num_zeros,
            ),
        }
    }

    /// Returns storage for read-only encoding and representation-specific inspection.
    pub fn registers(&self) -> &R {
        &self.registers
    }

    pub fn lg_config_k(&self) -> u8 {
        self.registers.lg_config_k()
    }

    pub fn num_registers(&self) -> usize {
        1 << self.lg_config_k()
    }

    pub fn get(&self, slot: u32) -> u8 {
        self.registers.get(slot)
    }

    pub fn estimate_state(&self) -> EstimateState {
        self.state
    }

    pub fn statistics(&self) -> RegisterStatistics {
        self.statistics.resolve(&self.registers)
    }

    #[inline]
    pub fn update(&mut self, coupon: Coupon) {
        if let Some((old, new)) = self.registers.update(coupon) {
            self.statistics
                .update(&mut self.state, self.registers.lg_config_k(), old, new);
        }
    }

    pub fn restore_estimate_state(&mut self, state: EstimateState) {
        if let EstimateState::Hip(_) = state {
            // Resuming HIP requires current sums before the next scalar update.
            self.statistics();
        }
        self.state = state;
    }

    pub fn estimate(&self) -> f64 {
        match self.state {
            EstimateState::Hip(value) => value,
            EstimateState::Composite => self.composite_estimate(),
        }
    }

    pub fn composite_estimate(&self) -> f64 {
        self.statistics().composite_estimate(self.lg_config_k())
    }

    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        let rse = relative_error(
            self.lg_config_k(),
            true,
            self.state.uses_composite_estimate(),
            num_std_dev,
        );
        self.estimate() / (1.0 + rse)
    }

    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        let statistics = self.statistics();
        let estimate = match self.state {
            EstimateState::Hip(value) => value,
            EstimateState::Composite => statistics.composite_estimate(self.lg_config_k()),
        };
        let rse = relative_error(
            self.lg_config_k(),
            false,
            self.state.uses_composite_estimate(),
            num_std_dev,
        );
        (estimate / (1.0 + rse)).max(f64::from((1 << self.lg_config_k()) - statistics.num_zeros))
    }

    pub fn is_empty(&self) -> bool {
        match self.statistics.cached_num_zeros() {
            Some(num_zeros) => num_zeros == (1 << self.lg_config_k()),
            // Do not rebuild statistics just to answer emptiness between bulk merges.
            None => self.registers.is_empty(),
        }
    }
}

impl HllArray<Registers8> {
    pub fn values(&self) -> &[u8] {
        self.registers.as_slice()
    }

    fn invalidate(&mut self) {
        self.state = EstimateState::Composite;
        self.statistics.take();
    }

    pub fn merge_registers(&mut self, values: impl Iterator<Item = (usize, u8)>) {
        // Invalidate before consuming the iterator so a panic cannot leave stale statistics.
        self.invalidate();
        self.registers.merge_registers(values);
    }

    pub fn merge_array_same_lgk(&mut self, src: &[u8]) {
        assert_eq!(
            src.len(),
            self.num_registers(),
            "Source and destination must have same lg_k"
        );
        self.invalidate();
        self.registers.merge_same_lgk(src);
    }

    pub fn merge_array_with_downsample(&mut self, src: &[u8], src_lg_k: u8) {
        assert!(
            src_lg_k > self.lg_config_k(),
            "Source lg_k must be greater than destination lg_k for downsampling"
        );
        assert_eq!(
            src.len(),
            1 << src_lg_k,
            "Source length must match 2^src_lg_k"
        );
        self.invalidate();
        self.registers.merge_with_downsample(src);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hll::array4::Registers4;
    use crate::hll::array6::Registers6;
    use crate::hll::array8::Array8;

    #[test]
    fn lower_bounds_respect_occupied_registers_in_every_representation() {
        fn check<R: Registers>() {
            let mut array = HllArray::<R>::new(4);
            for slot in 0..8 {
                array.update(Coupon::pack(slot, 1));
            }
            assert_eq!(array.lower_bound(NumStdDev::Three), 8.0);
            for slot in 8..16 {
                array.update(Coupon::pack(slot, 1));
            }
            assert_eq!(array.lower_bound(NumStdDev::Three), 16.0);
        }
        check::<Registers4>();
        check::<Registers6>();
        check::<Registers8>();
    }

    #[test]
    fn restored_hip_supports_updates_after_deferred_reconstruction() {
        let mut eager = Array8::new(4);
        for (slot, value) in [(0, 4), (1, 32), (2, 63)] {
            eager.update(Coupon::pack(slot, value));
        }
        let history = eager.estimate_state();
        let mut deferred = eager.clone();
        // Merging zero registers leaves the data unchanged but discards update history.
        deferred.merge_array_same_lgk(&[0; 16]);
        deferred.restore_estimate_state(history);
        for (slot, value) in [(3, 2), (0, 33), (3, 5)] {
            eager.update(Coupon::pack(slot, value));
            deferred.update(Coupon::pack(slot, value));
        }
        assert_eq!(deferred.estimate(), eager.estimate());
        assert_eq!(deferred.serialize(), eager.serialize());
    }

    #[test]
    fn interrupted_bulk_merge_cannot_leave_stale_statistics() {
        let mut array = Array8::new(4);
        array.update(Coupon::pack(0, 1));
        let mut expected = array.clone();
        expected.merge_registers(std::iter::once((1, 5)));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            array.merge_registers((1..3).map(|slot| {
                assert_eq!(slot, 1, "interrupted register input");
                (slot, 5)
            }));
        }));
        assert!(result.is_err());
        assert_eq!(array.estimate(), expected.estimate());
        assert_eq!(array.serialize(), expected.serialize());
    }
}
