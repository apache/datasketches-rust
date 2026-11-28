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

use std::hash::Hash;

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::MurmurHash3X64128;
use crate::hash::compute_seed_hash;
use crate::thetafamily::common::constants::MAX_LG_K;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::constants::MIN_LG_K;
use crate::thetafamily::common::constants::STRIDE_MASK;

/// Backing slot storage for [`RawHashTable`], abstracting AoS entry vectors (`Theta`, `Tuple`) and
/// strided SoA key/value buffers (`ArrayOfDoubles`).
pub trait TableSlots {
    fn len(&self) -> usize;

    fn key_at(&self, slot: usize) -> u64;

    fn allocate_like(&self, size: usize) -> Self;

    fn clear_and_resize(&mut self, size: usize);

    fn move_slot(&mut self, src_slot: usize, dst: &mut Self, dst_slot: usize);

    fn estimated_size(&self) -> usize;
}

/// Open-addressed QuickSelect hash table shared across Theta, Tuple, and ArrayOfDoubles sketches.
///
/// The slot container `S` supplies per-slot key inspection and slot movement while this type owns
/// parameter validation, MurmurHash3 key screening, double-hash probing, resizing, QuickSelect
/// rebuilding, trimming, and reset mechanics.
#[derive(Debug, Clone)]
pub struct RawHashTable<S> {
    lg_cur_size: u8,
    lg_nom_size: u8,
    lg_max_size: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    seed_hash: u16,
    // Operational threshold used to screen incoming hashes, independent of public emptiness.
    retention_theta: u64,
    slots: S,
    num_retained: usize,
}

impl<S: TableSlots> RawHashTable<S> {
    pub fn new_with_slots(
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        seed: u64,
        make_slots: impl FnOnce(usize) -> S,
    ) -> Result<Self, Error> {
        if !(MIN_LG_K..=MAX_LG_K).contains(&lg_nom_size) {
            return Err(Error::invalid_argument(format!(
                "lg_k must be in [{MIN_LG_K}, {MAX_LG_K}], got {lg_nom_size}"
            )));
        }
        if !(sampling_probability > 0.0 && sampling_probability <= 1.0) {
            return Err(Error::invalid_argument(format!(
                "sampling_probability must be in (0.0, 1.0], got {sampling_probability}"
            )));
        }
        let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
        let lg_max_size = lg_nom_size + 1;
        let lg_cur_size = starting_sub_multiple(lg_max_size, MIN_LG_K, resize_factor.lg_value());
        Ok(Self::allocate_with_slots(
            lg_cur_size,
            lg_nom_size,
            resize_factor,
            sampling_probability,
            starting_retention_theta(sampling_probability),
            seed,
            seed_hash,
            make_slots,
        ))
    }

    pub fn for_set_operation_with_slots(
        lg_cur_size: u8,
        lg_nom_size: u8,
        retention_theta: u64,
        seed: u64,
        seed_hash: u16,
        make_slots: impl FnOnce(usize) -> S,
    ) -> Self {
        Self::allocate_with_slots(
            lg_cur_size,
            lg_nom_size,
            ResizeFactor::X1,
            1.0,
            retention_theta,
            seed,
            seed_hash,
            make_slots,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn allocate_with_slots(
        lg_cur_size: u8,
        lg_nom_size: u8,
        resize_factor: ResizeFactor,
        sampling_probability: f32,
        retention_theta: u64,
        seed: u64,
        seed_hash: u16,
        make_slots: impl FnOnce(usize) -> S,
    ) -> Self {
        let lg_max_size = lg_nom_size + 1;
        assert!(
            lg_cur_size <= lg_max_size,
            "lg_cur_size must be <= lg_nom_size + 1, got lg_cur_size={lg_cur_size}, lg_nom_size={lg_nom_size}"
        );
        let size = if lg_cur_size > 0 {
            1usize << lg_cur_size
        } else {
            0
        };
        Self {
            lg_cur_size,
            lg_nom_size,
            lg_max_size,
            resize_factor,
            sampling_probability,
            seed,
            seed_hash,
            retention_theta,
            slots: make_slots(size),
            num_retained: 0,
        }
    }

    /// Hashes `value` with the table seed into the 63-bit Theta-family hash domain.
    #[inline]
    pub fn hash<T: Hash>(&self, value: T) -> u64 {
        let mut hasher = MurmurHash3X64128::with_seed(self.seed);
        value.hash(&mut hasher);
        let (h1, _) = hasher.finish128();
        h1 >> 1
    }

    #[inline]
    pub fn slots(&self) -> &S {
        &self.slots
    }

    #[inline]
    pub fn slots_mut(&mut self) -> &mut S {
        &mut self.slots
    }

    /// Screens `hash` against `0` and `retention_theta` and probes for its slot, returning
    /// `Some((slot, occupied))` or `None` if screened out or the table is full.
    #[inline]
    pub fn probe_slot(&self, hash: u64) -> Option<(usize, bool)> {
        if hash == 0 || hash >= self.retention_theta {
            return None;
        }
        let slot = self.find_in_curr_slots(hash)?;
        Some((slot, self.slots.key_at(slot) == hash))
    }

    /// Finds the slot containing `hash`, ignoring `retention_theta` (used by read-only lookups).
    #[inline]
    pub fn find_occupied_slot(&self, hash: u64) -> Option<usize> {
        if hash == 0 {
            return None;
        }
        let slot = self.find_in_curr_slots(hash)?;
        (self.slots.key_at(slot) == hash).then_some(slot)
    }

    /// Records a newly inserted entry and resizes or rebuilds if `capacity_threshold` is exceeded.
    #[inline]
    pub fn finish_insert(&mut self) {
        self.num_retained += 1;
        if self.num_retained > self.capacity_threshold() {
            if self.lg_cur_size <= self.lg_nom_size {
                self.resize();
            } else {
                self.rebuild();
            }
        }
    }

    /// Returns the current resize or rebuild capacity threshold.
    #[inline]
    pub fn capacity_threshold(&self) -> usize {
        if self.lg_cur_size <= self.lg_nom_size {
            self.slots.len() / 2
        } else {
            self.slots.len() - self.slots.len() / 16
        }
    }

    /// Trims the table to nominal size `1 << lg_nom_size`.
    pub fn trim(&mut self) {
        if self.num_retained > (1usize << self.lg_nom_size) {
            self.rebuild();
        }
    }

    /// Restores initial capacity and retention threshold and removes all entries.
    pub fn reset(&mut self) {
        let init_lg_cur = starting_sub_multiple(
            self.lg_nom_size + 1,
            MIN_LG_K,
            self.resize_factor.lg_value(),
        );
        self.slots.clear_and_resize(1usize << init_lg_cur);
        self.num_retained = 0;
        self.retention_theta = starting_retention_theta(self.sampling_probability);
        self.lg_cur_size = init_lg_cur;
    }

    #[inline]
    pub fn num_retained(&self) -> usize {
        self.num_retained
    }

    #[inline]
    pub fn retention_theta(&self) -> u64 {
        self.retention_theta
    }

    pub fn set_retention_theta(&mut self, retention_theta: u64) {
        assert!(
            (1..=MAX_THETA).contains(&retention_theta),
            "theta must be in [1, {MAX_THETA}], got {retention_theta}"
        );
        self.retention_theta = retention_theta;
    }

    #[inline]
    pub fn lg_nom_size(&self) -> u8 {
        self.lg_nom_size
    }

    #[inline]
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    #[inline]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn estimated_size(&self) -> usize {
        self.slots.estimated_size()
    }

    #[inline]
    fn find_in_curr_slots(&self, key: u64) -> Option<usize> {
        Self::find_in_slots(&self.slots, key, self.lg_cur_size)
    }

    fn find_in_slots(slots: &S, key: u64, lg_size: u8) -> Option<usize> {
        let mask = slots.len().checked_sub(1)?;
        let stride = (2 * ((key >> lg_size) & STRIDE_MASK) + 1) as usize;
        let start = (key as usize) & mask;
        let mut index = start;
        loop {
            let slot_key = slots.key_at(index);
            if slot_key == 0 || slot_key == key {
                return Some(index);
            }
            index = (index + stride) & mask;
            if index == start {
                return None;
            }
        }
    }

    fn resize(&mut self) {
        let new_lg_size = (self.lg_cur_size + self.resize_factor.lg_value()).min(self.lg_max_size);
        let mut new_slots = self.slots.allocate_like(1usize << new_lg_size);
        for src_slot in 0..self.slots.len() {
            let hash = self.slots.key_at(src_slot);
            if hash != 0 {
                let dst_slot = Self::find_in_slots(&new_slots, hash, new_lg_size)
                    .expect("resized table has a free slot for every retained entry");
                self.slots.move_slot(src_slot, &mut new_slots, dst_slot);
            }
        }
        self.slots = new_slots;
        self.lg_cur_size = new_lg_size;
    }

    fn rebuild(&mut self) {
        let k = 1usize << self.lg_nom_size;
        let mut retained = Vec::with_capacity(self.num_retained);
        for src_slot in 0..self.slots.len() {
            let hash = self.slots.key_at(src_slot);
            if hash != 0 {
                retained.push((hash, src_slot as u32));
            }
        }
        let (_, kth, _) = retained.select_nth_unstable_by_key(k, |&(hash, _)| hash);
        self.retention_theta = kth.0;
        retained.truncate(k);

        let mut new_slots = self.slots.allocate_like(1usize << self.lg_cur_size);
        for (hash, src_slot) in retained {
            let dst_slot = Self::find_in_slots(&new_slots, hash, self.lg_cur_size)
                .expect("rebuilt table has a free slot for every selected entry");
            self.slots
                .move_slot(src_slot as usize, &mut new_slots, dst_slot);
        }
        self.slots = new_slots;
        self.num_retained = k;
    }
}

/// Returns minimal `lg_size` whose rebuild capacity can hold `count` entries at `load_factor`.
pub fn lg_size_from_count_for_rebuild(count: usize, load_factor: f64) -> u8 {
    let log2_n = if count == 0 {
        0_u8
    } else {
        count.ilog2() as u8
    };
    log2_n
        + (if count > (((1u128 << ((log2_n as u32) + 1)) as f64) * load_factor) as usize {
            2
        } else {
            1
        })
}

/// Computes initial `lg_size` for a hash table from target `lg_size`, minimum `lg_size`, and
/// resize factor.
pub fn starting_sub_multiple(lg_target: u8, lg_min: u8, lg_resize_factor: u8) -> u8 {
    if lg_target <= lg_min {
        lg_min
    } else if lg_resize_factor == 0 {
        lg_target
    } else {
        ((lg_target - lg_min) % lg_resize_factor) + lg_min
    }
}

/// Computes the initial operational theta from a sampling probability.
pub fn starting_retention_theta(sampling_probability: f32) -> u64 {
    if sampling_probability < 1.0 {
        (MAX_THETA as f64 * sampling_probability as f64) as u64
    } else {
        MAX_THETA
    }
}
