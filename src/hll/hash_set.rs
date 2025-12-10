//! Hash set for storing unique coupons with linear probing
//!
//! Uses open addressing with a custom stride function to handle collisions.
//! Provides better performance than List when many coupons are stored.

use crate::hll::KEY_MASK_26;
use crate::hll::container::{COUPON_EMPTY, Container};

/// Hash set for efficient coupon storage with collision handling
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

    /// Insert coupon into hash set, ignoring duplicates
    pub fn update(&mut self, coupon: u32) {
        let mask = (1 << self.container.lg_size) - 1;
        let coupon = coupon;

        // Initial probe position from low bits of coupon
        let mut probe = coupon & mask;
        let starting_position = probe;

        loop {
            let value = &mut self.container.coupons[probe as usize];
            if value == &COUPON_EMPTY {
                // Found empty slot, insert new coupon
                *value = coupon;
                self.container.len += 1;
                break;
            } else if value == &coupon {
                // Duplicate found, nothing to do
                break;
            }

            // Collision: compute stride and probe next position
            // Stride is always odd to ensure all slots are visited
            let stride = ((coupon & KEY_MASK_26) >> self.container.lg_size) | 1;
            probe = (probe + stride) & mask;
            if probe == starting_position {
                panic!("HashSet full; no empty slots");
            }
        }
    }

    /// Internally grow the set container by a power of two, copying all
    /// the existing values to the new container.
    pub fn grow(&mut self, lg_size: usize) {
        debug_assert!(lg_size > self.container.lg_size);

        let mut new_set = HashSet::new(lg_size);
        for coupon in &self.container.coupons {
            new_set.update(*coupon)
        }

        self.container = new_set.container;
    }
}
