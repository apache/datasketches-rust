//! Simple list for storing unique coupons in order
//!
//! Provides sequential storage with linear search for duplicates.
//! Efficient for small numbers of coupons before transitioning to HashSet.

use crate::hll::container::{COUPON_EMPTY, Container};

/// List for sequential coupon storage with duplicate detection
pub struct List {
    pub(crate) container: Container,
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

    /// Create list from existing coupons (for deserialization)
    pub(crate) fn from_coupons(lg_size: usize, coupons: Box<[u32]>, len: usize) -> Self {
        Self {
            container: Container::from_coupons(lg_size, coupons, len),
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
}
