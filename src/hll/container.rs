//! Base container for coupon storage with cardinality estimation
//!
//! Provides a simple array-based storage for coupons (hash values) with
//! cubic interpolation-based cardinality estimation and confidence bounds.

use crate::hll::COUPON_RSE;
use crate::hll::coupon_mapping::{X_ARR, Y_ARR};
use crate::hll::cubic_interpolation::using_x_and_y_tables;

/// Sentinel value indicating an empty coupon slot
pub const COUPON_EMPTY: u32 = 0;

/// Container for storing coupons with basic cardinality estimation
pub struct Container {
    /// Log2 of container size
    pub lg_size: usize,
    /// Array of coupon values (0 = empty)
    pub coupons: Box<[u32]>,
    /// Number of non-empty coupons
    pub len: usize,
}

impl Container {
    pub fn new(lg_size: usize) -> Self {
        Self {
            lg_size,
            coupons: vec![COUPON_EMPTY; 1 << lg_size].into_boxed_slice(),
            len: 0,
        }
    }

    /// Create container from existing coupons
    pub fn from_coupons(lg_size: usize, coupons: Box<[u32]>, len: usize) -> Self {
        Self {
            lg_size,
            coupons,
            len,
        }
    }

    pub fn is_full(&self) -> bool {
        self.len == self.coupons.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get cardinality estimate using cubic interpolation
    pub fn estimate(&self) -> f64 {
        let len = self.len as f64;
        let est = using_x_and_y_tables(&X_ARR, &Y_ARR, len);
        len.max(est)
    }

    /// Get upper confidence bound for cardinality estimate
    pub fn upper_bound(&self, n_std_dev: f64) -> f64 {
        let len = self.len as f64;
        let est = using_x_and_y_tables(&X_ARR, &Y_ARR, len);
        let bound = est / (1.0 - n_std_dev * COUPON_RSE);
        len.max(bound)
    }

    /// Get lower confidence bound for cardinality estimate
    pub fn lower_bound(&self, n_std_dev: f64) -> f64 {
        let len = self.len as f64;
        let est = using_x_and_y_tables(&X_ARR, &Y_ARR, len);
        let bound = est / (1.0 + n_std_dev * COUPON_RSE);
        len.max(bound)
    }
}
