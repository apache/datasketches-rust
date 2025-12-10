//! Simple list for storing unique coupons in order
//!
//! Provides sequential storage with linear search for duplicates.
//! Efficient for small numbers of coupons before transitioning to HashSet.

use std::io;

use crate::hll::container::{COUPON_EMPTY, Container};
use crate::hll::serialization::*;

/// List for sequential coupon storage with duplicate detection
#[derive(Clone)]
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

    /// Deserialize a List from bytes
    pub fn deserialize(bytes: &[u8], empty: bool, compact: bool) -> io::Result<Self> {
        // Read coupon count from byte 6
        let coupon_count = bytes[LIST_COUNT_BYTE] as usize;

        // Compute array size
        let lg_arr = bytes[LG_ARR_BYTE] as usize;
        let array_size = if compact { coupon_count } else { 1 << lg_arr };

        // Validate length
        let expected_len = LIST_INT_ARR_START + (array_size * 4);
        if bytes.len() < expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "LIST data too short: expected {}, got {}",
                    expected_len,
                    bytes.len()
                ),
            ));
        }

        // Read coupons
        let mut coupons = vec![0u32; array_size];
        if !empty && coupon_count > 0 {
            for i in 0..array_size {
                let offset = LIST_INT_ARR_START + i * 4;
                coupons[i] = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);
            }
        }

        Ok(Self {
            container: Container::from_coupons(lg_arr, coupons.into_boxed_slice(), coupon_count),
        })
    }

    /// Serialize a List to bytes
    pub fn serialize(&self, lg_config_k: u8, tgt_hll_type: u8) -> io::Result<Vec<u8>> {
        let compact = true; // Always use compact format
        let empty = self.container.len == 0;
        let coupon_count = self.container.len;
        let lg_arr = self.container.lg_size;

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
        bytes[MODE_BYTE] = encode_mode_byte(CUR_MODE_LIST, tgt_hll_type);

        // Write coupons (only non-empty ones if compact)
        if !empty {
            let mut write_idx = 0;
            for coupon in self.container.coupons.iter() {
                if compact && *coupon == 0 {
                    continue; // Skip empty coupons in compact mode
                }
                let offset = LIST_INT_ARR_START + write_idx * 4;
                bytes[offset..offset + 4].copy_from_slice(&coupon.to_le_bytes());
                write_idx += 1;
                if write_idx >= array_size {
                    break;
                }
            }
        }

        Ok(bytes)
    }
}
