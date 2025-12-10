//! HyperLogLog Array6 mode - 6-bit packed representation
//!
//! Array6 stores HLL register values using 6 bits per slot, providing a range of 0-63.
//! This is sufficient for most HLL use cases without needing exception handling or
//! cur_min optimization like Array4.

use crate::hll::estimator::HipEstimator;
use crate::hll::{get_slot, get_value};

const VAL_MASK_6: u16 = 0x3F; // 6 bits: 0b0011_1111

/// Core Array6 data structure - stores 6-bit values with cross-byte packing
pub struct Array6 {
    lg_config_k: u8,
    /// Packed 6-bit values, may cross byte boundaries
    bytes: Box<[u8]>,
    /// Count of slots with value 0
    num_zeros: u32,
    /// HIP estimator for cardinality estimation
    estimator: HipEstimator,
}

impl Array6 {
    pub fn new(lg_config_k: u8) -> Self {
        let k = 1 << lg_config_k;
        let num_bytes = num_bytes_for_k(k);

        Self {
            lg_config_k,
            bytes: vec![0u8; num_bytes].into_boxed_slice(),
            num_zeros: k,
            estimator: HipEstimator::new(lg_config_k),
        }
    }

    /// Get value from a slot (6-bit value)
    ///
    /// Uses 16-bit window reads to handle values crossing byte boundaries.
    #[inline]
    fn get_raw(&self, slot: u32) -> u8 {
        let start_bit = slot * 6;
        let byte_idx = (start_bit >> 3) as usize; // Divide by 8
        let shift = (start_bit & 7) as u8; // Mod 8

        // Read 2 bytes as u16 (little-endian)
        let two_bytes = u16::from_le_bytes([self.bytes[byte_idx], self.bytes[byte_idx + 1]]);

        // Extract 6 bits at the shift position
        ((two_bytes >> shift) & VAL_MASK_6) as u8
    }

    /// Set value in a slot (6-bit value)
    ///
    /// Uses read-modify-write on 16-bit window to preserve surrounding bits.
    #[inline]
    fn put_raw(&mut self, slot: u32, value: u8) {
        debug_assert!(value <= 63, "6-bit value must be 0-63");

        let start_bit = slot * 6;
        let byte_idx = (start_bit >> 3) as usize;
        let shift = (start_bit & 0x7) as u8;

        // Read current 2 bytes
        let mut two_bytes = u16::from_le_bytes([self.bytes[byte_idx], self.bytes[byte_idx + 1]]);

        // Clear the 6-bit slot
        two_bytes &= !(VAL_MASK_6 << shift);

        // Insert new value
        two_bytes |= ((value as u16) & VAL_MASK_6) << shift;

        // Write back
        let bytes_out = two_bytes.to_le_bytes();
        self.bytes[byte_idx] = bytes_out[0];
        self.bytes[byte_idx + 1] = bytes_out[1];
    }

    /// Get value for a slot (public API)
    pub fn get(&self, slot: u32) -> u8 {
        self.get_raw(slot)
    }

    /// Update with a coupon
    pub fn update(&mut self, coupon: u32) {
        let mask = (1 << self.lg_config_k) - 1;
        let slot = get_slot(coupon) & mask;
        let new_value = get_value(coupon);

        let old_value = self.get_raw(slot);

        if new_value > old_value {
            // Update HIP and KxQ registers via estimator
            self.estimator
                .update(self.lg_config_k, old_value, new_value);

            // Update the slot
            self.put_raw(slot, new_value);

            // Track num_zeros (count of slots with value 0)
            if old_value == 0 {
                self.num_zeros -= 1;
            }
        }
    }

    /// Get the current cardinality estimate using HIP estimator
    pub fn estimate(&self) -> f64 {
        // Array6 doesn't use cur_min (always 0), so num_at_cur_min = num_zeros
        self.estimator.estimate(self.lg_config_k, 0, self.num_zeros)
    }

    /// Get the number of zero-valued slots
    pub fn num_zeros(&self) -> u32 {
        self.num_zeros
    }

    /// Deserialize Array6 from HLL mode bytes
    ///
    /// Expects full HLL preamble (40 bytes) followed by packed 6-bit data.
    pub(crate) fn deserialize(
        bytes: &[u8],
        lg_config_k: u8,
        compact: bool,
        ooo: bool,
    ) -> std::io::Result<Self> {
        use std::io::{Error, ErrorKind};

        let k = 1 << lg_config_k;
        let num_bytes = num_bytes_for_k(k);
        let expected_len = if compact {
            40 // Just preamble for compact empty sketch
        } else {
            40 + num_bytes
        };

        if bytes.len() < expected_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Array6 data too short: expected {}, got {}",
                    expected_len,
                    bytes.len()
                ),
            ));
        }

        // Read HIP estimator values from preamble
        let hip_accum = f64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let kxq0 = f64::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
            bytes[23],
        ]);
        let kxq1 = f64::from_le_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30],
            bytes[31],
        ]);

        // Read num_at_cur_min (for Array6, this is num_zeros since cur_min=0)
        let num_zeros = u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]);

        // Read packed byte array from offset 40
        let mut data = vec![0u8; num_bytes];
        if !compact {
            data.copy_from_slice(&bytes[40..40 + num_bytes]);
        }

        // Create estimator and restore state
        let mut estimator = HipEstimator::new(lg_config_k);
        estimator.set_hip_accum(hip_accum);
        estimator.set_kxq0(kxq0);
        estimator.set_kxq1(kxq1);
        estimator.set_out_of_order(ooo);

        Ok(Self {
            lg_config_k,
            bytes: data.into_boxed_slice(),
            num_zeros,
            estimator,
        })
    }

    /// Serialize Array6 to bytes
    ///
    /// Produces full HLL preamble (40 bytes) followed by packed 6-bit data.
    pub(crate) fn serialize(&self, lg_config_k: u8) -> std::io::Result<Vec<u8>> {
        let k = 1 << lg_config_k;
        let num_bytes = num_bytes_for_k(k);
        let total_size = 40 + num_bytes;
        let mut bytes = vec![0u8; total_size];

        // Offsets (same as sketch.rs constants)
        const PREAMBLE_INTS_BYTE: usize = 0;
        const SER_VER_BYTE: usize = 1;
        const FAMILY_BYTE: usize = 2;
        const LG_K_BYTE: usize = 3;
        const LG_ARR_BYTE: usize = 4;
        const FLAGS_BYTE: usize = 5;
        const HLL_CUR_MIN_BYTE: usize = 6;
        const MODE_BYTE: usize = 7;
        const HLL_PREINTS: u8 = 10;
        const HLL_FAMILY_ID: u8 = 7;
        const SER_VER: u8 = 1;
        const OUT_OF_ORDER_FLAG_MASK: u8 = 16;

        // Write standard header
        bytes[PREAMBLE_INTS_BYTE] = HLL_PREINTS;
        bytes[SER_VER_BYTE] = SER_VER;
        bytes[FAMILY_BYTE] = HLL_FAMILY_ID;
        bytes[LG_K_BYTE] = lg_config_k;
        bytes[LG_ARR_BYTE] = 0; // Not used for HLL mode

        // Write flags
        let mut flags = 0u8;
        if self.estimator.is_out_of_order() {
            flags |= OUT_OF_ORDER_FLAG_MASK;
        }
        bytes[FLAGS_BYTE] = flags;

        // cur_min is always 0 for Array6
        bytes[HLL_CUR_MIN_BYTE] = 0;

        // Mode byte: low 2 bits = HLL (2), bits 2-3 = HLL6 (1)
        bytes[MODE_BYTE] = 2 | (1 << 2); // 0b00000110 = HLL mode, HLL6 type

        // Write HIP estimator values
        bytes[8..16].copy_from_slice(&self.estimator.hip_accum().to_le_bytes());
        bytes[16..24].copy_from_slice(&self.estimator.kxq0().to_le_bytes());
        bytes[24..32].copy_from_slice(&self.estimator.kxq1().to_le_bytes());

        // Write num_at_cur_min (num_zeros for Array6)
        bytes[32..36].copy_from_slice(&self.num_zeros.to_le_bytes());

        // Write aux_count (always 0 for Array6)
        bytes[36..40].copy_from_slice(&0u32.to_le_bytes());

        // Write packed byte array
        bytes[40..].copy_from_slice(&self.bytes);

        Ok(bytes)
    }
}

// Constants

/// Calculate number of bytes needed for k slots with 6 bits each
fn num_bytes_for_k(k: u32) -> usize {
    // k slots * 6 bits = k * 6/8 bytes = k * 3/4 bytes
    // Add 1 for 16-bit window read safety
    (((k * 3) >> 2) + 1) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hll::{coupon, pack_coupon};

    #[test]
    fn test_array6_basic() {
        let arr = Array6::new(10); // 1024 buckets

        // Initially all slots should be 0
        assert_eq!(arr.get(0), 0);
        assert_eq!(arr.get(100), 0);
        assert_eq!(arr.num_zeros(), 1024);
    }

    #[test]
    fn test_num_bytes_calculation() {
        // k=16 slots: 16 * 6 bits = 96 bits = 12 bytes
        assert_eq!(num_bytes_for_k(16), (16 * 3 / 4) + 1);

        // k=1024: 1024 * 6 bits = 6144 bits = 768 bytes
        assert_eq!(num_bytes_for_k(1024), (1024 * 3 / 4) + 1);
    }

    #[test]
    fn test_get_set_raw() {
        let mut arr = Array6::new(4); // 16 slots

        // Test various 6-bit values across different slots
        arr.put_raw(0, 0);
        arr.put_raw(1, 1);
        arr.put_raw(2, 31);
        arr.put_raw(3, 63); // Max 6-bit value

        assert_eq!(arr.get_raw(0), 0);
        assert_eq!(arr.get_raw(1), 1);
        assert_eq!(arr.get_raw(2), 31);
        assert_eq!(arr.get_raw(3), 63);

        // Test that values don't interfere with each other
        arr.put_raw(5, 42);
        assert_eq!(arr.get_raw(5), 42);
        assert_eq!(arr.get_raw(3), 63); // Earlier value unchanged

        // Test all slots to ensure no cross-slot corruption
        for slot in 0..16 {
            arr.put_raw(slot, (slot % 64) as u8);
        }
        for slot in 0..16 {
            assert_eq!(arr.get_raw(slot), (slot % 64) as u8);
        }
    }

    #[test]
    fn test_boundary_crossing() {
        let mut arr = Array6::new(8); // 256 slots

        // Test values that will cross byte boundaries
        // Slot 1: starts at bit 6 (crosses byte 0/1 boundary)
        arr.put_raw(1, 0b111111);
        assert_eq!(arr.get_raw(1), 63);

        // Slot 2: starts at bit 12 (in byte 1)
        arr.put_raw(2, 0b101010);
        assert_eq!(arr.get_raw(2), 42);

        // Slot 3: starts at bit 18 (crosses byte 2/3 boundary)
        arr.put_raw(3, 0b110011);
        assert_eq!(arr.get_raw(3), 51);

        // Verify no interference
        assert_eq!(arr.get_raw(1), 63);
        assert_eq!(arr.get_raw(2), 42);
        assert_eq!(arr.get_raw(3), 51);
    }

    #[test]
    fn test_update_basic() {
        let mut arr = Array6::new(4);

        // Update slot 0 with value 5
        arr.update(pack_coupon(0, 5));
        assert_eq!(arr.get(0), 5);

        // Update with a smaller value (should be ignored)
        arr.update(pack_coupon(0, 3));
        assert_eq!(arr.get(0), 5);

        // Update with a larger value
        arr.update(pack_coupon(0, 8));
        assert_eq!(arr.get(0), 8);
    }

    #[test]
    fn test_num_zeros_tracking() {
        let mut arr = Array6::new(4); // 16 slots
        assert_eq!(arr.num_zeros(), 16);

        // Update one slot from 0 to non-zero
        arr.update(pack_coupon(0, 5));
        assert_eq!(arr.num_zeros(), 15);

        // Update same slot again (should not change num_zeros)
        arr.update(pack_coupon(0, 10));
        assert_eq!(arr.num_zeros(), 15);

        // Update another slot
        arr.update(pack_coupon(1, 3));
        assert_eq!(arr.num_zeros(), 14);
    }

    #[test]
    fn test_hip_estimator() {
        let mut arr = Array6::new(10); // 1024 buckets

        // Initially estimate should be 0
        assert_eq!(arr.estimate(), 0.0);

        // Add some unique values using real coupon hashing
        for i in 0..10_000u32 {
            let coupon = coupon(&mut &i.to_ne_bytes()[..]).unwrap();
            arr.update(coupon);
        }

        let estimate = arr.estimate();

        // Sanity checks
        assert!(estimate > 0.0, "Estimate should be positive");
        assert!(estimate.is_finite(), "Estimate should be finite");

        // Rough bounds for 10K unique items (very loose)
        assert!(estimate > 1_000.0, "Estimate seems too low");
        assert!(estimate < 100_000.0, "Estimate seems too high");
    }

    #[test]
    fn test_full_range() {
        let mut arr = Array6::new(6); // 64 slots

        // Test all possible 6-bit values (0-63)
        for val in 0..64u8 {
            arr.put_raw(val as u32, val);
        }

        for val in 0..64u8 {
            assert_eq!(arr.get_raw(val as u32), val);
        }
    }

    #[test]
    fn test_kxq_register_split() {
        let mut arr = Array6::new(8); // 256 buckets

        // Test that values < 32 and >= 32 are handled correctly
        arr.update(pack_coupon(0, 10)); // value < 32, goes to kxq0
        arr.update(pack_coupon(1, 40)); // value >= 32, goes to kxq1

        // Initial kxq0 = 256 (all zeros = 1.0 each)
        assert!(arr.estimator.kxq0() < 256.0, "kxq0 should have decreased");

        // kxq1 should have a small positive value (from 1/2^40)
        assert!(arr.estimator.kxq1() > 0.0, "kxq1 should be positive");
        assert!(
            arr.estimator.kxq1() < 0.001,
            "kxq1 should be small (1/2^40 is tiny)"
        );
    }
}
