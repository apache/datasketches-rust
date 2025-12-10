//! Binary serialization format constants for HLL sketches
//!
//! This module contains all constants related to the Apache DataSketches
//! binary serialization format, shared across all sketch modes.

/// Family ID for HLL sketches in DataSketches format
pub const HLL_FAMILY_ID: u8 = 7;

/// Current serialization version
pub const SER_VER: u8 = 1;

/// Flag indicating sketch is empty (no values inserted)
pub const EMPTY_FLAG_MASK: u8 = 4;

/// Flag indicating compact serialization (no empty slots stored)
pub const COMPACT_FLAG_MASK: u8 = 8;

/// Flag indicating out-of-order mode (HIP estimator invalid)
pub const OUT_OF_ORDER_FLAG_MASK: u8 = 16;

/// Flag indicating full-size serialization (all slots stored)
pub const FULL_SIZE_FLAG_MASK: u8 = 32;

/// Offset of preamble size field (in 4-byte ints)
pub const PREAMBLE_INTS_BYTE: usize = 0;

/// Offset of serialization version byte
pub const SER_VER_BYTE: usize = 1;

/// Offset of family ID byte
pub const FAMILY_BYTE: usize = 2;

/// Offset of lg_config_k byte
pub const LG_K_BYTE: usize = 3;

/// Offset of lg_arr (array size) byte
pub const LG_ARR_BYTE: usize = 4;

/// Offset of flags byte
pub const FLAGS_BYTE: usize = 5;

/// Offset of mode byte (current mode in low 2 bits, target type in bits 2-3)
pub const MODE_BYTE: usize = 7;

/// Preamble size for LIST mode (8 bytes = 2 ints)
pub const LIST_PREINTS: u8 = 2;

/// Preamble size for SET mode (12 bytes = 3 ints)
pub const HASH_SET_PREINTS: u8 = 3;

/// Preamble size for HLL mode (40 bytes = 10 ints)
pub const HLL_PREINTS: u8 = 10;

/// Offset of coupon count byte in LIST mode
pub const LIST_COUNT_BYTE: usize = 6;

/// Offset where coupon array starts in LIST mode
pub const LIST_INT_ARR_START: usize = 8;

/// Offset of coupon count in SET mode (4-byte int)
pub const HASH_SET_COUNT_INT: usize = 8;

/// Offset where coupon array starts in SET mode
pub const HASH_SET_INT_ARR_START: usize = 12;

/// Offset of cur_min byte in HLL mode header
pub const HLL_CUR_MIN_BYTE: usize = 6;

/// Offset of HIP accumulator (8-byte double) in HLL preamble
pub const HIP_ACCUM_DOUBLE: usize = 8;

/// Offset of KxQ0 register (8-byte double) in HLL preamble
pub const KXQ0_DOUBLE: usize = 16;

/// Offset of KxQ1 register (8-byte double) in HLL preamble
pub const KXQ1_DOUBLE: usize = 24;

/// Offset of num_at_cur_min (4-byte int) in HLL preamble
pub const CUR_MIN_COUNT_INT: usize = 32;

/// Offset of aux_count (4-byte int) in HLL preamble
pub const AUX_COUNT_INT: usize = 36;

/// Offset where HLL byte array data starts
pub const HLL_BYTE_ARR_START: usize = 40;

/// Total size of HLL preamble in bytes
pub const HLL_PREAMBLE_SIZE: usize = 40;

/// Extract current mode from mode byte (low 2 bits)
///
/// Returns: 0 = LIST, 1 = SET, 2 = HLL
#[inline]
pub fn extract_cur_mode(mode_byte: u8) -> u8 {
    mode_byte & 0x3
}

/// Extract target HLL type from mode byte (bits 2-3)
///
/// Returns: 0 = HLL4, 1 = HLL6, 2 = HLL8
#[inline]
pub fn extract_tgt_hll_type(mode_byte: u8) -> u8 {
    (mode_byte >> 2) & 0x3
}

/// Encode mode byte from current mode and target type
///
/// # Arguments
/// * `cur_mode` - 0 = LIST, 1 = SET, 2 = HLL
/// * `tgt_type` - 0 = HLL4, 1 = HLL6, 2 = HLL8
#[inline]
pub fn encode_mode_byte(cur_mode: u8, tgt_type: u8) -> u8 {
    (cur_mode & 0x3) | ((tgt_type & 0x3) << 2)
}

/// Current mode: LIST
pub const CUR_MODE_LIST: u8 = 0;

/// Current mode: SET
pub const CUR_MODE_SET: u8 = 1;

/// Current mode: HLL
pub const CUR_MODE_HLL: u8 = 2;

/// Target type: HLL4 (4-bit packed)
pub const TGT_HLL4: u8 = 0;

/// Target type: HLL6 (6-bit packed)
pub const TGT_HLL6: u8 = 1;

/// Target type: HLL8 (8-bit unpacked)
pub const TGT_HLL8: u8 = 2;

/// Size of a single coupon in bytes (u32)
pub const COUPON_SIZE_BYTES: usize = 4;

/// Size of a double (f64) in bytes
pub const DOUBLE_SIZE_BYTES: usize = 8;

/// Size of an int (u32) in bytes
pub const INT_SIZE_BYTES: usize = 4;

/// Read a u32 value from bytes at the given offset (little-endian)
#[inline]
pub fn read_u32_le(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

/// Read a f64 value from bytes at the given offset (little-endian)
#[inline]
pub fn read_f64_le(bytes: &[u8], offset: usize) -> f64 {
    f64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

/// Write a u32 value to bytes at the given offset (little-endian)
#[inline]
pub fn write_u32_le(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + INT_SIZE_BYTES].copy_from_slice(&value.to_le_bytes());
}

/// Write a f64 value to bytes at the given offset (little-endian)
#[inline]
pub fn write_f64_le(bytes: &mut [u8], offset: usize, value: f64) {
    bytes[offset..offset + DOUBLE_SIZE_BYTES].copy_from_slice(&value.to_le_bytes());
}
