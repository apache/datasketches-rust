use std::io;

use crate::hll::array4::Array4;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
use crate::hll::hash_set::HashSet;
use crate::hll::list::List;

// Binary format constants
const HLL_FAMILY_ID: u8 = 7;
const SER_VER: u8 = 1;

// Flag bit masks (byte 5)
const EMPTY_FLAG_MASK: u8 = 4;
const COMPACT_FLAG_MASK: u8 = 8;
const OUT_OF_ORDER_FLAG_MASK: u8 = 16;
const FULL_SIZE_FLAG_MASK: u8 = 32;

// Preamble offsets
const PREAMBLE_INTS_BYTE: usize = 0;
const SER_VER_BYTE: usize = 1;
const FAMILY_BYTE: usize = 2;
const LG_K_BYTE: usize = 3;
const LG_ARR_BYTE: usize = 4;
const FLAGS_BYTE: usize = 5;
const LIST_COUNT_BYTE: usize = 6;
const HLL_CUR_MIN_BYTE: usize = 6;
const MODE_BYTE: usize = 7;

// Data offsets
const LIST_INT_ARR_START: usize = 8;
const HASH_SET_COUNT_INT: usize = 8;
const HASH_SET_INT_ARR_START: usize = 12;
const HIP_ACCUM_DOUBLE: usize = 8;
const KXQ0_DOUBLE: usize = 16;
const KXQ1_DOUBLE: usize = 24;
const CUR_MIN_COUNT_INT: usize = 32;
const AUX_COUNT_INT: usize = 36;
const HLL_BYTE_ARR_START: usize = 40;

// Preamble sizes
const LIST_PREINTS: u8 = 2;
const HASH_SET_PREINTS: u8 = 3;
const HLL_PREINTS: u8 = 10;

/// Current sketch mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CurMode {
    List = 0,
    Set = 1,
    Hll = 2,
}

/// Target HLL type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TgtHllType {
    Hll4 = 0,
    Hll6 = 1,
    Hll8 = 2,
}

pub struct HllSketch {
    lg_config_k: u8,
    tgt_hll_type: TgtHllType,
    mode: Mode,
}

impl HllSketch {
    /// Check if two sketches are functionally equal
    pub fn equals(&self, other: &Self) -> bool {
        if self.lg_config_k != other.lg_config_k {
            return false;
        }

        match (&self.mode, &other.mode) {
            (Mode::List(l1), Mode::List(l2)) => l1 == l2,
            (Mode::Set(s1), Mode::Set(s2)) => s1 == s2,
            (Mode::Array4(_), Mode::Array4(_)) => {
                // TODO: Implement Array4 equality
                true
            }
            (Mode::Array6(_), Mode::Array6(_)) => {
                // TODO: Implement Array6 equality
                true
            }
            (Mode::Array8(_), Mode::Array8(_)) => {
                // TODO: Implement Array8 equality
                true
            }
            _ => false, // Different modes are not equal
        }
    }
}

enum Mode {
    List(List),
    Set(HashSet),
    Array4(Array4),
    Array6(Array6),
    Array8(Array8),
}

impl HllSketch {
    pub fn lg_config_k(&self) -> u8 {
        self.lg_config_k
    }

    pub fn deserialize(bytes: &[u8]) -> io::Result<HllSketch> {
        if bytes.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sketch data too short (< 8 bytes)",
            ));
        }

        // Read and validate preamble
        let preamble_ints = bytes[PREAMBLE_INTS_BYTE];
        let ser_ver = bytes[SER_VER_BYTE];
        let family_id = bytes[FAMILY_BYTE];
        let lg_config_k = bytes[LG_K_BYTE];
        let flags = bytes[FLAGS_BYTE];
        let mode_byte = bytes[MODE_BYTE];

        // Verify family ID (HLL = 7)
        if family_id != HLL_FAMILY_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid family: expected 7 (HLL), got {}", family_id),
            ));
        }

        // Verify serialization version
        if ser_ver != SER_VER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid serialization version: expected {}, got {}",
                    SER_VER, ser_ver
                ),
            ));
        }

        // Verify lg_k range (4-21 are valid)
        if !(4..=21).contains(&lg_config_k) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid lg_k: {}, must be in [4; 21]", lg_config_k),
            ));
        }

        // Extract mode and type
        let cur_mode = extract_cur_mode(mode_byte);
        let tgt_type = extract_tgt_hll_type(mode_byte);
        let empty = (flags & EMPTY_FLAG_MASK) != 0;
        let compact = (flags & COMPACT_FLAG_MASK) != 0;
        let ooo = (flags & OUT_OF_ORDER_FLAG_MASK) != 0;

        // Deserialize based on mode
        let mode = match cur_mode {
            CurMode::List => {
                if preamble_ints != LIST_PREINTS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "invalid preamble ints for LIST mode: expected {}, got {}",
                            LIST_PREINTS, preamble_ints
                        ),
                    ));
                }
                deserialize_list(bytes, empty, compact, ooo)?
            }
            CurMode::Set => {
                if preamble_ints != HASH_SET_PREINTS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "invalid preamble ints for SET mode: expected {}, got {}",
                            HASH_SET_PREINTS, preamble_ints
                        ),
                    ));
                }
                deserialize_set(bytes, compact)?
            }
            CurMode::Hll => {
                if preamble_ints != HLL_PREINTS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "invalid preamble ints for HLL mode: expected {}, got {}",
                            HLL_PREINTS, preamble_ints
                        ),
                    ));
                }
                deserialize_hll(bytes, lg_config_k, tgt_type, compact, ooo)?
            }
        };

        Ok(HllSketch {
            lg_config_k,
            tgt_hll_type: tgt_type,
            mode,
        })
    }

    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        match &self.mode {
            Mode::List(list) => serialize_list(list, self.lg_config_k, self.tgt_hll_type),
            Mode::Set(set) => serialize_set(set, self.lg_config_k, self.tgt_hll_type),
            Mode::Array4(arr) => serialize_hll4(arr, self.lg_config_k),
            Mode::Array6(arr) => serialize_hll6(arr, self.lg_config_k),
            Mode::Array8(arr) => serialize_hll8(arr, self.lg_config_k),
        }
    }
}

/// Extract current mode from mode byte (low 2 bits)
fn extract_cur_mode(mode_byte: u8) -> CurMode {
    match mode_byte & 0x3 {
        0 => CurMode::List,
        1 => CurMode::Set,
        2 => CurMode::Hll,
        _ => unreachable!(),
    }
}

/// Extract target HLL type from mode byte (bits 2-3)
fn extract_tgt_hll_type(mode_byte: u8) -> TgtHllType {
    match (mode_byte >> 2) & 0x3 {
        0 => TgtHllType::Hll4,
        1 => TgtHllType::Hll6,
        2 => TgtHllType::Hll8,
        _ => unreachable!(),
    }
}

/// Deserialize LIST mode sketch
fn deserialize_list(bytes: &[u8], empty: bool, compact: bool, _ooo: bool) -> io::Result<Mode> {
    List::deserialize(bytes, empty, compact).map(Mode::List)
}

/// Deserialize SET mode sketch
fn deserialize_set(bytes: &[u8], compact: bool) -> io::Result<Mode> {
    HashSet::deserialize(bytes, compact).map(Mode::Set)
}

/// Deserialize HLL mode sketch
fn deserialize_hll(
    bytes: &[u8],
    lg_config_k: u8,
    tgt_type: TgtHllType,
    compact: bool,
    ooo: bool,
) -> io::Result<Mode> {
    match tgt_type {
        TgtHllType::Hll4 => {
            Array4::deserialize(bytes, lg_config_k, compact, ooo).map(Mode::Array4)
        }
        TgtHllType::Hll6 => {
            Array6::deserialize(bytes, lg_config_k, compact, ooo).map(Mode::Array6)
        }
        TgtHllType::Hll8 => {
            Array8::deserialize(bytes, lg_config_k, compact, ooo).map(Mode::Array8)
        }
    }
}

/// Serialize LIST mode sketch
fn serialize_list(list: &List, lg_config_k: u8, tgt_hll_type: TgtHllType) -> io::Result<Vec<u8>> {
    list.serialize(lg_config_k, tgt_hll_type as u8)
}

/// Serialize SET mode sketch
fn serialize_set(set: &HashSet, lg_config_k: u8, tgt_hll_type: TgtHllType) -> io::Result<Vec<u8>> {
    set.serialize(lg_config_k, tgt_hll_type as u8)
}

/// Serialize HLL4 mode sketch
fn serialize_hll4(arr: &Array4, lg_config_k: u8) -> io::Result<Vec<u8>> {
    arr.serialize(lg_config_k)
}

/// Serialize HLL6 mode sketch
fn serialize_hll6(arr: &Array6, lg_config_k: u8) -> io::Result<Vec<u8>> {
    arr.serialize(lg_config_k)
}

/// Serialize HLL8 mode sketch
fn serialize_hll8(arr: &Array8, lg_config_k: u8) -> io::Result<Vec<u8>> {
    arr.serialize(lg_config_k)
}
