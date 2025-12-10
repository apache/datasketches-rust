use std::io;

use crate::hll::array4::Array4;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
use crate::hll::hash_set::HashSet;
use crate::hll::list::List;
use crate::hll::serialization::*;

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
        let cur_mode = extract_cur_mode_enum(mode_byte);
        let tgt_type = extract_tgt_hll_type_enum(mode_byte);
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

/// Extract current mode from mode byte using serialization module
fn extract_cur_mode_enum(mode_byte: u8) -> CurMode {
    match crate::hll::serialization::extract_cur_mode(mode_byte) {
        CUR_MODE_LIST => CurMode::List,
        CUR_MODE_SET => CurMode::Set,
        CUR_MODE_HLL => CurMode::Hll,
        _ => unreachable!(),
    }
}

/// Extract target HLL type from mode byte using serialization module
fn extract_tgt_hll_type_enum(mode_byte: u8) -> TgtHllType {
    match crate::hll::serialization::extract_tgt_hll_type(mode_byte) {
        TGT_HLL4 => TgtHllType::Hll4,
        TGT_HLL6 => TgtHllType::Hll6,
        TGT_HLL8 => TgtHllType::Hll8,
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
