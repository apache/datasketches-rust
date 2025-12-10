use std::io;

use crate::hll::HllType;
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

pub struct HllSketch {
    lg_config_k: u8,
    mode: Mode,
}

impl HllSketch {
    /// Check if two sketches are functionally equal
    pub fn equals(&self, other: &Self) -> bool {
        if self.lg_config_k != other.lg_config_k {
            return false;
        }

        match (&self.mode, &other.mode) {
            (Mode::List { list: l1, .. }, Mode::List { list: l2, .. }) => l1 == l2,
            (Mode::Set { set: s1, .. }, Mode::Set { set: s2, .. }) => s1 == s2,
            (Mode::Array4(a1), Mode::Array4(a2)) => a1 == a2,
            (Mode::Array6(a1), Mode::Array6(a2)) => a1 == a2,
            (Mode::Array8(a1), Mode::Array8(a2)) => a1 == a2,
            _ => false, // Different modes are not equal
        }
    }
}

enum Mode {
    List { list: List, hll_type: HllType },
    Set { set: HashSet, hll_type: HllType },
    Array4(Array4),
    Array6(Array6),
    Array8(Array8),
}

impl HllSketch {
    pub fn lg_config_k(&self) -> u8 {
        self.lg_config_k
    }

    /// Get the current cardinality estimate
    pub fn estimate(&self) -> f64 {
        match &self.mode {
            Mode::List { list, .. } => list.estimate(),
            Mode::Set { set, .. } => set.estimate(),
            Mode::Array4(arr) => arr.estimate(),
            Mode::Array6(arr) => arr.estimate(),
            Mode::Array8(arr) => arr.estimate(),
        }
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
        let hll_type = extract_hll_type_enum(mode_byte);
        let empty = (flags & EMPTY_FLAG_MASK) != 0;
        let compact = (flags & COMPACT_FLAG_MASK) != 0;
        let ooo = (flags & OUT_OF_ORDER_FLAG_MASK) != 0;

        // Deserialize based on mode
        let mode =
            match cur_mode {
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

                    let list = List::deserialize(bytes, empty, compact)?;
                    Mode::List { list, hll_type }
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

                    let set = HashSet::deserialize(bytes, compact)?;
                    Mode::Set { set, hll_type }
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

                    match hll_type {
                        HllType::Hll4 => Array4::deserialize(bytes, lg_config_k, compact, ooo)
                            .map(Mode::Array4)?,
                        HllType::Hll6 => Array6::deserialize(bytes, lg_config_k, compact, ooo)
                            .map(Mode::Array6)?,
                        HllType::Hll8 => Array8::deserialize(bytes, lg_config_k, compact, ooo)
                            .map(Mode::Array8)?,
                    }
                }
            };

        Ok(HllSketch { lg_config_k, mode })
    }

    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        match &self.mode {
            Mode::List { list, hll_type } => list.serialize(self.lg_config_k, *hll_type),
            Mode::Set { set, hll_type } => set.serialize(self.lg_config_k, *hll_type),
            Mode::Array4(arr) => arr.serialize(self.lg_config_k),
            Mode::Array6(arr) => arr.serialize(self.lg_config_k),
            Mode::Array8(arr) => arr.serialize(self.lg_config_k),
        }
    }
}

/// Extract current mode from mode byte using serialization module
fn extract_cur_mode_enum(mode_byte: u8) -> CurMode {
    match extract_cur_mode(mode_byte) {
        CUR_MODE_LIST => CurMode::List,
        CUR_MODE_SET => CurMode::Set,
        CUR_MODE_HLL => CurMode::Hll,
        _ => unreachable!(),
    }
}

/// Extract target HLL type from mode byte using serialization module
fn extract_hll_type_enum(mode_byte: u8) -> HllType {
    match extract_tgt_hll_type(mode_byte) {
        TGT_HLL4 => HllType::Hll4,
        TGT_HLL6 => HllType::Hll6,
        TGT_HLL8 => HllType::Hll8,
        _ => unreachable!(),
    }
}
