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

pub enum Mode {
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
                format!("invalid serialization version: expected {}, got {}", SER_VER, ser_ver),
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
                        format!("invalid preamble ints for LIST mode: expected {}, got {}", LIST_PREINTS, preamble_ints),
                    ));
                }
                deserialize_list(bytes, lg_config_k, empty, compact, ooo)?
            }
            CurMode::Set => {
                if preamble_ints != HASH_SET_PREINTS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid preamble ints for SET mode: expected {}, got {}", HASH_SET_PREINTS, preamble_ints),
                    ));
                }
                deserialize_set(bytes, lg_config_k, compact)?
            }
            CurMode::Hll => {
                if preamble_ints != HLL_PREINTS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid preamble ints for HLL mode: expected {}, got {}", HLL_PREINTS, preamble_ints),
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
fn deserialize_list(
    bytes: &[u8],
    lg_config_k: u8,
    empty: bool,
    compact: bool,
    _ooo: bool,
) -> io::Result<Mode> {
    // Read coupon count from byte 6
    let coupon_count = bytes[LIST_COUNT_BYTE] as usize;

    // Compute array size
    let lg_arr = bytes[LG_ARR_BYTE] as usize;
    let array_size = if compact {
        coupon_count
    } else {
        1 << lg_arr
    };

    // Validate length
    let expected_len = LIST_INT_ARR_START + (array_size * 4);
    if bytes.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("LIST data too short: expected {}, got {}", expected_len, bytes.len()),
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

    let list = List::from_coupons(lg_arr, coupons.into_boxed_slice(), coupon_count);
    Ok(Mode::List(list))
}

/// Deserialize SET mode sketch
fn deserialize_set(
    _bytes: &[u8],
    _lg_config_k: u8,
    _compact: bool,
) -> io::Result<Mode> {
    // TODO: Implement SET deserialization
    Ok(Mode::Set(HashSet::default()))
}

/// Deserialize HLL mode sketch
fn deserialize_hll(
    _bytes: &[u8],
    lg_config_k: u8,
    tgt_type: TgtHllType,
    _compact: bool,
    _ooo: bool,
) -> io::Result<Mode> {
    // TODO: Implement HLL deserialization
    match tgt_type {
        TgtHllType::Hll4 => Ok(Mode::Array4(Array4::new(lg_config_k))),
        TgtHllType::Hll6 => Ok(Mode::Array6(Array6::new(lg_config_k))),
        TgtHllType::Hll8 => Ok(Mode::Array8(Array8::new(lg_config_k))),
    }
}

/// Serialize LIST mode sketch
fn serialize_list(
    list: &List,
    lg_config_k: u8,
    tgt_hll_type: TgtHllType,
) -> io::Result<Vec<u8>> {
    let compact = true; // Always use compact format
    let empty = list.container.len == 0;
    let coupon_count = list.container.len;
    let lg_arr = list.container.lg_size;

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

    // Write mode byte: low 2 bits = current mode (0=LIST), bits 2-3 = target type
    bytes[MODE_BYTE] = (tgt_hll_type as u8) << 2; // Current mode is LIST (0)

    // Write coupons (only non-empty ones if compact)
    if !empty {
        let mut write_idx = 0;
        for coupon in list.container.coupons.iter() {
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

/// Serialize SET mode sketch
fn serialize_set(
    _set: &HashSet,
    _lg_config_k: u8,
    _tgt_hll_type: TgtHllType,
) -> io::Result<Vec<u8>> {
    // TODO: Implement SET serialization
    Err(io::Error::new(
        io::ErrorKind::Other,
        "SET serialization not yet implemented",
    ))
}

/// Serialize HLL4 mode sketch
fn serialize_hll4(
    _arr: &Array4,
    _lg_config_k: u8,
) -> io::Result<Vec<u8>> {
    // TODO: Implement HLL4 serialization
    Err(io::Error::new(
        io::ErrorKind::Other,
        "HLL4 serialization not yet implemented",
    ))
}

/// Serialize HLL6 mode sketch
fn serialize_hll6(
    _arr: &Array6,
    _lg_config_k: u8,
) -> io::Result<Vec<u8>> {
    // TODO: Implement HLL6 serialization
    Err(io::Error::new(
        io::ErrorKind::Other,
        "HLL6 serialization not yet implemented",
    ))
}

/// Serialize HLL8 mode sketch
fn serialize_hll8(
    _arr: &Array8,
    _lg_config_k: u8,
) -> io::Result<Vec<u8>> {
    // TODO: Implement HLL8 serialization
    Err(io::Error::new(
        io::ErrorKind::Other,
        "HLL8 serialization not yet implemented",
    ))
}
