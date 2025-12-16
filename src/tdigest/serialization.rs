use crate::tdigest::TDigest;

const PREAMBLE_LONGS_EMPTY_OR_SINGLE: u8 = 1;
const PREAMBLE_LONGS_MULTIPLE: u8 = 2;
const SERIAL_VERSION: u8 = 1;
const TDIGEST_FAMILY_ID: u8 = 20;

impl TDigest {
    /// Serializes this TDigest to bytes.
    pub fn serialize(&mut self) -> Vec<u8> {
        self.compress();
        let mut bytes = vec![];
        bytes.push(match self.total_weight() {
            0 => PREAMBLE_LONGS_EMPTY_OR_SINGLE,
            1 => PREAMBLE_LONGS_EMPTY_OR_SINGLE,
            _ => PREAMBLE_LONGS_MULTIPLE,
        });
        bytes.push(SERIAL_VERSION);
        bytes.push(TDIGEST_FAMILY_ID);
        bytes
    }
}
