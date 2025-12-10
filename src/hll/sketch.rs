use std::io;

const HLL_FAMILY_ID: u8 = 7;

pub struct HllSketch {
    lg_config_k: u8,
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

        // Read preamble
        let _preamble_longs = bytes[0];
        let _version = bytes[1];
        let family_id = bytes[2];
        let lg_config_k = bytes[3];
        let _lg_arr = bytes[4];
        let _flags = bytes[5];

        // Verify family ID (HLL = 7)
        if family_id != HLL_FAMILY_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid family: expected 7 (HLL), got {}", family_id),
            ));
        }

        // Verify lg_k range (4-21 are valid)
        if lg_config_k < 4 || lg_config_k > 21 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid lg_k: {}, must be in [4; 21]", lg_config_k),
            ));
        }

        // TODO: Parse flags to determine mode and type
        // TODO: Deserialize mode-specific data
        // TODO: Reconstruct appropriate container (List, Set, or HLL Array)

        Ok(HllSketch { lg_config_k })
    }
    
    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        Ok(vec![])
    }
}
