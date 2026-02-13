use crate::error::Error;

/// Defines the various families of sketch and set operation classes.
///
/// A family defines a set of classes that share fundamental algorithms and behaviors. The classes
/// within a family may still differ by how they are stored and accessed.
pub struct Family {
    /// The byte ID for this family.
    pub id: u8,
    /// The name for this family.
    pub name: &'static str,
    /// The minimum preamble size for this family in longs (8-bytes integer).
    pub min_pre_longs: u8,
    /// The maximum preamble size for this family in longs (8-bytes integer).
    pub max_pre_longs: u8,
}

impl Family {
    /// The HLL family of sketches.
    pub const HLL: Family = Family {
        id: 7,
        name: "HLL",
        min_pre_longs: 1,
        max_pre_longs: 1,
    };

    /// The Frequency family of sketches.
    pub const FREQUENCY: Family = Family {
        id: 10,
        name: "FREQUENCY",
        min_pre_longs: 1,
        max_pre_longs: 4,
    };

    /// Compressed Probabilistic Counting (CPC) Sketch.
    pub const CPC: Family = Family {
        id: 16,
        name: "CPC",
        min_pre_longs: 1,
        max_pre_longs: 5,
    };

    /// CountMin Sketch
    pub const COUNTMIN: Family = Family {
        id: 17,
        name: "COUNTMIN",
        min_pre_longs: 2,
        max_pre_longs: 2,
    };

    /// T-Digest for estimating quantiles and ranks.
    pub const TDIGEST: Family = Family {
        id: 20,
        name: "TDIGEST",
        min_pre_longs: 1,
        max_pre_longs: 2,
    };

    /// Bloom Filter.
    pub const BLOOMFILTER: Family = Family {
        id: 24,
        name: "BLOOMFILTER",
        min_pre_longs: 3,
        max_pre_longs: 4,
    };
}

impl Family {
    pub fn validate_id(&self, family_id: u8) -> Result<(), Error> {
        if family_id != self.id {
            Err(Error::invalid_family(self.id, family_id, self.name))
        } else {
            Ok(())
        }
    }
}
