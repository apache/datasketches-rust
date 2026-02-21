// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    /// Theta Sketch for cardinality estimation.
    pub const THETA: Family = Family {
        id: 3,
        name: "THETA",
        min_pre_longs: 1,
        max_pre_longs: 3,
    };

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
        id: 18,
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
        id: 21,
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
