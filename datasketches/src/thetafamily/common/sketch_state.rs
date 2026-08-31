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

use crate::thetacommon::constants::MAX_THETA;

/// Observable metadata consumed by shared Theta-family set operations.
///
/// Empty sketches cannot carry a retained count, theta, or ordering claim in this representation.
/// A non-empty sketch may retain zero entries when theta screened every input.
#[derive(Clone, Copy, Debug)]
pub enum ThetaFamilySketchMetadata {
    Empty {
        seed_hash: u16,
    },
    NonEmpty {
        seed_hash: u16,
        theta: u64,
        ordered: bool,
        num_retained: usize,
    },
}

/// Canonical in-memory state for a compact Theta-family sketch.
///
/// The empty variant deliberately has neither retained entries nor theta. This makes the only
/// representable empty state use `MAX_THETA`, report exact mode, and serialize through the
/// canonical empty-image path. The non-empty variant may contain no retained entries after theta
/// screening.
#[derive(Clone, Debug)]
pub enum CompactSketchState<E> {
    Empty {
        seed_hash: u16,
    },
    NonEmpty {
        retained_entries: Vec<E>,
        theta: u64,
        seed_hash: u16,
        ordered: bool,
    },
}

impl<E> CompactSketchState<E> {
    pub fn empty(seed_hash: u16) -> Self {
        Self::Empty { seed_hash }
    }

    pub fn non_empty(retained_entries: Vec<E>, theta: u64, seed_hash: u16, ordered: bool) -> Self {
        Self::NonEmpty {
            retained_entries,
            theta,
            seed_hash,
            ordered,
        }
    }

    pub fn theta(&self) -> u64 {
        match self {
            Self::Empty { .. } => MAX_THETA,
            Self::NonEmpty { theta, .. } => *theta,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty { .. })
    }

    pub fn is_estimation_mode(&self) -> bool {
        matches!(self, Self::NonEmpty { theta, .. } if *theta < MAX_THETA)
    }

    pub fn seed_hash(&self) -> u16 {
        match self {
            Self::Empty { seed_hash } | Self::NonEmpty { seed_hash, .. } => *seed_hash,
        }
    }

    pub fn retained_entries(&self) -> &[E] {
        match self {
            Self::Empty { .. } => &[],
            Self::NonEmpty {
                retained_entries, ..
            } => retained_entries,
        }
    }

    pub fn retained_entries_capacity(&self) -> usize {
        match self {
            Self::Empty { .. } => 0,
            Self::NonEmpty {
                retained_entries, ..
            } => retained_entries.capacity(),
        }
    }

    pub fn is_ordered(&self) -> bool {
        match self {
            Self::Empty { .. } => true,
            Self::NonEmpty { ordered, .. } => *ordered,
        }
    }

    #[cfg(feature = "theta")]
    pub fn map_retained_entries<T>(self, mut f: impl FnMut(E) -> T) -> CompactSketchState<T> {
        match self {
            Self::Empty { seed_hash } => CompactSketchState::empty(seed_hash),
            Self::NonEmpty {
                retained_entries,
                theta,
                seed_hash,
                ordered,
            } => CompactSketchState::non_empty(
                retained_entries.into_iter().map(&mut f).collect(),
                theta,
                seed_hash,
                ordered,
            ),
        }
    }
}
