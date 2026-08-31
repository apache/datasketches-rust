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

use std::num::NonZeroU64;

use crate::thetacommon::constants::MAX_THETA;

/// A validated Theta-family retention threshold.
///
/// Hash zero is reserved and hashes are made non-negative by dropping their high bit, so every
/// usable threshold is in `1..=MAX_THETA`.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ThetaThreshold(NonZeroU64);

impl ThetaThreshold {
    pub const MAX: Self = Self(NonZeroU64::new(MAX_THETA).unwrap());

    /// Creates a threshold if `value` is in the valid Theta-family range.
    pub fn try_new(value: u64) -> Option<Self> {
        let value = NonZeroU64::new(value)?;
        (value.get() <= MAX_THETA).then_some(Self(value))
    }

    /// Creates a threshold known by the caller to be valid.
    ///
    /// # Panics
    ///
    /// Panics if `value` is outside `1..=MAX_THETA`.
    pub fn new(value: u64) -> Self {
        Self::try_new(value)
            .unwrap_or_else(|| panic!("theta must be in [1, {MAX_THETA}], got {value}"))
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    pub fn is_estimation_mode(self) -> bool {
        self < Self::MAX
    }
}

/// Canonical state exposed by a Theta-family sketch.
///
/// `NonEmpty` means that the sketch has observed or represents non-empty input. It may still
/// retain zero entries when every observed hash was screened out by theta.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThetaSketchState {
    Empty,
    NonEmpty { theta: ThetaThreshold },
}

impl ThetaSketchState {
    pub fn non_empty(theta: ThetaThreshold) -> Self {
        Self::NonEmpty { theta }
    }

    pub fn theta(self) -> ThetaThreshold {
        match self {
            Self::Empty => ThetaThreshold::MAX,
            Self::NonEmpty { theta } => theta,
        }
    }

    pub fn is_empty(self) -> bool {
        matches!(self, Self::Empty)
    }

    pub fn is_estimation_mode(self) -> bool {
        matches!(self, Self::NonEmpty { theta } if theta.is_estimation_mode())
    }
}

/// Whether an update sketch has observed an update call since construction or reset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateSketchState {
    NeverUpdated,
    Updated,
}

impl UpdateSketchState {
    pub fn theta_sketch_state(self, retention_theta: ThetaThreshold) -> ThetaSketchState {
        match self {
            Self::NeverUpdated => ThetaSketchState::Empty,
            Self::Updated => ThetaSketchState::non_empty(retention_theta),
        }
    }
}

/// Observable metadata consumed by shared Theta-family set operations.
///
/// Empty sketches cannot carry a retained count, theta, or ordering claim in this representation.
#[derive(Clone, Copy, Debug)]
pub enum ThetaSketchMetadata {
    Empty {
        seed_hash: u16,
    },
    NonEmpty {
        seed_hash: u16,
        theta: ThetaThreshold,
        ordered: bool,
        num_retained: usize,
    },
}

impl ThetaSketchMetadata {
    pub fn from_theta_sketch_state(
        seed_hash: u16,
        theta_sketch_state: ThetaSketchState,
        ordered: bool,
        num_retained: usize,
    ) -> Self {
        match theta_sketch_state {
            ThetaSketchState::Empty => {
                debug_assert_eq!(num_retained, 0);
                Self::Empty { seed_hash }
            }
            ThetaSketchState::NonEmpty { theta } => Self::NonEmpty {
                seed_hash,
                theta,
                ordered,
                num_retained,
            },
        }
    }

    pub fn seed_hash(self) -> u16 {
        match self {
            Self::Empty { seed_hash } | Self::NonEmpty { seed_hash, .. } => seed_hash,
        }
    }

    pub fn theta_sketch_state(self) -> ThetaSketchState {
        match self {
            Self::Empty { .. } => ThetaSketchState::Empty,
            Self::NonEmpty { theta, .. } => ThetaSketchState::non_empty(theta),
        }
    }

    pub fn is_empty(self) -> bool {
        matches!(self, Self::Empty { .. })
    }

    pub fn theta(self) -> ThetaThreshold {
        self.theta_sketch_state().theta()
    }

    pub fn is_ordered(self) -> bool {
        match self {
            Self::Empty { .. } => true,
            Self::NonEmpty { ordered, .. } => ordered,
        }
    }

    pub fn num_retained(self) -> usize {
        match self {
            Self::Empty { .. } => 0,
            Self::NonEmpty { num_retained, .. } => num_retained,
        }
    }
}

/// Canonical in-memory state for a compact Theta-family sketch.
///
/// The empty variant deliberately has neither retained entries nor theta. This makes the only
/// representable empty state use `MAX_THETA`, report exact mode, and serialize through the
/// canonical empty-image path.
#[derive(Clone, Debug)]
pub enum CompactSketchState<E> {
    Empty {
        seed_hash: u16,
    },
    NonEmpty {
        retained_entries: Vec<E>,
        theta: ThetaThreshold,
        seed_hash: u16,
        ordered: bool,
    },
}

impl<E> CompactSketchState<E> {
    pub fn empty(seed_hash: u16) -> Self {
        Self::Empty { seed_hash }
    }

    pub fn non_empty(
        retained_entries: Vec<E>,
        theta: ThetaThreshold,
        seed_hash: u16,
        ordered: bool,
    ) -> Self {
        Self::NonEmpty {
            retained_entries,
            theta,
            seed_hash,
            ordered,
        }
    }

    pub fn theta_sketch_state(&self) -> ThetaSketchState {
        match self {
            Self::Empty { .. } => ThetaSketchState::Empty,
            Self::NonEmpty { theta, .. } => ThetaSketchState::non_empty(*theta),
        }
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
