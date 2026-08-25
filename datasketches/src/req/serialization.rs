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

//! REQ sketch wire format — constants and helpers shared by sketch + compactor serdes.

use crate::codec::assert::ensure_preamble_longs_in;
use crate::codec::assert::ensure_serial_version_is;
use crate::error::Error;
use crate::req::INITIAL_SECTIONS_PER_COMPACTOR;
use crate::req::MIN_K;
use crate::req::nearest_even_section_size;

pub(super) const SERIAL_VERSION: u8 = 1;
pub(super) const PREAMBLE_INTS_EXACT: u8 = 2;
pub(super) const PREAMBLE_INTS_ESTIMATION: u8 = 4;
pub(super) const RAW_ITEMS_THRESHOLD: u64 = 4;

/// Flag bits — match the C++ enum order: RESERVED1, RESERVED2, IS_EMPTY, IS_HIGH_RANK, RAW_ITEMS,
/// IS_LEVEL_ZERO_SORTED.
pub(super) const FLAG_IS_EMPTY: u8 = 1 << 2;
pub(super) const FLAG_IS_HIGH_RANK: u8 = 1 << 3;
pub(super) const FLAG_RAW_ITEMS: u8 = 1 << 4;
pub(super) const FLAG_IS_LEVEL_ZERO_SORTED: u8 = 1 << 5;

fn section_growth_threshold(num_sections: u8) -> Option<u64> {
    num_sections
        .checked_sub(1)
        .and_then(|shift| 1u64.checked_shl(u32::from(shift)))
}

fn reachable_section_doublings(state: u64, num_sections: u8) -> Option<u32> {
    let mut sections = INITIAL_SECTIONS_PER_COMPACTOR;
    let mut doublings = 0;
    while sections < num_sections {
        if state < section_growth_threshold(sections)? {
            return None;
        }
        sections = sections.checked_mul(2)?;
        doublings += 1;
    }
    (sections == num_sections).then_some(doublings)
}

fn compatible_next_section_sizes(section_size_raw: f32) -> [Option<f32>; 2] {
    let single_precision = section_size_raw / std::f32::consts::SQRT_2;
    let widened = (f64::from(section_size_raw) / std::f64::consts::SQRT_2) as f32;
    [
        (nearest_even_section_size(single_precision) >= u32::from(MIN_K))
            .then_some(single_precision),
        (nearest_even_section_size(section_size_raw) > u32::from(MIN_K)
            && nearest_even_section_size(widened) >= u32::from(MIN_K))
        .then_some(widened),
    ]
}

fn has_reachable_section_configuration(
    k: u16,
    state: u64,
    section_size_raw: f32,
    num_sections: u8,
) -> bool {
    let Some(doublings) = reachable_section_doublings(state, num_sections) else {
        return false;
    };

    // The wire format exposes the running floating-point section size. C++
    // updates it in single precision, while Java divides in double precision
    // before narrowing to f32. Accept either result at every step so a sketch
    // can be deserialized and continued in another implementation.
    let mut candidates = vec![k as f32];
    for _ in 0..doublings {
        let mut next = Vec::with_capacity(candidates.len() * 2);
        for candidate in candidates {
            for value in compatible_next_section_sizes(candidate)
                .into_iter()
                .flatten()
            {
                if !next.contains(&value) {
                    next.push(value);
                }
            }
        }
        candidates = next;
    }
    if !candidates
        .iter()
        .any(|candidate| candidate.to_bits() == section_size_raw.to_bits())
    {
        return false;
    }

    // If every compatible producer would already have doubled the section
    // count at this state, the serialized configuration is stale.
    section_growth_threshold(num_sections).is_none_or(|threshold| {
        state < threshold
            || compatible_next_section_sizes(section_size_raw)
                .iter()
                .any(Option::is_none)
    })
}

pub(super) fn validate_compactor_state(
    k: u16,
    expected_lg_weight: u8,
    state: u64,
    section_size_raw: f32,
    lg_weight: u8,
    num_sections: u8,
) -> Result<(), Error> {
    if lg_weight != expected_lg_weight {
        return Err(Error::deserial(format!(
            "REQ compactor lg_weight {lg_weight} does not match level {expected_lg_weight}"
        )));
    }
    if !has_reachable_section_configuration(k, state, section_size_raw, num_sections) {
        return Err(Error::deserial(format!(
            "REQ compactor section configuration is not reachable (k={k}, state={state}, section_size_raw={section_size_raw}, num_sections={num_sections})"
        )));
    }
    Ok(())
}

pub(super) fn check_serial_version(actual: u8) -> Result<(), Error> {
    ensure_serial_version_is(SERIAL_VERSION, actual)
}

pub(super) fn check_preamble_ints(actual: u8, num_levels: u8) -> Result<(), Error> {
    let expected = if num_levels > 1 {
        PREAMBLE_INTS_ESTIMATION
    } else {
        PREAMBLE_INTS_EXACT
    };
    ensure_preamble_longs_in(&[expected], actual)
}
