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

use std::collections::HashSet;

use crate::aod::ArrayOfDoublesSketchView;
use crate::aod::CompactArrayOfDoublesSketch;
use crate::aod::policy::check_num_values;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::fold_hash::FoldHashState;

/// Set difference operator (`A and not B`) for ArrayOfDoubles sketches.
///
/// Surviving keys keep their values from `A` unchanged.
///
/// # Examples
///
/// ```
/// use datasketches::aod::ArrayOfDoublesANotB;
/// use datasketches::aod::ArrayOfDoublesSketchBuilder;
///
/// let mut a = ArrayOfDoublesSketchBuilder::default().build()?;
/// a.update("apple", [1.0])?;
/// a.update("banana", [2.0])?;
/// let mut b = ArrayOfDoublesSketchBuilder::default().build()?;
/// b.update("banana", [20.0])?;
///
/// let diff = ArrayOfDoublesANotB::default().compute(&a, &b, true)?;
/// assert_eq!(diff.num_retained(), 1);
/// assert_eq!(diff.iter().next().unwrap().values(), &[1.0]);
/// # Ok::<(), datasketches::error::Error>(())
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ArrayOfDoublesANotB {
    seed_hash: u16,
}

impl Default for ArrayOfDoublesANotB {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED).unwrap()
    }
}

impl ArrayOfDoublesANotB {
    /// Creates a set difference operator for `seed`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the computed seed hash is zero.
    pub fn with_seed(seed: u64) -> Result<Self, Error> {
        Ok(Self {
            seed_hash: compute_seed_hash(seed, ErrorKind::InvalidArgument)?,
        })
    }

    /// Computes `a and not b`.
    ///
    /// If `ordered` is `true`, retained entries are sorted ascending by hash.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `a` and `b` have different `num_values`, or if either
    /// non-empty input has a seed hash that differs from this operator's seed.
    pub fn compute<'a, 'b>(
        &self,
        a: impl Into<ArrayOfDoublesSketchView<'a>>,
        b: impl Into<ArrayOfDoublesSketchView<'b>>,
        ordered: bool,
    ) -> Result<CompactArrayOfDoublesSketch, Error> {
        let (a, b) = (a.into(), b.into());
        check_num_values(a.num_values(), usize::from(b.num_values()), "A-not-B input")?;
        if !a.is_empty() {
            check_seed_hash(
                self.seed_hash,
                a.seed_hash(),
                "A",
                ErrorKind::InvalidArgument,
            )?;
        }
        if !b.is_empty() {
            check_seed_hash(
                self.seed_hash,
                b.seed_hash(),
                "B",
                ErrorKind::InvalidArgument,
            )?;
        }
        if a.is_empty() {
            return Ok(CompactArrayOfDoublesSketch::empty(
                self.seed_hash,
                a.num_values(),
            ));
        }
        if b.is_empty() {
            if let Some(table) = a.table {
                return Ok(table.to_non_empty_compact(ordered));
            }
            return Ok(CompactArrayOfDoublesSketch::compact_non_empty(
                a.keys.to_vec(),
                a.values.to_vec(),
                a.theta64(),
                self.seed_hash,
                a.num_values(),
                a.is_ordered(),
                ordered,
            ));
        }

        let theta = a.theta64().min(b.theta64());
        let (hashes, values) = if b.num_retained() == 0 {
            retain_not_in_b(a, theta, |_| false)
        } else if let Some(b_table) = b.table {
            retain_not_in_b(a, theta, |hash| b_table.find_values(hash).is_some())
        } else if a.is_ordered() && b.is_ordered() {
            let mut remaining = b.keys.iter().copied().peekable();
            retain_not_in_b(a, theta, |hash| {
                while remaining.next_if(|&other| other < hash).is_some() {}
                remaining.peek() == Some(&hash)
            })
        } else {
            let mut excluded = HashSet::<u64, FoldHashState>::with_capacity_and_hasher(
                b.num_retained(),
                FoldHashState::new(),
            );
            for &hash in b.keys {
                if hash < theta {
                    excluded.insert(hash);
                } else if b.is_ordered() {
                    break;
                }
            }
            retain_not_in_b(a, theta, |hash| excluded.contains(&hash))
        };

        if hashes.is_empty() && theta == MAX_THETA {
            Ok(CompactArrayOfDoublesSketch::empty(
                self.seed_hash,
                a.num_values(),
            ))
        } else {
            Ok(CompactArrayOfDoublesSketch::compact_non_empty(
                hashes,
                values,
                theta,
                self.seed_hash,
                a.num_values(),
                a.is_ordered(),
                ordered,
            ))
        }
    }
}

fn retain_not_in_b(
    a: ArrayOfDoublesSketchView<'_>,
    theta: u64,
    mut contains: impl FnMut(u64) -> bool,
) -> (Vec<u64>, Vec<f64>) {
    let mut hashes = Vec::new();
    let mut values = Vec::new();
    let ordered = a.is_ordered();
    for entry in a.iter() {
        let hash = entry.hash();
        if hash < theta {
            if !contains(hash) {
                hashes.push(hash);
                values.extend_from_slice(entry.values());
            }
        } else if ordered {
            break;
        }
    }
    (hashes, values)
}
