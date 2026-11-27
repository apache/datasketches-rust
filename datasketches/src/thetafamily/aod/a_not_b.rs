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

use crate::aod::ArrayOfDoublesSketchView;
use crate::aod::CompactArrayOfDoublesSketch;
use crate::aod::policy::check_num_values;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::tuple::TupleANotB;

/// Set difference operator (`A and not B`) for ArrayOfDoubles sketches.
///
/// Surviving keys keep their values from `A` unchanged.
#[derive(Debug, Clone, Copy)]
pub struct ArrayOfDoublesANotB {
    inner: TupleANotB,
    seed_hash: u16,
}

impl Default for ArrayOfDoublesANotB {
    fn default() -> Self {
        Self::with_seed(crate::hash::DEFAULT_UPDATE_SEED).unwrap()
    }
}

impl ArrayOfDoublesANotB {
    /// Creates a set difference operator for the given seed.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the computed seed hash is zero.
    pub fn with_seed(seed: u64) -> Result<Self, Error> {
        Ok(Self {
            inner: TupleANotB::with_seed(seed)?,
            seed_hash: compute_seed_hash(seed, ErrorKind::InvalidArgument)?,
        })
    }

    /// Computes `a and not b`.
    ///
    /// If `ordered` is `true`, retained entries are sorted ascending by hash.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the inputs have different `num_values` or a non-empty input
    /// has a seed hash that differs from this operator's seed.
    pub fn compute<'a, 'b>(
        &self,
        a: impl Into<ArrayOfDoublesSketchView<'a>>,
        b: impl Into<ArrayOfDoublesSketchView<'b>>,
        ordered: bool,
    ) -> Result<CompactArrayOfDoublesSketch, Error> {
        let a = a.into();
        let b = b.into();
        check_num_values(a.num_values(), usize::from(b.num_values()), "A-not-B input")?;
        if !a.is_empty() {
            check_seed_hash(
                self.seed_hash,
                a.seed_hash(),
                "ArrayOfDoubles A-not-B input A",
                ErrorKind::InvalidArgument,
            )?;
        }
        if !b.is_empty() {
            check_seed_hash(
                self.seed_hash,
                b.seed_hash(),
                "ArrayOfDoubles A-not-B input B",
                ErrorKind::InvalidArgument,
            )?;
        }
        let result = self
            .inner
            .compute(a.into_tuple_view(), b.into_tuple_view(), ordered)?;
        Ok(CompactArrayOfDoublesSketch::from_tuple(
            result,
            a.num_values(),
        ))
    }
}
