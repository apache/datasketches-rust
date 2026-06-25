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
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::xor::sketch::Fingerprint;
use crate::xor::sketch::Xor8;
use crate::xor::sketch::XorFilter;

const DEFAULT_MAX_ATTEMPTS: u32 = 20;

/// Builder for creating Xor filters.
///
/// Xor filters require distinct keys and are immutable after construction.
///
/// # Examples
///
/// ```
/// use datasketches::xor::Xor8;
///
/// let keys: Vec<u64> = (0..10_000).collect();
/// let filter = Xor8::builder()
///     .seed(42)
///     .max_attempts(25)
///     .build(&keys)
///     .unwrap();
///
/// assert!(filter.contains(9999));
/// ```
#[derive(Debug, Clone)]
pub struct XorFilterBuilder {
    seed: u64,
    max_attempts: u32,
}

impl Default for XorFilterBuilder {
    fn default() -> Self {
        Self {
            seed: DEFAULT_UPDATE_SEED,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }
}

impl XorFilterBuilder {
    /// Sets the hash seed used to construct the filter.
    ///
    /// Filters built with different seeds are incompatible.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::Xor8;
    ///
    /// let keys: Vec<u64> = (0..100).collect();
    /// let filter = Xor8::builder().seed(123).build(&keys).unwrap();
    /// assert!(filter.contains(10));
    /// ```
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Sets the maximum number of construction attempts.
    ///
    /// Construction can fail if the key set contains duplicates.
    ///
    /// # Panics
    ///
    /// Panics if `max_attempts` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::xor::Xor8;
    ///
    /// let keys: Vec<u64> = (0..100).collect();
    /// let filter = Xor8::builder().max_attempts(10).build(&keys).unwrap();
    /// assert!(filter.contains(10));
    /// ```
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        assert!(max_attempts > 0, "max_attempts must be at least 1");
        self.max_attempts = max_attempts;
        self
    }

    /// Builds an Xor8 filter from the provided keys.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::error::ErrorKind::InvalidArgument)
    /// if construction fails or parameters are invalid.
    pub fn build(self, keys: &[u64]) -> Result<Xor8, Error> {
        let core = self.build_with_fingerprint::<u8>(keys)?;
        Ok(Xor8 { core })
    }

    /// Builds an Xor filter with the specified fingerprint type.
    pub(super) fn build_with_fingerprint<Fp: Fingerprint>(
        self,
        keys: &[u64],
    ) -> Result<XorFilter<Fp>, Error> {
        XorFilter::build_from_keys(keys, self.seed, self.max_attempts)
    }
}
