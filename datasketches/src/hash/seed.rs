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
use crate::error::ErrorKind;
use crate::hash::MurmurHash3X64128;

/// Computes and checks the 16-bit seed hash from the given long seed.
///
/// The computed seed hash must not be zero in order to maintain compatibility with older
/// serialized versions that did not have this concept.
///
/// # Panics
///
/// Panics if the computed seed hash is zero.
pub(crate) fn compute_seed_hash(seed: u64) -> u16 {
    use std::hash::Hasher;

    let mut hasher = MurmurHash3X64128::with_seed(0);
    hasher.write(&seed.to_le_bytes());
    let (h1, _) = hasher.finish128();
    let seed_hash = (h1 & 0xffff) as u16;
    assert_ne!(seed_hash, 0);
    seed_hash
}

/// Checks that an actual seed hash matches the expected seed hash.
pub(crate) fn check_seed_hash(
    expected: u16,
    actual: u16,
    name: &'static str,
    kind: ErrorKind,
) -> Result<(), Error> {
    if actual != expected {
        return Err(Error::new(
            kind,
            format!("incompatible seed hash of {name}: expected {expected}, got {actual}"),
        ));
    }
    Ok(())
}
