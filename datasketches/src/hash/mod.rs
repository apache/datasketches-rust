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

mod murmurhash;
mod xxhash;

pub(crate) use self::murmurhash::MurmurHash3X64128;
#[allow(unused_imports)]
pub(crate) use self::xxhash::XxHash64;

/// The seed 9001 used in the sketch update methods is a prime number that was chosen very early
/// on in experimental testing.
///
/// Choosing a seed is somewhat arbitrary, and the author cannot prove that this particular seed
/// is somehow superior to other seeds. There was some early Internet discussion that a seed of 0
/// did not produce as clean avalanche diagrams as non-zero seeds, but this may have been more
/// related to the MurmurHash2 release, which did have some issues. As far as the author can
/// determine, MurmurHash3 does not have these problems.
///
/// In order to perform set operations on two sketches it is critical that the same hash function
/// and seed are identical for both sketches, otherwise the assumed 1:1 relationship between the
/// original source key value and the hashed bit string would be violated. Once you have developed
/// a history of stored sketches you are stuck with it.
/// The default seed used for hashing.
pub(crate) const DEFAULT_UPDATE_SEED: u64 = 9001;

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

/// Reads an u64 from a byte slice in little-endian order.
///
/// # Panics
///
/// Panics if `bytes.len()` is greater than 8.
fn read_u64_le(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(buf)
}
