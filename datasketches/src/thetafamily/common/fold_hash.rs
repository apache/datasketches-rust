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

use std::hash::BuildHasher;
use std::hash::Hasher;

use crate::common::random::random_u64;

const FOLD_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;

#[inline]
fn folded_multiply(x: u64, y: u64) -> u64 {
    let product = u128::from(x) * u128::from(y);
    (product as u64) ^ ((product >> 64) as u64)
}

/// Per-operation random state for hashing pre-hashed 64-bit sketch keys.
///
/// Keys in Theta-family sketches are already uniform MurmurHash3 outputs, so SipHash is
/// unnecessary overhead, but compact sketches can be deserialized from untrusted bytes whose raw
/// hashes were chosen to collide. Seeding a 128-bit folded multiply from the thread-local PRNG on
/// each call prevents crafted inputs from forcing worst-case probe chains.
pub struct FoldHashState(u64);

impl FoldHashState {
    #[inline]
    pub fn new() -> Self {
        Self(random_u64())
    }
}

impl BuildHasher for FoldHashState {
    type Hasher = FoldHasher;

    #[inline]
    fn build_hasher(&self) -> FoldHasher {
        FoldHasher(self.0)
    }
}

pub struct FoldHasher(u64);

impl Hasher for FoldHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = folded_multiply(self.0 ^ u64::from(byte), FOLD_MULTIPLIER);
        }
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = folded_multiply(self.0 ^ value, FOLD_MULTIPLIER);
    }
}
