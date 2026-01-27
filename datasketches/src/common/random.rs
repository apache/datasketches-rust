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

//! Shared random utilities for sketches.

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Random number source for sketches.
pub trait RandomSource {
    /// Returns the next random 64-bit value.
    fn next_u64(&mut self) -> u64;

    /// Returns a random boolean value.
    fn next_bool(&mut self) -> bool {
        (self.next_u64() & 1) != 0
    }
}

/// Xorshift-based random generator for sketch operations.
#[derive(Debug, Clone, Copy)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    /// Creates a new generator using the provided seed.
    pub fn seeded(seed: u64) -> Self {
        let state = if seed == 0 { 0x9e3779b97f4a7c15 } else { seed };
        Self { state }
    }
}

impl Default for XorShift64 {
    fn default() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut seed = nanos as u64 ^ (std::process::id() as u64);
        if seed == 0 {
            seed = 0x9e3779b97f4a7c15;
        }
        Self::seeded(seed)
    }
}

impl RandomSource for XorShift64 {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}
