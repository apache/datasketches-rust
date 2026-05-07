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

//! Hashable value wrappers for sketches.
//!
//! Sketch update APIs accept any value that implements [`Hash`]. For most Rust values,
//! passing the value directly is sufficient. This module provides [`Canonical`] wrappers for
//! cases where the input must follow compatible canonicalization rules as other datasketches
//! implementation.
//!
//! Canonicalization is useful when the same logical value can have multiple Rust representations.
//! For example, `f32` and `f64` floating-point inputs are canonicalized through the same `f64`
//! representation, signed zero values hash the same, and all NaN values use one canonical NaN bit
//! pattern. Narrow integer inputs are canonicalized through datasketches-cpp's signed-extension
//! rules. Byte and string inputs are hashed as raw bytes without Rust's slice or string length
//! prefix.
//!
//! Empty byte and string inputs have zero bytes to hash. datasketches-cpp skips empty strings
//! before hashing, so check `Canonical::is_empty` before updating a sketch when that behavior
//! matters.

mod canonical;

use std::hash::Hash;
use std::hash::Hasher;

pub use self::canonical::Canonical;
pub use self::canonical::canonical_f32;
pub use self::canonical::canonical_f64;
pub use self::canonical::canonical_i8;
pub use self::canonical::canonical_i16;
pub use self::canonical::canonical_i32;
pub use self::canonical::canonical_i64;
pub use self::canonical::canonical_slice;
pub use self::canonical::canonical_str;
pub use self::canonical::canonical_string;
pub use self::canonical::canonical_u8;
pub use self::canonical::canonical_u16;
pub use self::canonical::canonical_u32;
pub use self::canonical::canonical_u64;
pub use self::canonical::canonical_vec;

#[doc(hidden)] // for doctest
pub fn calculate_hash<T: Hash>(t: T) -> u64 {
    use std::hash::DefaultHasher;

    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
