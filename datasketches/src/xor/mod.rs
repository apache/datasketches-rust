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

//! Xor filter implementation for probabilistic set membership testing.
//!
//! Xor filters are immutable, space-efficient structures with no false negatives.
//! They are built from a set of distinct 64-bit keys and are optimized for fast lookups.
//!
//! # Usage
//!
//! ```rust
//! use datasketches::xor::Xor8;
//!
//! let keys: Vec<u64> = (0..10_000).collect();
//! let filter = Xor8::builder().build(&keys).unwrap();
//!
//! assert!(filter.contains(42));
//! ```
//!
//! # Notes
//!
//! - The input keys must be distinct. Duplicate keys can cause construction to fail.
//! - Xor filters are immutable once built.

mod builder;
mod sketch;

pub use self::builder::XorFilterBuilder;
pub use self::sketch::Xor8;
