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

//! XOR filters for immutable probabilistic set membership.
//!
//! Use [`XorFilterBuilder`] to build a filter from values or precomputed hashes. A negative
//! query result rules out membership. A positive result may be a false positive.
//!
//! # Cross-language hashing
//!
//! For cross-language use, apply the following input conventions:
//!
//! * Use `i64`/`u64` for 64-bit integers and [`natural_extend`](crate::hash::value::natural_extend)
//!   for narrower integers.
//! * Use [`canonical_float`](crate::hash::value::canonical_float) for floating-point values.
//! * Use [`raw_bytes`](crate::hash::value::raw_bytes) for UTF-8 strings and byte slices.
//!
//! Skip empty strings and byte slices when building the filter. For these inputs, return
//! `false` without querying the filter.
//!
//! Value-based APIs use xxHash64 with seed `0`. For precomputed hashes, use the same hash function
//! when building and querying the filter, and use the precomputed-hash APIs throughout.
//!
//! # Examples
//!
//! ```
//! use datasketches::xor::XorFilterBuilder;
//! use datasketches::xor::XorFilterType;
//!
//! let mut builder = XorFilterBuilder::new(XorFilterType::Xor8, 0);
//! builder.update_values(["apple", "banana"]);
//! let filter = builder.build().unwrap();
//!
//! assert!(filter.contains("apple"));
//! ```
//!
//! # References
//!
//! * Graf and Lemire, "Xor Filters: Faster and Smaller Than Bloom and Cuckoo Filters," ACM Journal
//!   of Experimental Algorithmics 25 (2020).

mod filter;

pub use self::filter::XorFilter;
pub use self::filter::XorFilterBuilder;
pub use self::filter::XorFilterType;
