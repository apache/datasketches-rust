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

//! Xor filters for immutable probabilistic set membership.
//!
//! An xor filter is built from the complete input set and cannot be updated afterward. Compared
//! with a Bloom filter at the same false-positive probability, it generally needs less space and
//! performs exactly three fingerprint reads per query. Construction needs temporary space linear
//! in the number of distinct hashes.
//!
//! The [`XorFilterBuilder`] hashes ordinary Rust values with xxHash64. Call
//! [`XorFilterBuilder::update_hash`] or [`XorFilter::from_hashes`] when hashes have already been
//! computed and must not be hashed again.
//!
//! # Cross-language hashing
//!
//! The serialized filter representation is portable, but Rust's [`Hash`](std::hash::Hash)
//! implementations do not always encode values like other languages. Use the strategies in
//! [`crate::hash::value`] when the input hashes must match another DataSketches implementation.
//! A filter built through the precomputed-hash APIs remains portable only when every reader uses
//! the same external hash function.
//!
//! # Examples
//!
//! ```
//! use datasketches::xor::XorFilterBuilder;
//! use datasketches::xor::XorFilterType;
//!
//! let mut builder = XorFilterBuilder::new(XorFilterType::Xor8);
//! builder.update("apple");
//! builder.update("banana");
//! let filter = builder.build().unwrap();
//!
//! assert!(filter.contains(&"apple"));
//! assert!(!filter.contains(&"grape"));
//! ```
//!
//! # References
//!
//! * Graf and Lemire, "Xor Filters: Faster and Smaller Than Bloom and Cuckoo Filters," ACM Journal
//!   of Experimental Algorithmics 25 (2020).

mod filter;
mod serialization;

pub use self::filter::XorFilter;
pub use self::filter::XorFilterBuilder;
pub use self::filter::XorFilterType;
