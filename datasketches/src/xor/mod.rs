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
//! [`XorFilterBuilder::build_from_hashes`] or [`XorFilter::from_hashes`] when hashes have already
//! been computed and must not be hashed again.
//!
//! # Cross-language hashing
//!
//! Standard serialization supports 8-, 16-, and 32-bit fingerprints using family `22`, version `1`.
//! Interoperability requires the reader to support the encoded fingerprint width. The 8- and
//! 16-bit images match Java's format; 32-bit images extend the same layout with little-endian
//! `u32` slots and cannot be read by implementations limited to 8/16 bits.
//! Construction may choose a different valid fingerprint array from another implementation;
//! byte-for-byte equality of independently constructed filters is not guaranteed. Rust's
//! [`Hash`](std::hash::Hash) implementations do not always encode values like other languages. Use
//! the strategies in [`crate::hash::value`] when the input hashes must match another DataSketches
//! implementation. A filter built through the precomputed-hash APIs remains portable only when
//! every reader uses the same external hash function.
//!
//! Java hashes integer scalars as 64-bit little-endian values and strings as UTF-8 bytes with
//! xxHash64 seed `0`. Use `u64`/`i64` and [`crate::hash::value::raw_bytes`] accordingly. Java
//! skips empty strings; callers must do so explicitly here. External hashes (for example XXH3)
//! must use the precomputed-hash APIs throughout; Java's typed query API will not match them.
//!
//! # Application-defined storage
//!
//! [`XorFilter::from_parts`] and [`XorFilter::into_parts`] preserve the construction seed, segment
//! length, and typed fingerprint slots without adding metadata to an existing disk format.
//! Imported parts may omit the item count. Raw parts use the same query algorithm as `xorf`
//! 0.13 for all three widths, but builders need not produce identical fingerprint arrays.
//! Filters with a known zero item count always reject queries; imported parts with an unknown
//! count retain the ordinary fingerprint comparison, including possible false positives.
//!
//! # Examples
//!
//! ```
//! use datasketches::xor::XorFilterBuilder;
//! use datasketches::xor::XorFilterType;
//!
//! let filter = XorFilterBuilder::new(XorFilterType::Xor8)
//!     .build(["apple", "banana"])
//!     .unwrap();
//!
//! assert!(filter.contains(&"apple"));
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
pub use self::filter::XorFingerprints;
