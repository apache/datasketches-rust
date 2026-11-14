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

//! Bloom filters for probabilistic set membership testing.
//!
//! [`BloomFilter`] tests whether an element may belong to a set with no false negatives and a
//! tunable false-positive probability. Unlike resizing sketches, its capacity is fixed at
//! construction.
//!
//! Construct a filter with [`BloomFilter::with_accuracy`] (or
//! [`BloomFilter::with_accuracy_and_seed`]) from a target distinct-item count and false-positive
//! probability, or with [`BloomFilter::with_size`] (or [`BloomFilter::with_size_and_seed`]) to
//! specify bit count and hash count directly.
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
//! Skip empty strings and byte slices before inserting into the filter. For these inputs,
//! return `false` without querying the filter, including for queries combined with insertion.
//!
//! # Examples
//!
//! ```
//! use datasketches::bloom::BloomFilter;
//!
//! let mut filter = BloomFilter::with_accuracy(1000, 0.01).unwrap();
//! filter.insert("apple");
//! filter.insert(42_u64);
//! assert!(filter.contains("apple"));
//! assert!(!filter.contains("grape"));
//!
//! let manual = BloomFilter::with_size(10_000, 7).unwrap();
//! assert_eq!(manual.capacity(), 10_048);
//! assert_eq!(manual.num_hashes(), 7);
//!
//! let mut other = BloomFilter::with_accuracy(1000, 0.01).unwrap();
//! other.insert("banana");
//! filter.union(&other).unwrap();
//! assert!(filter.contains("apple"));
//! assert!(filter.contains("banana"));
//!
//! let restored = BloomFilter::deserialize(&filter.serialize()).unwrap();
//! assert_eq!(filter, restored);
//! ```
//!
//! # References
//!
//! * Bloom, Burton H. (1970). "Space/time trade-offs in hash coding with allowable errors"
//! * Kirsch and Mitzenmacher (2008). "Less Hashing, Same Performance: Building a Better Bloom
//!   Filter"

mod bit_array;
mod filter;

pub use self::filter::BloomFilter;
