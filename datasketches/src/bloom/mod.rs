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

//! Bloom Filter implementation for probabilistic set membership testing.
//!
//! A Bloom filter is a space-efficient probabilistic data structure used to test whether
//! an element is a member of a set. False positive matches are possible, but false negatives
//! are not. In other words, a query returns either "possibly in set" or "definitely not in set".
//!
//! # Properties
//!
//! * **No false negatives**: If an item was inserted, `contains()` will always return `true`
//! * **Possible false positives**: `contains()` may return `true` for items never inserted
//! * **Fixed size**: Unlike typical sketches, Bloom filters do not resize automatically
//! * **Linear space**: Size is proportional to the expected number of distinct items
//!
//! These guarantees describe normal operation. After [`invert()`](BloomFilter::invert) neither
//! the no-false-negative nor the false-positive guarantee holds; see its documentation.
//!
//! # Usage
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use datasketches::bloom::BloomFilter;
//! use datasketches::bloom::BloomFilterBuilder;
//!
//! // Create a filter optimized for 1000 items with 1% false positive rate
//! let mut filter = BloomFilterBuilder::with_accuracy(1000, 0.01).build()?;
//!
//! // Insert items
//! filter.insert("apple");
//! filter.insert("banana");
//! filter.insert(42_u64);
//!
//! // Check membership
//! assert!(filter.contains(&"apple")); // true - possibly present (and known to be inserted here)
//! assert!(!filter.contains(&"grape")); // false - definitely not present
//!
//! // Get statistics
//! println!("Capacity: {} bits", filter.capacity());
//! println!("Bits used: {}", filter.bits_used());
//! println!("Est. FPP: {:.4}%", filter.estimated_fpp() * 100.0);
//! # Ok(())
//! # }
//! ```
//!
//! # Creating Filters
//!
//! There are two ways to create a Bloom filter:
//!
//! ## By Accuracy (Recommended)
//!
//! Automatically calculates optimal size and hash functions:
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use datasketches::bloom::BloomFilterBuilder;
//!
//! let filter = BloomFilterBuilder::with_accuracy(
//!     10_000, // Expected max items
//!     0.01,   // Target false positive probability (1%)
//! )
//! .seed(9001) // Optional: custom seed
//! .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## By Size (Manual)
//!
//! Specify requested bit count and hash functions (rounded up to a multiple of 64 bits):
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use datasketches::bloom::BloomFilterBuilder;
//!
//! let filter = BloomFilterBuilder::with_size(
//!     95_851, // Number of bits
//!     7,      // Number of hash functions
//! )
//! .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Set Operations
//!
//! Bloom filters support efficient set operations:
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use datasketches::bloom::BloomFilterBuilder;
//!
//! let mut filter1 = BloomFilterBuilder::with_accuracy(100, 0.01).build()?;
//! let mut filter2 = BloomFilterBuilder::with_accuracy(100, 0.01).build()?;
//!
//! filter1.insert("a");
//! filter2.insert("b");
//!
//! // Union: recognizes items from either filter
//! filter1.union(&filter2);
//! assert!(filter1.contains(&"a"));
//! assert!(filter1.contains(&"b"));
//!
//! // Intersect: recognizes only items in both filters
//! // filter1.intersect(&filter2);
//!
//! // Invert: approximately inverts set membership
//! // filter1.invert();
//! # Ok(())
//! # }
//! ```
//!
//! # Implementation Details
//!
//! * Uses XXHash64 for hashing
//! * Implements double hashing (Kirsch-Mitzenmacher method) for k hash functions
//! * Bits packed efficiently in `u64` words
//! * Compatible serialization format (family ID: 21)
//!
//! # References
//!
//! * Bloom, Burton H. (1970). "Space/time trade-offs in hash coding with allowable errors"
//! * Kirsch and Mitzenmacher (2008). "Less Hashing, Same Performance: Building a Better Bloom
//!   Filter"

mod sketch;

pub use self::sketch::BloomFilter;
pub use self::sketch::BloomFilterBuilder;
