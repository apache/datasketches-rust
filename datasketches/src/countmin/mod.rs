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

//! CountMin sketch implementation for frequency estimation.
//!
//! The CountMin sketch provides approximate frequency counts for streaming data
//! with configurable relative error and confidence bounds.
//!
//! # Cross-language hashing
//!
//! For version 2 interchange, use `i64` counters and the following input encodings:
//!
//! * Use `i64`/`u64` for eight-byte little-endian integers and
//!   [`natural_extend`](crate::hash::value::natural_extend) for narrower integers.
//! * Use [`raw_bytes`](crate::hash::value::raw_bytes) for UTF-8 strings and byte slices without
//!   length prefixes or terminators.
//!
//! The version 2 specification ignores empty strings and byte arrays. For cross-language use,
//! callers skip these updates and return zero for empty-input estimates and both bounds.
//!
//! # Usage
//!
//! This example handles empty strings before updating or querying. Byte slices use the same
//! `is_empty()` checks with `raw_bytes::from_slice`.
//!
//! ```
//! use datasketches::countmin::CountMinSketch;
//! use datasketches::hash::value::raw_bytes;
//!
//! let mut sketch = CountMinSketch::<i64>::new(3, 1000)?;
//! for input in ["", "apple", ""] {
//!     let item = raw_bytes::from_str(input);
//!     if !item.is_empty() {
//!         sketch.update_with_weight(item, 5);
//!     }
//! }
//! assert_eq!(sketch.total_weight(), 5);
//! assert_eq!(sketch.estimate(raw_bytes::from_str("apple")), 5);
//!
//! let item = raw_bytes::from_str("");
//! let (estimate, lower, upper) = if item.is_empty() {
//!     (0, 0, 0)
//! } else {
//!     (
//!         sketch.estimate(item),
//!         sketch.lower_bound(item),
//!         sketch.upper_bound(item),
//!     )
//! };
//! assert_eq!((estimate, lower, upper), (0, 0, 0));
//! # Ok::<(), datasketches::error::Error>(())
//! ```
//!
//! # Configuration Helpers
//!
//! ```
//! use datasketches::countmin::CountMinSketch;
//!
//! let buckets = CountMinSketch::<i64>::suggest_num_buckets(0.01).unwrap();
//! let hashes = CountMinSketch::<i64>::suggest_num_hashes(0.99).unwrap();
//! let sketch = CountMinSketch::<i64>::new(hashes, buckets).unwrap();
//! assert_eq!(sketch.estimate(1i64), 0);
//! ```

mod sketch;
pub use self::sketch::CountMinSketch;

mod value;
pub use self::value::CountMinValue;
pub use self::value::UnsignedCountMinValue;
