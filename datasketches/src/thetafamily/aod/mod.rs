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

//! ArrayOfDoubles extends Theta distinct-count estimation with fixed-size `f64` vector summaries
//! stored in contiguous arrays. Each update supplies `num_values` values, and repeated or unioned
//! keys combine values element by element.
//!
//! # Cross-language hashing
//!
//! For cross-language use, apply the following input conventions to keys:
//!
//! * Use `i64`/`u64` for 64-bit integers and [`sign_extend`](crate::hash::value::sign_extend) for
//!   narrower integers.
//! * Use [`canonical_float`](crate::hash::value::canonical_float) for floating-point keys.
//! * Use [`raw_bytes`](crate::hash::value::raw_bytes) for UTF-8 strings and byte slices.
//!
//! Skip empty string and byte-slice keys before updating the sketch.
//!
//! # Cross-language compatibility
//!
//! * Interchange uses the compact ArrayOfDoubles binary format (`Family::TUPLE`, `sketch_type =
//!   3`). DataSketches Java's updatable `ArrayOfDoublesUpdatableSketch.toByteArray()` (`sketch_type
//!   = 2`) and `ArrayOfDoublesUnion.toByteArray()` (`sketch_type = 4`) must be compacted via
//!   `.compact()` or `.getResult()` before deserializing in Rust, C++, or Go.
//! * DataSketches Java reads `num_values` as a signed byte and accepts only `1..=127`. Values in
//!   `128..=255` can be exchanged with DataSketches C++ and Go, but not DataSketches Java.
//!
//! # Examples
//!
//! ```
//! use datasketches::aod::ArrayOfDoublesSketchBuilder;
//!
//! let mut sketch = ArrayOfDoublesSketchBuilder::default()
//!     .num_values(2)
//!     .build()?;
//! sketch.update("apple", [1.0, 10.0])?;
//! sketch.update("apple", [2.0, 20.0])?;
//!
//! let compact = sketch.compact(true);
//! let entry = compact.iter().next().unwrap();
//! assert_eq!(entry.values(), &[3.0, 30.0]);
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod a_not_b;
mod hash_table;
mod intersection;
mod jaccard_similarity;
mod policy;
mod serialization;
mod sketch;
mod union;

pub use self::a_not_b::ArrayOfDoublesANotB;
pub use self::intersection::ArrayOfDoublesIntersection;
pub use self::jaccard_similarity::ArrayOfDoublesJaccardSimilarity;
pub use self::policy::ArrayOfDoublesCombiner;
pub use self::sketch::ArrayOfDoublesEntry;
pub use self::sketch::ArrayOfDoublesSketch;
pub use self::sketch::ArrayOfDoublesSketchBuilder;
pub use self::sketch::ArrayOfDoublesSketchView;
pub use self::sketch::CompactArrayOfDoublesSketch;
pub use self::union::ArrayOfDoublesUnion;
pub use self::union::ArrayOfDoublesUnionBuilder;
