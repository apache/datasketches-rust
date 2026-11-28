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

//! ArrayOfNumbers extends Theta distinct-count estimation with fixed-width numeric vector
//! summaries stored in contiguous arrays. Each update supplies `num_values` values, and repeated
//! or unioned keys combine values element by element. `ArrayOfDoubles` names are retained as `f64`
//! aliases.
//!
//! The initial generic implementation supports `f32` and `f64` elements. `num_values` counts
//! elements, not bytes. Compact images do not tag the element type, so callers must deserialize
//! with the correct `T`.
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
//! * Interchange uses the compact ArrayOfNumbers binary format (`Family::TUPLE`, `sketch_type =
//!   3`). DataSketches Java's updatable `ArrayOfDoublesUpdatableSketch.toByteArray()` (`sketch_type
//!   = 2`) and `ArrayOfDoublesUnion.toByteArray()` (`sketch_type = 4`) must be compacted via
//!   `.compact()` or `.getResult()` before deserializing in Rust, C++, or Go.
//! * DataSketches Java supports only `f64` and reads `num_values` as a signed byte, accepting only
//!   `1..=127`. Values in `128..=255`, and all `f32` images, can be exchanged with DataSketches C++
//!   and Go, but not DataSketches Java.
//!
//! # Examples
//!
//! ```
//! use datasketches::aod::ArrayOfNumbersSketchBuilder;
//!
//! let mut sketch = ArrayOfNumbersSketchBuilder::<f32>::default()
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
mod policy;
mod serialization;
mod sketch;
mod union;

pub use self::a_not_b::ArrayOfNumbersANotB;
pub use self::intersection::ArrayOfNumbersIntersection;
pub use self::policy::ArrayOfDoublesCombiner;
pub use self::policy::ArrayOfNumbersCombiner;
pub use self::policy::ArrayOfNumbersValue;
pub use self::sketch::ArrayOfNumbersEntry;
pub use self::sketch::ArrayOfNumbersSketch;
pub use self::sketch::ArrayOfNumbersSketchBuilder;
pub use self::sketch::ArrayOfNumbersSketchView;
pub use self::sketch::CompactArrayOfNumbersSketch;
pub use self::union::ArrayOfNumbersUnion;
pub use self::union::ArrayOfNumbersUnionBuilder;

/// A retained hash and its `f64` values.
pub type ArrayOfDoublesEntry<'a> = ArrayOfNumbersEntry<'a, f64>;
/// Mutable ArrayOfDoubles sketch using `f64` summaries.
pub type ArrayOfDoublesSketch = ArrayOfNumbersSketch<f64>;
/// Builder for [`ArrayOfDoublesSketch`].
pub type ArrayOfDoublesSketchBuilder = ArrayOfNumbersSketchBuilder<f64>;
/// Read-only view of a mutable or compact ArrayOfDoubles sketch.
pub type ArrayOfDoublesSketchView<'a> = ArrayOfNumbersSketchView<'a, f64>;
/// Compact, immutable ArrayOfDoubles sketch.
pub type CompactArrayOfDoublesSketch = CompactArrayOfNumbersSketch<f64>;
/// Union of ArrayOfDoubles sketches.
pub type ArrayOfDoublesUnion = ArrayOfNumbersUnion<f64>;
/// Builder for [`ArrayOfDoublesUnion`].
pub type ArrayOfDoublesUnionBuilder = ArrayOfNumbersUnionBuilder<f64>;
/// Intersection of ArrayOfDoubles sketches with a caller-supplied combiner.
pub type ArrayOfDoublesIntersection<C> = ArrayOfNumbersIntersection<f64, C>;
/// Set difference operator (`A and not B`) for ArrayOfDoubles sketches.
pub type ArrayOfDoublesANotB = ArrayOfNumbersANotB;
