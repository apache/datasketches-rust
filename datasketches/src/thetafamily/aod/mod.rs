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

//! Tuple sketches with a fixed-size array of `f64` summaries per retained key.
//!
//! ArrayOfDoubles combines Theta distinct-count estimation with additive vector summaries. Every
//! sketch has a `num_values` configuration, and each update supplies exactly that many values.
//! Repeated keys and keys shared by union inputs combine their values element by element.
//!
//! # Cross-language hashing
//!
//! For cross-language use, apply the following input conventions:
//!
//! * Use `i64`/`u64` for 64-bit integers and [`sign_extend`](crate::hash::value::sign_extend) for
//!   narrower integers.
//! * Use [`canonical_float`](crate::hash::value::canonical_float) for floating-point keys.
//! * Use [`raw_bytes`](crate::hash::value::raw_bytes) for UTF-8 strings and byte slices.
//!
//! Skip empty string and byte-slice keys before updating the sketch.
//!
//! # Examples
//!
//! ```
//! use datasketches::aod::ArrayOfDoublesSketchBuilder;
//!
//! let mut sketch = ArrayOfDoublesSketchBuilder::default()
//!     .num_values(2)
//!     .build()
//!     .unwrap();
//! sketch.update("apple", [1.0, 10.0]).unwrap();
//! sketch.update("apple", [2.0, 20.0]).unwrap();
//!
//! let compact = sketch.compact(true);
//! let entry = compact.iter().next().unwrap();
//! assert_eq!(entry.values(), &[3.0, 30.0]);
//! ```

mod a_not_b;
mod intersection;
mod policy;
mod serialization;
mod sketch;
mod union;

pub use self::a_not_b::ArrayOfDoublesANotB;
pub use self::intersection::ArrayOfDoublesIntersection;
pub use self::policy::ArrayOfDoublesCombiner;
pub use self::sketch::ArrayOfDoublesEntry;
pub use self::sketch::ArrayOfDoublesSketch;
pub use self::sketch::ArrayOfDoublesSketchBuilder;
pub use self::sketch::ArrayOfDoublesSketchView;
pub use self::sketch::CompactArrayOfDoublesSketch;
pub use self::union::ArrayOfDoublesUnion;
pub use self::union::ArrayOfDoublesUnionBuilder;
