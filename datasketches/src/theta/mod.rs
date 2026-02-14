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

//! Theta sketch implementation for cardinality estimation.
//!
//! Theta sketch is a generalization of the Kth Minimum Value (KMV) sketch that uses
//! a hash table to store retained entries and a theta parameter (sampling threshold)
//! to control memory usage. When the hash table reaches capacity, theta is reduced
//! to maintain the nominal size k.
//!
//! # Overview
//!
//! Theta sketches provide approximate distinct count (cardinality) estimation with
//! configurable accuracy and memory usage. The implementation supports:
//!
//! - **ThetaSketch**: Mutable sketch for building from input data
//! - **CompactThetaSketch**: Immutable sketch for serialization and set operations
//!
//! # Usage
//!
//! ```rust
//! # use datasketches::theta::ThetaSketch;
//! let mut sketch = ThetaSketch::builder().build();
//! sketch.update("apple");
//! assert!(sketch.estimate() >= 1.0);
//! ```
//!
//! # Serialization
//!
//! Theta sketches can be serialized to a compact binary format that is compatible
//! with the Java and C++ DataSketches implementations:
//!
//! ```rust
//! # use datasketches::theta::{ThetaSketch, CompactThetaSketch};
//! let mut sketch = ThetaSketch::builder().build();
//! sketch.update("apple");
//!
//! // Serialize to bytes
//! let bytes = sketch.serialize();
//!
//! // Deserialize
//! let restored = CompactThetaSketch::deserialize(&bytes).unwrap();
//! assert_eq!(sketch.estimate(), restored.estimate());
//! ```

mod compact;
mod hash_table;
mod serialization;
mod sketch;

pub use self::compact::CompactThetaSketch;
pub use self::sketch::ThetaSketch;
pub use self::sketch::ThetaSketchBuilder;
