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

//! Density sketch implementation for density estimation from streaming data.
//!
//! The sketch maintains a coreset of points using a compaction scheme and
//! provides density estimates at query points via a kernel function.
//!
//! # References
//!
//! - Zohar Karnin, Edo Liberty, "Discrepancy, Coresets, and Sketches in Machine Learning".
//! - Apache DataSketches C++ density sketch implementation (density_sketch.hpp).
//!
//! # Usage
//!
//! ```rust
//! # use datasketches::density::DensitySketch;
//! let mut sketch: DensitySketch<f64> = DensitySketch::new(10, 3);
//! sketch.update(vec![0.0, 0.0, 0.0]);
//! sketch.update(vec![1.0, 2.0, 3.0]);
//! let estimate = sketch.estimate(&[0.0, 0.0, 0.0]);
//! assert!(estimate > 0.0);
//! ```

mod serialization;
mod sketch;

pub use self::sketch::DensityItem;
pub use self::sketch::DensityKernel;
pub use self::sketch::DensitySketch;
pub use self::sketch::DensityValue;
pub use self::sketch::GaussianKernel;
