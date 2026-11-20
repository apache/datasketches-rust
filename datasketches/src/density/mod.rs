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

//! Density sketch for streaming kernel density estimation and coreset construction.
//!
//! [`DensitySketch`] maintains a weighted coreset of multidimensional floating-point vectors so
//! kernel density estimates can be evaluated at arbitrary query points using bounded memory. It is
//! based on [Discrepancy, Coresets, and Sketches in Machine Learning](https://proceedings.mlr.press/v99/karnin19a/karnin19a.pdf)
//! by Zohar Karnin and Edo Liberty.
//!
//! Point coordinates use `f32` or `f64` ([`DensityValue`]). [`DensitySketch::new`] uses the
//! default [`GaussianKernel`]; supply a custom [`Kernel`] with [`DensitySketch::with_kernel`]. The
//! binary serialization format supports cross-language interchange when both sides use family ID
//! `23`, the same coordinate type, and equivalent kernels.
//!
//! # Examples
//!
//! ```
//! use datasketches::density::DensitySketch;
//!
//! let mut sketch = DensitySketch::<f64>::new(100, 2)?;
//! sketch.update(&[0.0, 0.0])?;
//! sketch.update(&[1.0, 1.0])?;
//!
//! let Some(density) = sketch.estimate(&[0.0, 0.0])? else {
//!     return Ok(());
//! };
//! assert!(density > 0.5 && density <= 1.0);
//! # Ok::<(), datasketches::error::Error>(())
//! ```

mod iter;
mod kernel;
mod serialization;
mod sketch;
mod value;

pub use self::iter::DensitySketchIter;
pub use self::kernel::GaussianKernel;
pub use self::kernel::Kernel;
pub use self::sketch::DensitySketch;
pub use self::value::DensityValue;

const MIN_K: u16 = 2;
const MAX_LEVELS: usize = 64;
