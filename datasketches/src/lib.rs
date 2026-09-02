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

//! # Apache® DataSketches™ Core Rust Library Component
//!
//! This crate provides compact, mergeable summaries for answering queries over large data streams.
//! It implements a subset of the algorithms available in the other Apache DataSketches language
//! components.
//!
//! ## Enabling sketches
//!
//! Sketch implementations are opt-in Cargo features; this crate enables none by default. Enable
//! only the algorithms an application uses:
//!
//! ```text
//! cargo add datasketches --features hll,theta
//! ```
//!
//! Each feature exposes a same-named module. For example, `hll` exposes `datasketches::hll` and
//! `tdigest` exposes `datasketches::tdigest`.
//!
//! ## Choosing a sketch
//!
//! * Use `bloom` for probabilistic membership queries.
//! * Use `countmin` for point-frequency estimates and `frequencies` for discovering heavy hitters.
//! * Use `hll` for fast distinct counts, `cpc` for compact serialized distinct counts, or `theta`
//!   when set operations are required.
//! * Use `kll`, `req`, or `tdigest` for ranks and quantiles. KLL provides strong general-purpose
//!   rank accuracy, REQ targets configurable high- or low-rank accuracy, and T-Digest emphasizes
//!   distribution tails.
//! * Use `tuple` when retained Theta keys need application-defined summaries.
//!
//! See each module's documentation for accuracy, memory, serialization, and update examples.
//!
//! ## Cross-language hashing
//!
//! Compatible serialization does not by itself make ordinary Rust [`Hash`](std::hash::Hash)
//! input compatible with Java, C++, or Go. Rust strings and slices include type-specific framing,
//! and short integers require different widening rules for different sketch families. When
//! sketches must represent the same updates across languages, use the wrappers in [`hash::value`]:
//!
//! * `raw_bytes` for byte and string contents;
//! * `canonical_float` for floating-point values;
//! * `sign_extend` for short integers used with HLL and CPC;
//! * `natural_extend` for short integers used with Bloom filters.
//!
//! Other DataSketches implementations skip empty strings rather than hashing them. Check for empty
//! input before updating when that cross-language behavior is required.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

// See https://github.com/apache/datasketches-rust/issues/28 for more information.
#[cfg(target_endian = "big")]
compile_error!("datasketches does not support big-endian targets");

// sketches modules
#[cfg(feature = "bloom")]
pub mod bloom;
#[cfg(feature = "countmin")]
pub mod countmin;
#[cfg(feature = "cpc")]
pub mod cpc;
#[cfg(feature = "frequencies")]
pub mod frequencies;
#[cfg(feature = "hll")]
pub mod hll;
#[cfg(feature = "kll")]
pub mod kll;
#[cfg(feature = "req")]
pub mod req;
#[cfg(feature = "tdigest")]
pub mod tdigest;
#[cfg(any(feature = "theta", feature = "tuple"))]
mod thetafamily;
#[cfg(any(feature = "theta", feature = "tuple"))]
pub use self::thetafamily::common as thetacommon;
#[cfg(feature = "theta")]
pub use self::thetafamily::theta;
#[cfg(feature = "tuple")]
pub use self::thetafamily::tuple;
#[cfg(feature = "xor")]
pub mod xor;

// common modules
pub mod codec;
pub mod common;
pub mod error;
pub mod hash;
