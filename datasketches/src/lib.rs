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
//! The Sketching Core Library provides a range of stochastic streaming algorithms and closely
//! related Rust technologies that are particularly useful when integrating this technology into
//! systems that must deal with massive data.
//!
//! This library is divided into modules that constitute distinct groups of functionality.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

// See https://github.com/apache/datasketches-rust/issues/28 for more information.
#[cfg(target_endian = "big")]
compile_error!("datasketches does not support big-endian targets");

#[cfg(feature = "bloom")]
#[cfg_attr(docsrs, doc(cfg(feature = "bloom")))]
pub mod bloom;
pub mod codec;
pub mod common;
#[cfg(feature = "countmin")]
#[cfg_attr(docsrs, doc(cfg(feature = "countmin")))]
pub mod countmin;
#[cfg(feature = "cpc")]
#[cfg_attr(docsrs, doc(cfg(feature = "cpc")))]
pub mod cpc;
pub mod error;
#[cfg(feature = "frequencies")]
#[cfg_attr(docsrs, doc(cfg(feature = "frequencies")))]
pub mod frequencies;
pub mod hash_value;
#[cfg(feature = "hll")]
#[cfg_attr(docsrs, doc(cfg(feature = "hll")))]
pub mod hll;
#[cfg(feature = "tdigest")]
#[cfg_attr(docsrs, doc(cfg(feature = "tdigest")))]
pub mod tdigest;
#[cfg(feature = "theta")]
#[cfg_attr(docsrs, doc(cfg(feature = "theta")))]
pub mod theta;

mod hash;
