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

//! Binary serialization format constants for Tuple sketches.
//!
//! The Tuple compact format reuses the uncompressed Theta layout (preamble, flags, theta) but uses
//! the Tuple family id, carries a sketch-type byte, and stores the user summary bytes after each
//! retained hash. See the C++/Java reference implementations for the on-disk format.

/// Current serial version written by this implementation.
pub(super) const SERIAL_VERSION: u8 = 3;
/// Legacy serial version still accepted on read.
pub(super) const SERIAL_VERSION_LEGACY: u8 = 1;

/// Current sketch-type byte written by this implementation.
pub(super) const SKETCH_TYPE: u8 = 1;
/// Legacy sketch-type byte still accepted on read.
pub(super) const SKETCH_TYPE_LEGACY: u8 = 5;

pub(super) const FLAGS_IS_READ_ONLY: u8 = 1 << 1;
pub(super) const FLAGS_IS_EMPTY: u8 = 1 << 2;
pub(super) const FLAGS_IS_COMPACT: u8 = 1 << 3;
pub(super) const FLAGS_IS_ORDERED: u8 = 1 << 4;
