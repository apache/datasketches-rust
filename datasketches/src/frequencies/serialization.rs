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

//! Serialization constants and helpers for frequency sketches.

/// Family ID for frequency sketches.
pub const FAMILY_ID: u8 = 10;
/// Serialization version.
pub const SERIAL_VERSION: u8 = 1;

/// Preamble longs for empty sketch.
pub const PREAMBLE_LONGS_EMPTY: u8 = 1;
/// Preamble longs for non-empty sketch.
pub const PREAMBLE_LONGS_NONEMPTY: u8 = 4;

/// Empty flag mask (both bits for compatibility).
pub const EMPTY_FLAG_MASK: u8 = 5;
