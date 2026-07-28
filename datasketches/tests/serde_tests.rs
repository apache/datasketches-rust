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

//! Serialization compatibility tests grouped by sketch.

// This target is also compiled without sketch features during feature-matrix checks.
#[allow(dead_code)]
#[path = "serialization_tests/support.rs"]
mod support;

#[cfg(feature = "bloom")]
#[path = "serialization_tests/bloom.rs"]
mod bloom;

#[cfg(feature = "countmin")]
#[path = "serialization_tests/countmin.rs"]
mod countmin;

#[cfg(feature = "cpc")]
#[path = "serialization_tests/cpc.rs"]
mod cpc;

#[cfg(feature = "frequencies")]
#[path = "serialization_tests/frequencies.rs"]
mod frequencies;

#[cfg(feature = "hll")]
#[path = "serialization_tests/hll.rs"]
mod hll;

#[cfg(feature = "tdigest")]
#[path = "serialization_tests/tdigest.rs"]
mod tdigest;

#[cfg(feature = "theta")]
#[path = "serialization_tests/theta.rs"]
mod theta;

#[cfg(feature = "tuple")]
#[path = "serialization_tests/tuple.rs"]
mod tuple;
