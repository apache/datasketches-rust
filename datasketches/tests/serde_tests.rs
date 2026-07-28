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

use std::path::PathBuf;

pub fn serialization_test_data(sub_dir: &str, name: &str) -> PathBuf {
    const SERDE_TESTS_DIR: &str = "tests/serde_tests";

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(SERDE_TESTS_DIR)
        .join(sub_dir)
        .join(name);

    if !path.exists() {
        panic!(
            r#"serialization test data file not found: {}

            Run the following command from the project root to download the missing
            serialization test data:

            $ cargo x prepare-testdata --all
        "#,
            path.display(),
        );
    }

    path
}

#[cfg(feature = "bloom")]
#[path = "serde_tests/bloom.rs"]
mod bloom;

#[cfg(feature = "countmin")]
#[path = "serde_tests/countmin.rs"]
mod countmin;

#[cfg(feature = "cpc")]
#[path = "serde_tests/cpc.rs"]
mod cpc;

#[cfg(feature = "frequencies")]
#[path = "serde_tests/frequencies.rs"]
mod frequencies;

#[cfg(feature = "hll")]
#[path = "serde_tests/hll.rs"]
mod hll;

#[cfg(feature = "tdigest")]
#[path = "serde_tests/tdigest.rs"]
mod tdigest;

#[cfg(feature = "theta")]
#[path = "serde_tests/theta.rs"]
mod theta;

#[cfg(feature = "tuple")]
#[path = "serde_tests/tuple.rs"]
mod tuple;
