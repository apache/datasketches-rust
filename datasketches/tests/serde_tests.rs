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

use datasketches::error::Error;
use datasketches::error::ErrorKind;

pub fn serialization_test_data(sub_dir: &str, name: &str) -> PathBuf {
    const SERDE_TESTS_DIR: &str = "tests/serde_tests";

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(SERDE_TESTS_DIR)
        .join(sub_dir)
        .join(name);

    if !path.exists() {
        panic!(
            r#"serialization test data file not found: {}

            Please ensure test data files are present in the repository. Generally, you can
            run the following commands from the project root to download the test data files
            if they are missing:

            $ ./tools/download_serde_tests_data.py
        "#,
            path.display(),
        );
    }

    path
}

pub fn assert_truncated_inputs_rejected<T>(
    bytes: &[u8],
    deserialize: impl Fn(&[u8]) -> Result<T, Error>,
) {
    assert!(!bytes.is_empty(), "valid serialization must not be empty");
    if let Err(err) = deserialize(bytes) {
        panic!("valid serialization was rejected before truncation checks: {err}");
    }

    for len in 0..bytes.len() {
        match deserialize(&bytes[..len]) {
            Ok(_) => panic!(
                "deserializer accepted a truncated input of {len}/{} bytes",
                bytes.len()
            ),
            Err(err) => assert_eq!(
                err.kind(),
                ErrorKind::InvalidData,
                "unexpected error for a truncated input of {len}/{} bytes",
                bytes.len()
            ),
        }
    }
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
