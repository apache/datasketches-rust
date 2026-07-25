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

#[cfg(feature = "tuple")]
use datasketches::tuple::DefaultUpdatePolicy;
#[cfg(feature = "tuple")]
use datasketches::tuple::TupleSketch;
#[cfg(feature = "tuple")]
use datasketches::tuple::TupleSketchBuilder;

#[allow(dead_code)] // false-positive
pub fn test_data(name: &str) -> PathBuf {
    const TEST_DATA_DIR: &str = "tests/test_data";

    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(TEST_DATA_DIR)
        .join(name)
}

#[allow(dead_code)] // not every test target uses all helpers
pub fn serialization_test_data(sub_dir: &str, name: &str) -> PathBuf {
    const SERDE_TEST_DATA_DIR: &str = "tests/serialization_test_data";

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(SERDE_TEST_DATA_DIR)
        .join(sub_dir)
        .join(name);

    if !path.exists() {
        panic!(
            r#"serialization test data file not found: {}

            Please ensure test data files are present in the repository. Generally, you can
            run the following commands from the project root to download the test data files
            if they are missing:

            $ ./tools/download_serialization_test_data.py
        "#,
            path.display(),
        );
    }

    path
}

/// Returns a tuple sketch builder with the default additive `u64` summary policy.
#[cfg(feature = "tuple")]
#[allow(dead_code)] // not every test target uses all helpers
pub fn default_tuple_sketch_builder() -> TupleSketchBuilder<DefaultUpdatePolicy<u64>> {
    TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
}

/// Builds a tuple sketch updated with keys `start..start + count`, each with summary 1.
#[cfg(feature = "tuple")]
#[allow(dead_code)] // not every test target uses all helpers
pub fn tuple_sketch_with_range(start: u64, count: u64) -> TupleSketch<DefaultUpdatePolicy<u64>> {
    let mut sketch = default_tuple_sketch_builder().build();
    for i in 0..count {
        sketch.update(start + i, 1u64);
    }
    sketch
}
