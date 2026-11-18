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

use std::io;
use std::path::Path;

use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;

use crate::write_snapshot;

const TYPES: [(XorFilterType, &str); 3] = [
    (XorFilterType::Xor8, "xor8"),
    (XorFilterType::Xor16, "xor16"),
    (XorFilterType::Xor32, "xor32"),
];

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for (filter_type, prefix) in TYPES {
        for n in [0_u64, 1, 10, 100, 1_000, 10_000, 100_000] {
            let mut builder = XorFilterBuilder::new(filter_type).seed(9001);
            for i in 0..n {
                builder = builder.update(i as i64);
            }
            let filter = builder.build().unwrap();
            write_snapshot(
                output_dir,
                &format!("{prefix}_n{n}_rust.sk"),
                &filter.serialize(),
            )?;
        }
    }
    Ok(())
}
