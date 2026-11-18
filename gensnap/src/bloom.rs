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

use datasketches::bloom::BloomFilterBuilder;
use datasketches::hash::value::canonical_float;

use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in [0_u64, 10_000, 2_000_000, 30_000_000] {
        for h in [3_u16, 5] {
            let mut filter = BloomFilterBuilder::with_size(n.max(1_000), h)
                .build()
                .unwrap();
            for i in 0..(n / 10) {
                filter.insert(i as i64);
            }
            if n > 0 {
                filter.insert(canonical_float::from_f64(f64::NAN));
            }
            write_snapshot(
                output_dir,
                &format!("bf_n{n}_h{h}_rust.sk"),
                &filter.serialize(),
            )?;
        }
    }
    Ok(())
}
