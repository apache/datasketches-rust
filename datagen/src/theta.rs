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

use datasketches::theta::ThetaSketchBuilder;

use crate::N_STD;
use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in N_STD {
        let mut sketch = ThetaSketchBuilder::default().build().unwrap();
        for i in 0..n {
            sketch.update(i as i64);
        }
        write_snapshot(
            output_dir,
            &format!("theta_n{n}_rust.sk"),
            &sketch.compact(true).serialize(),
        )?;
    }

    for n in [10_u64, 100, 1_000, 10_000, 100_000, 1_000_000] {
        let mut sketch = ThetaSketchBuilder::default().build().unwrap();
        for i in 0..n {
            sketch.update(i as i64);
        }
        write_snapshot(
            output_dir,
            &format!("theta_compressed_n{n}_rust.sk"),
            &sketch.compact(true).serialize_compressed(),
        )?;
    }

    let mut non_empty_no_entries = ThetaSketchBuilder::default()
        .sampling_probability(0.01)
        .build()
        .unwrap();
    non_empty_no_entries.update(1_i64);
    write_snapshot(
        output_dir,
        "theta_non_empty_no_entries_rust.sk",
        &non_empty_no_entries.compact(true).serialize(),
    )?;

    Ok(())
}
