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

use datasketches::uddsketch::UddSketch;

use crate::N_STD;
use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in N_STD {
        let mut sketch = UddSketch::new(256, 0.01).unwrap();
        for i in 0..n {
            let value = match i % 5 {
                0 => (i + 1) as f64,
                1 => -((i + 1) as f64),
                2 => 0.0,
                3 => 2f64.powi(((i % 64) as i32) - 32),
                _ => -2f64.powi(((i % 64) as i32) - 32),
            };
            sketch.update(value);
        }
        write_snapshot(
            output_dir,
            &format!("uddsketch_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }
    Ok(())
}
