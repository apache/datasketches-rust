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

use datasketches::density::DensitySketch;

use crate::write_snapshot;

const COUNTS: [u64; 6] = [0, 1, 10, 100, 1_000, 10_000];

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in COUNTS {
        let mut sketch = DensitySketch::<f32>::new(10, 3).unwrap();
        for i in 0..n {
            let x = i as f32;
            sketch.update(&[x, x.sqrt(), -x]).unwrap();
        }
        write_snapshot(
            output_dir,
            &format!("density_float_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    for n in COUNTS {
        let mut sketch = DensitySketch::<f64>::new(10, 3).unwrap();
        for i in 0..n {
            let x = i as f64;
            sketch.update(&[x, x.sqrt(), -x]).unwrap();
        }
        write_snapshot(
            output_dir,
            &format!("density_double_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }
    Ok(())
}
