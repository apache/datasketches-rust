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

use datasketches::ddsketch::DdSketch;
use datasketches::ddsketch::DdSketchBuilder;
use datasketches::ddsketch::MappingKind;
use datasketches::ddsketch::StoreKind;

use crate::N_STD;
use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in N_STD {
        let mut sketch = DdSketch::<u64>::new(0.01).unwrap();
        for i in 1..=n {
            sketch.update(i as f64);
        }
        write_snapshot(
            output_dir,
            &format!("ddsketch_cubic_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    for n in N_STD {
        let mut sketch = DdSketchBuilder::new(0.01)
            .mapping(MappingKind::Logarithmic)
            .store(StoreKind::CollapsingLowestDense { max_num_bins: 512 })
            .build::<u64>()
            .unwrap();
        for i in 0..n {
            let value = match i % 5 {
                0 => 0.0,
                1 | 2 => -((i + 1) as f64),
                _ => (i + 1) as f64,
            };
            sketch.update(value);
        }
        write_snapshot(
            output_dir,
            &format!("ddsketch_signed_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    Ok(())
}
