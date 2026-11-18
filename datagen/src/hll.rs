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

use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;

use crate::N_STD;
use crate::write_snapshot;

const TYPES: [(HllType, &str); 3] = [
    (HllType::Hll4, "hll4"),
    (HllType::Hll6, "hll6"),
    (HllType::Hll8, "hll8"),
];

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for (hll_type, prefix) in TYPES {
        for n in N_STD {
            let mut sketch = HllSketch::new(12, hll_type).unwrap();
            for i in 0..n {
                sketch.update(i as i64);
            }
            write_snapshot(
                output_dir,
                &format!("{prefix}_n{n}_rust.sk"),
                &sketch.serialize(),
            )?;
        }
    }

    let mut union = HllUnion::new(12).unwrap();
    for range in [0..1_000_i64, 500..520, 2_000..7_000] {
        let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
        for i in range {
            sketch.update(i);
        }
        union.update(&sketch);
    }
    for (hll_type, prefix) in TYPES {
        write_snapshot(
            output_dir,
            &format!("{prefix}_union_rust.sk"),
            &union.serialize(hll_type),
        )?;
    }

    Ok(())
}
