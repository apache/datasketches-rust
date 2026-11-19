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

use datasketches::cpc::CpcSketch;
use datasketches::hash::value::sign_extend;

use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in [0_u64, 100, 200, 2_000, 20_000] {
        let mut sketch = CpcSketch::new(11).unwrap();
        for i in 0..n {
            sketch.update(i as i64);
        }
        write_snapshot(
            output_dir,
            &format!("cpc_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    let mut sketch = CpcSketch::new(11).unwrap();
    sketch.update(sign_extend::from_i8(-1));
    sketch.update(sign_extend::from_i16(-1));
    sketch.update(sign_extend::from_i32(-1));
    sketch.update(-1_i64);
    write_snapshot(output_dir, "cpc_negative_one_rust.sk", &sketch.serialize())?;
    Ok(())
}
