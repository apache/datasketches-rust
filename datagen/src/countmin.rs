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

use datasketches::countmin::CountMinSketch;
use datasketches::hash::value::raw_bytes;

use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in [0_u64, 1, 10, 100, 1_000, 10_000] {
        let mut sketch = CountMinSketch::<i64>::new(3, 1_000).unwrap();
        populate(&mut sketch, n);
        write_snapshot(
            output_dir,
            &format!("count_min_v2_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    let mut seeded = CountMinSketch::<i64>::with_seed(3, 1_000, 42).unwrap();
    populate(&mut seeded, 1_000);
    write_snapshot(
        output_dir,
        "count_min_v2_seed42_n1000_rust.sk",
        &seeded.serialize(),
    )?;
    Ok(())
}

fn populate(sketch: &mut CountMinSketch<i64>, n: u64) {
    for i in 0..n {
        sketch.update_with_weight(i as i64, (i + 1) as i64);
        sketch.update_with_weight(raw_bytes::from_string(i.to_string()), 1_i64);
    }
}
