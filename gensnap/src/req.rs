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

use datasketches::req::RankAccuracy;
use datasketches::req::ReqFloat;
use datasketches::req::ReqSketch;

use crate::N_STD;
use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in N_STD {
        let mut sketch = ReqSketch::<ReqFloat<f32>>::new(12, RankAccuracy::HighRank).unwrap();
        for i in 1..=n {
            sketch.update(ReqFloat::<f32>::new(i as f32).unwrap());
        }
        write_snapshot(
            output_dir,
            &format!("req_float_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    for m in [1_u64, 10] {
        let mut sketch = ReqSketch::<ReqFloat<f32>>::new(12, RankAccuracy::HighRank).unwrap();
        for i in 1..=m {
            sketch.update(ReqFloat::<f32>::new(-(i as f32)).unwrap());
        }
        write_snapshot(
            output_dir,
            &format!("req_float_negative_n{m}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    for m in [1_i32, 10] {
        let mut sketch = ReqSketch::<ReqFloat<f32>>::new(12, RankAccuracy::HighRank).unwrap();
        for i in -m..=m {
            sketch.update(ReqFloat::<f32>::new(i as f32).unwrap());
        }
        write_snapshot(
            output_dir,
            &format!("req_float_mixed_n{m}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    Ok(())
}
