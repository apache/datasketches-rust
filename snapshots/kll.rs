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

use datasketches::kll::KllFloat;
use datasketches::kll::KllSketch;

use crate::N_STD;
use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in N_STD {
        let mut sketch = KllSketch::<KllFloat<f32>>::new(200).unwrap();
        for i in 1..=n {
            sketch.update(KllFloat::<f32>::new(i as f32).unwrap());
        }
        write_snapshot(
            output_dir,
            &format!("kll_float_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;

        let mut sketch = KllSketch::<KllFloat<f64>>::new(200).unwrap();
        for i in 1..=n {
            sketch.update(KllFloat::<f64>::new(i as f64).unwrap());
        }
        write_snapshot(
            output_dir,
            &format!("kll_double_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;

        let mut sketch = KllSketch::<i64>::new(200).unwrap();
        for i in 1..=n {
            sketch.update(i as i64);
        }
        write_snapshot(
            output_dir,
            &format!("kll_long_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;

        let mut sketch = KllSketch::<String>::new(200).unwrap();
        let width = n.to_string().len();
        for i in 1..=n {
            sketch.update(format!("{i:>width$}"));
        }
        write_snapshot(
            output_dir,
            &format!("kll_string_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }
    Ok(())
}
