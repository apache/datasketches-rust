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

use datasketches::reservoir::ReservoirSketch;
use datasketches::reservoir::ReservoirUnion;
use datasketches::reservoir::ReservoirValue;

use crate::write_snapshot;

const EXACT_N: [usize; 5] = [1, 10, 32, 100, 128];
const SAMPLING_K: [usize; 3] = [32, 64, 128];

fn generate_typed<T: ReservoirValue>(
    output_dir: &Path,
    type_tag: &str,
    make_item: impl Fn(usize) -> T,
) -> io::Result<()> {
    let empty_sketch = ReservoirSketch::<T>::new(128).unwrap();
    write_snapshot(
        output_dir,
        &format!("reservoir_items_{type_tag}_empty_k128_rust.sk"),
        &empty_sketch.serialize(),
    )?;

    for n in EXACT_N {
        let mut sketch = ReservoirSketch::<T>::new(128).unwrap();
        for i in 0..n {
            sketch.update(make_item(i));
        }
        write_snapshot(
            output_dir,
            &format!("reservoir_items_{type_tag}_exact_n{n}_k128_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    for k in SAMPLING_K {
        let mut sketch = ReservoirSketch::<T>::new(k).unwrap();
        for i in 0..1_000 {
            sketch.update(make_item(i));
        }
        write_snapshot(
            output_dir,
            &format!("reservoir_items_{type_tag}_sampling_n1000_k{k}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    let empty_union = ReservoirUnion::<T>::new(128).unwrap();
    write_snapshot(
        output_dir,
        &format!("reservoir_items_union_{type_tag}_empty_maxk128_rust.sk"),
        &empty_union.serialize(),
    )?;

    for n in EXACT_N {
        let mut union = ReservoirUnion::<T>::new(128).unwrap();
        for i in 0..n {
            union.update_value(make_item(i));
        }
        write_snapshot(
            output_dir,
            &format!("reservoir_items_union_{type_tag}_exact_n{n}_maxk128_rust.sk"),
            &union.serialize(),
        )?;
    }

    for max_k in SAMPLING_K {
        let mut union = ReservoirUnion::<T>::new(max_k).unwrap();
        for i in 0..1_000 {
            union.update_value(make_item(i));
        }
        write_snapshot(
            output_dir,
            &format!("reservoir_items_union_{type_tag}_sampling_n1000_maxk{max_k}_rust.sk"),
            &union.serialize(),
        )?;
    }

    Ok(())
}

pub fn generate(output_dir: &Path) -> io::Result<()> {
    generate_typed(output_dir, "long", |i| i as i64)?;
    generate_typed(output_dir, "double", |i| i as f64)?;
    generate_typed(output_dir, "string", |i| format!("item{i}"))?;
    Ok(())
}
