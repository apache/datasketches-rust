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

use std::fs;
use std::io;
use std::path::PathBuf;

use clap::Parser;
use datasketches::bloom::BloomFilterBuilder;
use datasketches::countmin::CountMinSketch;

#[derive(Parser)]
#[command(about = "Generate deterministic Rust serialization snapshots")]
struct Arguments {
    /// Directory in which snapshot files are written.
    #[arg(long, default_value = "serialization/rust/snapshots")]
    output: PathBuf,
}

fn main() -> io::Result<()> {
    let arguments = Arguments::parse();
    fs::create_dir_all(&arguments.output)?;

    write_snapshot(
        &arguments.output,
        "bloom_empty_rust.sk",
        &BloomFilterBuilder::with_accuracy(128, 0.01)
            .build()
            .serialize(),
    )?;

    let mut bloom = BloomFilterBuilder::with_accuracy(128, 0.01).build();
    for value in ["alpha", "beta", "gamma"] {
        bloom.insert(value);
    }
    write_snapshot(
        &arguments.output,
        "bloom_non_empty_rust.sk",
        &bloom.serialize(),
    )?;

    let empty_countmin = CountMinSketch::<i64>::with_seed(4, 32, 9001);
    write_snapshot(
        &arguments.output,
        "count_min_empty_rust.sk",
        &empty_countmin.serialize(),
    )?;

    let mut countmin = CountMinSketch::<i64>::with_seed(4, 32, 9001);
    for (value, weight) in [("alpha", 3), ("beta", 2), ("gamma", 5)] {
        for _ in 0..weight {
            countmin.update(value);
        }
    }
    write_snapshot(
        &arguments.output,
        "count_min_non_empty_rust.sk",
        &countmin.serialize(),
    )?;

    Ok(())
}

fn write_snapshot(output: &PathBuf, name: &str, bytes: &[u8]) -> io::Result<()> {
    let path = output.join(name);
    fs::write(&path, bytes)?;
    println!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}
