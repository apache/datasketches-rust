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
use std::path::Path;
use std::path::PathBuf;

use clap::Parser;
use clap::ValueEnum;

mod bloom;
mod countmin;
mod cpc;
mod ddsketch;
mod density;
mod frequencies;
mod hll;
mod kll;
mod req;
mod tdigest;
mod theta;
mod tuple;
mod uddsketch;
mod xor;

const N_STD: [u64; 8] = [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

#[derive(Parser)]
#[command(about = "Generate Rust DataSketches serialization compatibility snapshots.")]
struct Args {
    #[arg(
        long,
        value_name = "DIR",
        default_value = "serialization_test_data/rust_generated_files",
        help = "Directory to write generated .sk snapshot files into."
    )]
    output_dir: PathBuf,

    #[arg(
        long = "sketch",
        value_name = "FAMILY",
        help = "Sketch family to generate (repeatable; all families when omitted)."
    )]
    sketches: Vec<SketchFamily>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum SketchFamily {
    Bloom,
    #[value(name = "countmin")]
    CountMin,
    Cpc,
    #[value(name = "ddsketch")]
    DdSketch,
    Density,
    Frequencies,
    Hll,
    Kll,
    Req,
    #[value(name = "tdigest")]
    TDigest,
    Theta,
    Tuple,
    #[value(name = "uddsketch")]
    UddSketch,
    Xor,
}

impl SketchFamily {
    fn generate(self, output_dir: &Path) -> io::Result<()> {
        match self {
            Self::Bloom => bloom::generate(output_dir),
            Self::CountMin => countmin::generate(output_dir),
            Self::Cpc => cpc::generate(output_dir),
            Self::DdSketch => ddsketch::generate(output_dir),
            Self::Density => density::generate(output_dir),
            Self::Frequencies => frequencies::generate(output_dir),
            Self::Hll => hll::generate(output_dir),
            Self::Kll => kll::generate(output_dir),
            Self::Req => req::generate(output_dir),
            Self::TDigest => tdigest::generate(output_dir),
            Self::Theta => theta::generate(output_dir),
            Self::Tuple => tuple::generate(output_dir),
            Self::UddSketch => uddsketch::generate(output_dir),
            Self::Xor => xor::generate(output_dir),
        }
    }
}

fn write_snapshot(output_dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    fs::write(output_dir.join(name), bytes)
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(args) {
        eprintln!("failed to generate serialization snapshots: {error}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> io::Result<()> {
    fs::create_dir_all(&args.output_dir)?;
    for &family in SketchFamily::value_variants() {
        if args.sketches.is_empty() || args.sketches.contains(&family) {
            family.generate(&args.output_dir)?;
        }
    }
    Ok(())
}
