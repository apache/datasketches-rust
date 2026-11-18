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

mod bench;
mod check;
mod docs;
mod lint;
mod prepare_testdata;
mod test;

use std::path::Path;
use std::process::Command as StdCommand;

use clap::Parser;
use clap::Subcommand;

use crate::bench::CommandBench;
use crate::check::CommandCheck;
use crate::docs::CommandDocs;
use crate::lint::CommandLint;
use crate::prepare_testdata::CommandPrepareTestData;
use crate::test::CommandTest;

fn main() {
    let cmd = Command::parse();
    cmd.run()
}

#[derive(Parser)]
struct Command {
    #[clap(subcommand)]
    sub: SubCommand,
}

impl Command {
    fn run(self) {
        match self.sub {
            SubCommand::Bench(cmd) => cmd.run(),
            SubCommand::Check(cmd) => cmd.run(),
            SubCommand::Docs(cmd) => cmd.run(),
            SubCommand::Lint(cmd) => cmd.run(),
            SubCommand::Test(cmd) => cmd.run(),
            SubCommand::PrepareTestData(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCommand {
    #[clap(about = "Run workspace benchmarks.")]
    Bench(CommandBench),
    #[clap(about = "Check datasketches under the feature matrix.")]
    Check(CommandCheck),
    #[clap(about = "Generate documentation and open for preview")]
    Docs(CommandDocs),
    #[clap(about = "Run linter checks.")]
    Lint(CommandLint),
    #[clap(about = "Run the test suite.")]
    Test(CommandTest),
    #[clap(
        name = "prepare-testdata",
        about = "Prepare serialization compatibility test data."
    )]
    PrepareTestData(CommandPrepareTestData),
}

fn datasketches_features() -> Vec<String> {
    use cargo_metadata::Metadata;
    use cargo_metadata::MetadataCommand;

    let datasketches_manifest = Path::new(env!("CARGO_WORKSPACE_DIR")).join("Cargo.toml");

    let Metadata { packages, .. } = MetadataCommand::new()
        .manifest_path(datasketches_manifest)
        .exec()
        .expect("failed to get cargo metadata");

    let pkg = packages
        .into_iter()
        .find(|p| p.name == "datasketches")
        .expect("failed to find datasketches package");

    let mut features = pkg
        .features
        .into_keys()
        .filter(|feature| feature != "default")
        .collect::<Vec<_>>();
    features.sort();
    features
}

fn find_command(cmd: &str) -> StdCommand {
    match which::which(cmd) {
        Ok(exe) => {
            let mut cmd = StdCommand::new(exe);
            cmd.current_dir(env!("CARGO_WORKSPACE_DIR"));
            cmd
        }
        Err(err) => {
            panic!("{cmd} not found: {err}");
        }
    }
}

fn run_command(mut cmd: StdCommand) {
    println!("{cmd:?}");
    let status = cmd.status().expect("failed to execute process");
    assert!(status.success(), "command failed: {status}");
}
