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

use std::error::Error;
use std::fs;
use std::io;
use std::io::Cursor;
use std::path::Path;
use std::process::Command as StdCommand;
use std::time::Duration;

use clap::Parser;
use clap::Subcommand;
use zip::ZipArchive;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

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

#[derive(Parser)]
#[clap(name = "check")]
struct CommandCheck {}

impl CommandCheck {
    fn run(self) {
        let features = datasketches_features();

        run_command(make_check_cmd(&[]));
        for feature in features.chunks(1) {
            run_command(make_check_cmd(feature));
        }
        run_command(make_check_cmd(&features));
    }
}

#[derive(Parser)]
#[clap(name = "docs")]
struct CommandDocs {}

impl CommandDocs {
    fn run(self) {
        run_command(make_docs_cmd(true));
    }
}

#[derive(Parser)]
struct CommandTest {
    #[arg(long, help = "Run tests serially and do not capture output.")]
    no_capture: bool,
}

impl CommandTest {
    fn run(self) {
        let features = datasketches_features();
        run_command(make_test_cmd(self.no_capture, &features));
    }
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

#[derive(Parser)]
#[clap(name = "lint")]
struct CommandLint {
    #[arg(long, help = "Automatically apply lint suggestions.")]
    fix: bool,
}

impl CommandLint {
    fn run(self) {
        run_command(make_clippy_cmd(self.fix));
        run_command(make_format_cmd(self.fix));
        run_command(make_docs_cmd(false));
        run_command(make_taplo_cmd(self.fix));
        run_command(make_typos_cmd());
        run_command(make_hawkeye_cmd(self.fix));
    }
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

fn ensure_installed(bin: &str, crate_name: &str) {
    if which::which(bin).is_err() {
        let mut cmd = find_command("cargo");
        cmd.args(["install", crate_name]);
        run_command(cmd);
    }
}

fn run_command(mut cmd: StdCommand) {
    println!("{cmd:?}");
    let status = cmd.status().expect("failed to execute process");
    assert!(status.success(), "command failed: {status}");
}

fn make_test_cmd(no_capture: bool, features: &[String]) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["test", "--workspace", "--no-default-features"]);
    for feature in features {
        cmd.args(["--features", feature]);
    }
    if no_capture {
        cmd.args(["--", "--nocapture"]);
    }
    cmd
}

fn make_check_cmd(features: &[String]) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.env("RUSTFLAGS", "-Dwarnings");
    cmd.args([
        "+nightly",
        "check",
        "--package",
        "datasketches",
        "--all-targets",
        "--no-default-features",
    ]);
    for feature in features {
        cmd.args(["--features", feature]);
    }
    cmd
}

fn make_format_cmd(fix: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["+nightly", "fmt", "--all"]);
    if !fix {
        cmd.arg("--check");
    }
    cmd
}

fn make_clippy_cmd(fix: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args([
        "+nightly",
        "clippy",
        "--tests",
        "--all-features",
        "--all-targets",
        "--workspace",
    ]);
    if fix {
        cmd.args(["--allow-staged", "--allow-dirty", "--fix"]);
    } else {
        cmd.args(["--", "-D", "warnings"]);
    }
    cmd
}

fn make_docs_cmd(open: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.env("RUSTDOCFLAGS", "--cfg docsrs -D warnings");
    cmd.args([
        "+nightly",
        "doc",
        "--package",
        "datasketches",
        "--all-features",
        "--no-deps",
    ]);
    if open {
        cmd.args(["--open"]);
    }
    cmd
}

fn make_hawkeye_cmd(fix: bool) -> StdCommand {
    ensure_installed("hawkeye", "hawkeye");
    let mut cmd = find_command("hawkeye");
    if fix {
        cmd.args(["format", "--fail-if-updated=false"]);
    } else {
        cmd.args(["check"]);
    }
    cmd
}

fn make_typos_cmd() -> StdCommand {
    ensure_installed("typos", "typos-cli");
    find_command("typos")
}

fn make_taplo_cmd(fix: bool) -> StdCommand {
    ensure_installed("taplo", "taplo-cli");
    let mut cmd = find_command("taplo");
    if fix {
        cmd.args(["format"]);
    } else {
        cmd.args(["format", "--check"]);
    }
    cmd
}

#[derive(Parser)]
#[clap(name = "prepare-testdata")]
struct CommandPrepareTestData {
    #[arg(long, help = "Prepare Java test data.")]
    java: bool,
    #[arg(long, help = "Prepare C++ test data.")]
    cpp: bool,
    #[arg(long, help = "Prepare all test data.")]
    all: bool,
}

impl CommandPrepareTestData {
    fn run(self) {
        if let Err(error) = self.prepare() {
            eprintln!("failed to prepare serialization test data: {error}");
            std::process::exit(1);
        }
    }

    fn prepare(self) -> Result<()> {
        const REVISION: &str = "0016a517cc87e13339298550afe8e6a7e961bf46";
        let serde_tests =
            Path::new(env!("CARGO_WORKSPACE_DIR")).join("datasketches/tests/serde_tests");
        let archive_url =
            format!("https://github.com/apache/datasketches-tck/archive/{REVISION}/main.zip");

        println!("Downloading serialization snapshots from {archive_url}");

        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(60))
            .timeout_read(Duration::from_secs(60))
            .build();
        let response = agent.get(&archive_url).call()?;
        let mut reader = response.into_reader();

        let mut archive_bytes = Vec::new();
        io::copy(&mut reader, &mut archive_bytes)?;

        let mut archive = ZipArchive::new(Cursor::new(archive_bytes))?;
        let mut snapshots = Vec::new();
        for language in self.languages() {
            let source_directory = Path::new("serialization").join(language).join("snapshots");
            let mut members = Vec::new();

            for index in 0..archive.len() {
                let member = archive.by_index(index)?;
                let Some(path) = member.enclosed_name() else {
                    continue;
                };

                if member.is_dir()
                    || path.extension().is_none_or(|extension| extension != "sk")
                    || path
                        .parent()
                        .is_none_or(|parent| !parent.ends_with(&source_directory))
                {
                    continue;
                }

                let name = path
                    .file_name()
                    .expect("snapshot path has a file name")
                    .to_owned();
                members.push((index, name));
            }

            if members.is_empty() {
                return Err(format!("no {language} snapshots found in the TCK archive").into());
            }
            snapshots.push((language, members));
        }

        for (language, members) in snapshots {
            let destination = serde_tests.join(format!("{language}_generated_files"));
            if fs::exists(&destination)? {
                println!(
                    "Removing existing {language} snapshots from {}",
                    destination.display()
                );
                fs::remove_dir_all(&destination)?;
            }
            fs::create_dir_all(&destination)?;

            let count = members.len();
            for (index, name) in members {
                let mut source = archive.by_index(index)?;
                let mut output = fs::File::create(destination.join(name))?;
                io::copy(&mut source, &mut output)?;
            }

            println!(
                "Extracted {count} {language} snapshots into {}",
                destination.display()
            );
        }
        Ok(())
    }

    fn languages(&self) -> Vec<&'static str> {
        let mut languages = vec![];
        if self.all || self.cpp {
            languages.push("cpp");
        }
        if self.all || self.java {
            languages.push("java");
        }

        if languages.is_empty() {
            vec!["cpp", "java"]
        } else {
            languages
        }
    }
}
