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

use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::path::Path;
use std::process::Command as StdCommand;
use std::time::Duration;

use clap::Parser;
use clap::Subcommand;
use tempfile::NamedTempFile;
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
        const PINNED_COMMIT: &str = "0016a517";

        let serde_tests =
            Path::new(env!("CARGO_WORKSPACE_DIR")).join("datasketches/tests/serde_tests");
        let archive_url =
            format!("https://github.com/apache/datasketches-tck/archive/{PINNED_COMMIT}/main.zip");

        let all = self.all || !(self.java || self.cpp);
        let languages = [("java", all || self.java), ("cpp", all || self.cpp)];

        println!("Downloading serialization snapshots from {archive_url}");

        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(60))
            .timeout_read(Duration::from_secs(60))
            .build();
        let response = agent.get(&archive_url).call()?;
        let mut reader = response.into_reader();

        let archive_file = NamedTempFile::new()?;
        io::copy(&mut reader, &mut archive_file.as_file())?;

        let mut archive = ZipArchive::new(archive_file.reopen()?)?;
        for (language, selected) in languages {
            if !selected {
                continue;
            }
            let destination = serde_tests.join(format!("{language}_generated_files"));
            extract_snapshots(&mut archive, &destination, language)?;
        }

        Ok(())
    }
}

fn extract_snapshots<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    destination: &Path,
    language: &str,
) -> Result<()> {
    let source_directory = Path::new("serialization").join(language).join("snapshots");
    let mut members = Vec::new();
    let mut expected_files = BTreeSet::new();

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
        expected_files.insert(name.clone());
        members.push((index, name));
    }

    if members.is_empty() {
        return Err(format!("no {language} snapshots found in the TCK archive").into());
    }

    ensure_not_symlink(destination)?;
    if let Some(parent) = destination.parent() {
        ensure_not_symlink(parent)?;
    }
    fs::create_dir_all(destination)?;

    for entry in fs::read_dir(destination)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "sk")
            && path
                .file_name()
                .is_some_and(|name| !expected_files.contains(name))
        {
            fs::remove_file(path)?;
        }
    }

    for (index, name) in members {
        let mut source = archive.by_index(index)?;
        let mut output = NamedTempFile::new_in(destination)?;
        io::copy(&mut source, &mut output)?;
        output.persist(destination.join(&name))?;
    }

    println!(
        "Extracted {} {language} snapshots into {}",
        expected_files.len(),
        destination.display()
    );
    Ok(())
}

fn ensure_not_symlink(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(format!(
            "snapshot output path cannot be a symbolic link: {}",
            path.display()
        )
        .into());
    }
    Ok(())
}
