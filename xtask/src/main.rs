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
use std::path::Path;
use std::process::Command as StdCommand;
use std::time::Duration;

use clap::Parser;
use clap::Subcommand;
use flate2::read::GzDecoder;
use tar::Archive;

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
    #[arg(
        value_name = "LANG",
        value_parser = ["java", "cpp", "c++"],
        help = "Languages to prepare (all by default)."
    )]
    langs: Vec<String>,
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
            format!("https://api.github.com/repos/apache/datasketches-tck/tarball/{REVISION}");

        println!("Downloading serialization snapshots from {archive_url}");

        let timeout = Some(Duration::from_secs(60));
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_connect(timeout)
            .timeout_recv_response(timeout)
            .timeout_recv_body(timeout)
            .build()
            .into();
        let response = agent
            .get(&archive_url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2026-03-10")
            .call()?;
        let (_, body) = response.into_parts();
        let mut archive = Archive::new(GzDecoder::new(body.into_reader()));

        let mut targets = vec![];
        for language in self.languages() {
            let source_directory = Path::new("serialization").join(language).join("snapshots");
            let destination = serde_tests.join(format!("{language}_generated_files"));
            if fs::exists(&destination)? {
                println!(
                    "Removing existing {language} snapshots from {}",
                    destination.display()
                );
                fs::remove_dir_all(&destination)?;
            }
            fs::create_dir_all(&destination)?;
            targets.push((language, source_directory, destination, 0_usize));
        }

        for member in archive.entries()? {
            let mut member = member?;
            let path = member.path()?;
            if !member.header().entry_type().is_file()
                || path.extension().is_none_or(|extension| extension != "sk")
            {
                continue;
            }

            let Some((_, _, destination, count)) = targets.iter_mut().find(|target| {
                path.parent()
                    .is_some_and(|parent| parent.ends_with(&target.1))
            }) else {
                continue;
            };

            let name = path.file_name().expect("snapshot path has a file name");
            let mut output = fs::File::create(destination.join(name))?;
            io::copy(&mut member, &mut output)?;
            *count += 1;
        }

        for (language, _, destination, count) in targets {
            if count == 0 {
                return Err(format!("no {language} snapshots found in the TCK archive").into());
            }
            println!(
                "Extracted {count} {language} snapshots into {}",
                destination.display()
            );
        }
        Ok(())
    }

    fn languages(&self) -> Vec<&'static str> {
        if self.langs.is_empty() {
            return vec!["cpp", "java"];
        }

        let mut languages = vec![];
        for language in &self.langs {
            let language = match language.as_str() {
                "java" => "java",
                "cpp" | "c++" => "cpp",
                _ => unreachable!("language is validated by clap"),
            };
            if !languages.contains(&language) {
                languages.push(language);
            }
        }
        languages
    }
}
