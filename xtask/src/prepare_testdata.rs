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
use std::time::Duration;

use clap::Parser;
use flate2::read::GzDecoder;
use tar::Archive;

use crate::find_command;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Parser)]
#[clap(name = "prepare-testdata")]
pub struct CommandPrepareTestData {
    #[arg(
        value_name = "LANG",
        value_parser = ["java", "cpp", "c++", "go", "golang", "rust"],
        help = "Languages to prepare (all by default)."
    )]
    langs: Vec<String>,
}

impl CommandPrepareTestData {
    pub fn run(self) {
        if let Err(error) = self.prepare() {
            eprintln!("failed to prepare serialization test data: {error}");
            std::process::exit(1);
        }
    }

    fn prepare(self) -> Result<()> {
        let serde_tests =
            Path::new(env!("CARGO_WORKSPACE_DIR")).join("tests-integration/tests/serde_tests");
        let languages = self.languages();
        let remote_languages: Vec<&'static str> = languages
            .iter()
            .copied()
            .filter(|&language| language != "rust")
            .collect();

        if !remote_languages.is_empty() {
            prepare_remote(&serde_tests, &remote_languages)?;
        }
        if languages.contains(&"rust") {
            prepare_rust(&serde_tests)?;
        }
        Ok(())
    }

    fn languages(&self) -> Vec<&'static str> {
        if self.langs.is_empty() {
            return vec!["cpp", "go", "java", "rust"];
        }

        let mut languages = vec![];
        for language in &self.langs {
            let language = match language.as_str() {
                "java" => "java",
                "cpp" | "c++" => "cpp",
                "go" | "golang" => "go",
                "rust" => "rust",
                _ => unreachable!("language is validated by clap"),
            };
            if !languages.contains(&language) {
                languages.push(language);
            }
        }
        languages
    }
}

fn prepare_remote(serde_tests: &Path, languages: &[&'static str]) -> Result<()> {
    const REVISION: &str = "8193bab45ca5da3b513ea43fb5793e7e5b4da548";
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
    for &language in languages {
        let directory_name = format!("{language}_generated_files");
        let source_directory = Path::new("serialization_test_data").join(&directory_name);
        let destination = serde_tests.join(directory_name);
        reset_destination(language, &destination)?;
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

fn prepare_rust(serde_tests: &Path) -> Result<()> {
    let destination = serde_tests.join("rust_generated_files");
    reset_destination("rust", &destination)?;

    let mut cmd = find_command("cargo");
    cmd.args([
        "run",
        "--release",
        "--package",
        "datagen",
        "--",
        "--output-dir",
    ]);
    cmd.arg(&destination);
    println!("{cmd:?}");
    let status = cmd.status()?;
    if !status.success() {
        return Err(format!("datagen failed: {status}").into());
    }

    let mut count = 0_usize;
    for entry in fs::read_dir(&destination)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "sk")
        {
            count += 1;
        }
    }
    if count == 0 {
        return Err("no rust snapshots were generated".into());
    }
    println!(
        "Generated {count} rust snapshots into {}",
        destination.display()
    );
    Ok(())
}

fn reset_destination(language: &str, destination: &Path) -> io::Result<()> {
    if fs::exists(destination)? {
        println!(
            "Removing existing {language} snapshots from {}",
            destination.display()
        );
        fs::remove_dir_all(destination)?;
    }
    fs::create_dir_all(destination)
}
