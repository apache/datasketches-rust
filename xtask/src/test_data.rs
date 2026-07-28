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
use std::time::Duration;

use clap::Parser;
use tempfile::NamedTempFile;
use zip::ZipArchive;

const ARCHIVE_URL: &str = "https://github.com/apache/datasketches-tck/archive/0016a517/main.zip";

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Parser)]
#[clap(name = "test-data")]
pub(crate) struct CommandTestData {}

impl CommandTestData {
    pub(crate) fn run(self) {
        if let Err(error) = self.prepare() {
            eprintln!("failed to prepare serialization test data: {error}");
            std::process::exit(1);
        }
    }

    fn prepare(self) -> Result<()> {
        let repository_root = Path::new(env!("CARGO_WORKSPACE_DIR"));
        let serde_tests = repository_root.join("datasketches/tests/serde_tests");

        println!("Downloading serialization snapshots from {ARCHIVE_URL}");
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(60))
            .timeout_read(Duration::from_secs(60))
            .build();
        let response = agent.get(ARCHIVE_URL).call()?;
        let mut reader = response.into_reader();

        let archive_file = NamedTempFile::new()?;
        io::copy(&mut reader, &mut archive_file.as_file())?;

        let mut archive = ZipArchive::new(archive_file.reopen()?)?;
        for language in ["cpp", "java"] {
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
