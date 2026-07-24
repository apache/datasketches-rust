#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath


def download_archive(destination):
    archive_url = "https://github.com/apache/datasketches-tck/archive/0016a517/main.zip"
    print(f"Downloading serialization snapshots from {archive_url}")
    request = urllib.request.Request(archive_url)
    with urllib.request.urlopen(request, timeout=60) as response:
        with destination.open("wb") as output:
            shutil.copyfileobj(response, output)


def extract_snapshots(archive, destination, language):
    source_parts = ("serialization", language, "snapshots")
    members = []
    for member in archive.infolist():
        path = PurePosixPath(member.filename)
        if (
            not member.is_dir()
            and path.suffix == ".sk"
            and path.parent.parts[-3:] == source_parts
        ):
            members.append((path.name, member))
    if not members:
        raise RuntimeError(f"no {language} snapshots found in the TCK archive")

    if destination.parent.is_symlink() or destination.is_symlink():
        raise RuntimeError(f"snapshot output path cannot be a symbolic link: {destination}")
    destination.mkdir(parents=True, exist_ok=True)

    expected_files = {name for name, _ in members}
    for existing_file in destination.glob("*.sk"):
        if existing_file.name not in expected_files:
            existing_file.unlink()

    for name, member in members:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=destination,
                prefix=f".{name}.",
                delete=False,
            ) as output:
                temp_path = Path(output.name)
                source = archive.open(member)
                with source:
                    shutil.copyfileobj(source, output)
            temp_path.replace(destination / name)
        finally:
            if temp_path is not None and temp_path.exists():
                temp_path.unlink()

    print(f"Extracted {len(members)} {language} snapshots into {destination}")


def main():
    parser = argparse.ArgumentParser(
        description="Download serialization test data from apache/datasketches-tck."
    )
    parser.add_argument("--java", action="store_true", help="Download Java test data")
    parser.add_argument("--cpp", action="store_true", help="Download C++ test data")
    parser.add_argument("--all", action="store_true", help="Download all test data")
    args = parser.parse_args()

    repository_root = Path(__file__).resolve().parents[1]
    serialization_test_data = repository_root / "datasketches" / "tests" / "serialization_test_data"
    generated_targets = {
        "cpp": serialization_test_data / "cpp_generated_files",
        "java": serialization_test_data / "java_generated_files",
    }

    languages = []
    if args.java or args.all:
        languages.append("java")
    if args.cpp or args.all:
        languages.append("cpp")
    if not languages:
        languages = list(generated_targets)

    with tempfile.TemporaryDirectory(prefix="datasketches-tck-") as temp_dir:
        archive_path = Path(temp_dir) / "datasketches-tck.zip"
        download_archive(archive_path)
        with zipfile.ZipFile(archive_path) as archive:
            for language in languages:
                extract_snapshots(archive, generated_targets[language], language)


if __name__ == "__main__":
    main()
