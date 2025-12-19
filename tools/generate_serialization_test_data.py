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

import os
import subprocess
import sys
import shutil
import re
from pathlib import Path

def check_command_installed(command):
    """Checks if a command is available in the system path."""
    if shutil.which(command) is None:
        print(f"Error: '{command}' is not installed or not in PATH.")
        sys.exit(1)

def check_java_version():
    """Checks if Java 25 is installed."""
    try:
        # java -version prints to stderr
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        output = result.stderr
        match = re.search(r'version "(\d+)', output)
        if match:
            version = int(match.group(1))
            if version != 25:
                print(f"Error: Java 25 is required, but found Java {version}.")
                sys.exit(1)
            print(f"Found Java {version}.")
        else:
            print("Error: Could not parse Java version.")
            print(output)
            sys.exit(1)
    except Exception as e:
        print(f"Error checking Java version: {e}")
        sys.exit(1)

def run_command(command, cwd=None, shell=False):
    """Runs a shell command and prints output."""
    print(f"Running: {' '.join(command) if isinstance(command, list) else command}")
    try:
        subprocess.check_call(command, cwd=cwd, shell=shell)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        sys.exit(1)

def main():
    # 1. Check prerequisites
    check_command_installed("git")
    check_command_installed("mvn")
    check_command_installed("java")
    check_java_version()

    # 2. Define paths
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    temp_dir = project_root / "tmp_datasketches_java"
    output_dir = project_root / "tests" / "serialization_test_data" / "java_generated_files"

    # 3. Setup temporary directory
    if temp_dir.exists():
        print(f"Removing existing temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir)

    temp_dir.mkdir()

    # 4. Clone repository
    repo_url = "https://github.com/apache/datasketches-java.git"
    run_command(["git", "clone", repo_url, str(temp_dir)])

    # 5. Run Maven to generate files
    # The files are generated in serialization_test_data/java_generated_files relative to the java repo root
    # We rely on the profile 'generate-java-files'
    mvn_cmd = ["mvn", "test", "-P", "generate-java-files"]
    if os.name == 'nt': # Windows
        mvn_cmd = ["mvn.cmd", "test", "-P", "generate-java-files"]

    run_command(mvn_cmd, cwd=temp_dir)

    # 6. Copy generated files
    generated_files_dir = temp_dir / "serialization_test_data" / "java_generated_files"

    if not generated_files_dir.exists():
        print(f"Error: Expected generated files directory not found at {generated_files_dir}")
        sys.exit(1)

    print(f"Copying files from {generated_files_dir} to {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    files_copied = 0
    for file_path in generated_files_dir.glob("*.sk"):
        shutil.copy2(file_path, output_dir)
        print(f"Copied: {file_path.name}")
        files_copied += 1

    if files_copied == 0:
        print("Warning: No .sk files were found to copy.")
    else:
        print(f"Successfully copied {files_copied} files.")

    # 7. Cleanup (Optional: User might want to inspect if something failed, but we cleaned up at start)
    # Leaving it there for now as it's in .gitignore.
    # Uncomment next line to clean up after success
    # shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()
