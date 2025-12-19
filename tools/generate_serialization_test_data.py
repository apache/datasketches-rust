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
import argparse
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

def generate_java_files(project_root):
    print("--- Generating Java Test Data ---")
    
    # 1. Check prerequisites
    check_command_installed("git")
    
    mvn_cmd_name = "mvn"
    if os.name == 'nt':
        mvn_cmd_name = "mvn.cmd"
    check_command_installed(mvn_cmd_name)
    
    check_command_installed("java")
    check_java_version()

    # 2. Define paths
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
    mvn_cmd = [mvn_cmd_name, "test", "-P", "generate-java-files"]
    
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
    
    # Cleanup done by .gitignore, but could be done here.

def generate_cpp_files(project_root):
    print("--- Generating C++ Test Data ---")
    
    # 1. Check prerequisites
    check_command_installed("git")
    check_command_installed("cmake")

    # 2. Define paths
    temp_dir = project_root / "tmp_datasketches_cpp"
    output_dir = project_root / "tests" / "serialization_test_data" / "cpp_generated_files"

    # 3. Setup temporary directory
    if temp_dir.exists():
        print(f"Removing existing temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir)
    
    temp_dir.mkdir()

    # 4. Clone repository
    repo_url = "https://github.com/apache/datasketches-cpp.git"
    run_command(["git", "clone", repo_url, str(temp_dir)])

    # 5. Build and Run CMake
    build_dir = temp_dir / "build"
    build_dir.mkdir(exist_ok=True)
    
    # cmake .. -DGENERATE=true
    run_command(["cmake", "..", "-DGENERATE=true"], cwd=build_dir)
    
    # cmake --build build --config Release
    # Note: cwd is temp_dir because build dir is 'build' relative to it, but the command is cmake --build build
    # Actually standard cmake usage: cmake --build <build_dir>
    run_command(["cmake", "--build", ".", "--config", "Release"], cwd=build_dir)
    
    # cmake --build build --config Release --target test
    # This runs the tests which we expect to generate the files because of -DGENERATE=true
    run_command(["cmake", "--build", ".", "--config", "Release", "--target", "test"], cwd=build_dir)

    # 6. Copy generated files
    # The instructions say: cp datasketches-cpp/build/*/test/*_cpp.sk serialization_test_data/cpp_generated_files
    # We need to find where they are exactly.
    # It seems they might be in build/test/ or subdirectories depending on generator.
    
    print(f"Copying files to {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    files_copied = 0
    # Search recursively in build directory for *_cpp.sk
    for file_path in build_dir.rglob("*_cpp.sk"):
         # Avoid copying from CMakeFiles or other intermediate dirs if possible, but the pattern is specific enough
        shutil.copy2(file_path, output_dir)
        print(f"Copied: {file_path.name}")
        files_copied += 1

    if files_copied == 0:
        print("Warning: No *_cpp.sk files were found to copy.")
    else:
        print(f"Successfully copied {files_copied} files.")

def main():
    parser = argparse.ArgumentParser(description="Generate serialization test data for Java and/or C++.")
    parser.add_argument("--java", action="store_true", help="Generate Java test data")
    parser.add_argument("--cpp", action="store_true", help="Generate C++ test data")
    parser.add_argument("--all", action="store_true", help="Generate both Java and C++ test data")

    args = parser.parse_args()

    # Default to all if no arguments provided
    if not args.java and not args.cpp and not args.all:
        args.all = True

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    if args.java or args.all:
        generate_java_files(project_root)
    
    if args.cpp or args.all:
        generate_cpp_files(project_root)

if __name__ == "__main__":
    main()