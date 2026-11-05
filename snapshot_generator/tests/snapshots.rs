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
use std::process::Command;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

#[test]
fn generates_identical_snapshots_across_processes() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "datasketches-snapshots-{}-{nonce}",
        std::process::id()
    ));
    let first = root.join("first");
    let second = root.join("second");
    for output in [&first, &second] {
        let status = Command::new(env!("CARGO_BIN_EXE_snapshot_generator"))
            .arg("--output")
            .arg(output)
            .status()
            .unwrap();
        assert!(status.success(), "snapshot generation failed");
        assert_eq!(fs::read_dir(output).unwrap().count(), 4);
    }
    for name in [
        "bloom_empty_rust.sk",
        "bloom_non_empty_rust.sk",
        "count_min_empty_rust.sk",
        "count_min_non_empty_rust.sk",
    ] {
        let first_bytes = fs::read(first.join(name)).unwrap();
        assert!(!first_bytes.is_empty(), "empty snapshot: {name}");
        assert_eq!(
            first_bytes,
            fs::read(second.join(name)).unwrap(),
            "nondeterministic snapshot: {name}"
        );
    }
    fs::remove_dir_all(root).unwrap();
}
