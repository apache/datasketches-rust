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

use std::io;
use std::path::Path;

use datasketches::frequencies::FrequentItemsSketch;

use crate::N_STD;
use crate::write_snapshot;

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in N_STD {
        let mut sketch = FrequentItemsSketch::<i64>::new(64).unwrap();
        for i in 1..=n {
            sketch.update(i as i64);
        }
        write_snapshot(
            output_dir,
            &format!("frequent_long_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    for n in N_STD {
        let mut sketch = FrequentItemsSketch::<String>::new(64).unwrap();
        for i in 1..=n {
            sketch.update(i.to_string());
        }
        write_snapshot(
            output_dir,
            &format!("frequent_string_n{n}_rust.sk"),
            &sketch.serialize(),
        )?;
    }

    let mut long_purged = FrequentItemsSketch::<i64>::new(256).unwrap();
    for i in 1..=193_i64 {
        long_purged.update(i);
    }
    write_snapshot(
        output_dir,
        "frequent_long_purged_rust.sk",
        &long_purged.serialize(),
    )?;

    let mut string_purged = FrequentItemsSketch::<String>::new(256).unwrap();
    for i in 1..=193 {
        string_purged.update(i.to_string());
    }
    write_snapshot(
        output_dir,
        "frequent_string_purged_rust.sk",
        &string_purged.serialize(),
    )?;

    let mut ascii = FrequentItemsSketch::<String>::new(64).unwrap();
    for (ch, weight) in [('a', 1_u64), ('b', 2), ('c', 3), ('d', 4)] {
        ascii.update_with_weight(ch.to_string().repeat(29), weight);
    }
    write_snapshot(
        output_dir,
        "frequent_string_ascii_rust.sk",
        &ascii.serialize(),
    )?;

    let mut utf8 = FrequentItemsSketch::<String>::new(64).unwrap();
    for (item, weight) in [
        ("абвгд", 1_u64),
        ("еёжзи", 2),
        ("йклмн", 3),
        ("опрст", 4),
        ("уфхцч", 5),
        ("шщъыь", 6),
        ("эюя", 7),
    ] {
        utf8.update_with_weight(item.to_string(), weight);
    }
    write_snapshot(
        output_dir,
        "frequent_string_utf8_rust.sk",
        &utf8.serialize(),
    )?;

    Ok(())
}
