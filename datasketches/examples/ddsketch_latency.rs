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

//! Aggregates request latency distributions across hosts before querying service percentiles.
//!
//! Each host sends a sketch of latency observations in milliseconds. Integer weights represent
//! repeated observations with the same latency. Merging preserves those counts; averaging the
//! hosts' individual percentiles would not produce a service percentile.
//! The default collapse direction favors high quantiles. A collapsed query is reported explicitly
//! because it no longer has the configured relative accuracy.

use datasketches::ddsketch::DdSketch;
use datasketches::error::Error;

fn main() -> Result<(), Error> {
    let hosts = [
        [(2.0, 900), (10.0, 90), (100.0, 10)],
        [(5.0, 500), (20.0, 450), (500.0, 50)],
        [(1.0, 950), (50.0, 40), (2000.0, 10)],
    ];
    let mut service = DdSketch::new(2048, 0.01)?;
    let mut host = service.clone();
    let mut message = Vec::new();
    for observations in hosts {
        host.reset();
        for (milliseconds, requests) in observations {
            host.update_weighted(milliseconds, requests)?;
        }
        host.serialize_into(&mut message);
        service.merge(&DdSketch::deserialize(&message)?)?;
    }
    println!("Requests: {}", service.total_weight());
    for rank in [0.5, 0.95, 0.99] {
        if let Some(value) = service.quantile(rank)? {
            let collapsed = service.quantile_is_collapsed(rank)?.unwrap();
            println!(
                "p{:.0}: {value:.3} ms (collapsed: {collapsed})",
                rank * 100.0
            );
        }
    }
    Ok(())
}
