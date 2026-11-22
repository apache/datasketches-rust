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

//! Regional gateway nodes summarize request latencies spanning microsecond cache hits to
//! multi-second cold-storage fetches with `DdSketch`, ship serialized partial sketches to a
//! central aggregator, and query service-level tail quantiles within a 1% relative value bound.

use datasketches::ddsketch::DdSketch;
use datasketches::ddsketch::DdSketchBuilder;
use datasketches::ddsketch::StoreKind;

const RELATIVE_ACCURACY: f64 = 0.01;
const REGIONS: usize = 4;
const REQUESTS_PER_REGION: usize = 25_000;

/// Synthesizes bimodal request latencies in milliseconds: fast L1 cache hits (~0.05..0.5 ms),
/// regional RPCs (~5..50 ms), and occasional cold-tier tail spikes (~500..8,000 ms).
fn region_latency_ms(region: usize, seq: usize) -> f64 {
    let ticket = (region * 7_919 + seq * 104_729) % 10_000;
    let frac = (ticket as f64 + 0.5) / 10_000.0;
    if ticket < 8_500 {
        0.05 * 10.0_f64.powf(frac)
    } else if ticket < 9_900 {
        5.0 * 10.0_f64.powf(frac)
    } else {
        500.0 * 16.0_f64.powf(frac)
    }
}

fn published_sketch() -> DdSketch {
    // Collapsing lowest-magnitude buckets bounds per-instance memory while keeping high-quantile
    // tail estimates within `RELATIVE_ACCURACY` even if unexpected sub-nanosecond noise appears.
    DdSketchBuilder::new(RELATIVE_ACCURACY)
        .store(StoreKind::CollapsingLowestDense { max_num_bins: 1024 })
        .build()
        .unwrap()
}

fn main() {
    let mut exact = Vec::with_capacity(REGIONS * REQUESTS_PER_REGION);
    let mut payloads = Vec::with_capacity(REGIONS);

    for region in 0..REGIONS {
        let mut local = published_sketch();
        for seq in 0..REQUESTS_PER_REGION {
            let latency = region_latency_ms(region, seq);
            exact.push(latency);
            local.update(latency);
        }
        payloads.push(local.serialize());
    }

    let mut global = published_sketch();
    let mut wire_bytes = 0_usize;
    for payload in &payloads {
        wire_bytes += payload.len();
        let decoded = DdSketch::deserialize(payload).unwrap();
        global.merge(&decoded).unwrap();
    }

    exact.sort_by(f64::total_cmp);
    let ranks = [0.50, 0.90, 0.99, 0.999];
    let estimates = global.quantiles(&ranks).unwrap().unwrap();

    println!(
        "Requests: {} across {REGIONS} regions ({wire_bytes} serialized bytes shipped, {} buckets retained)",
        global.total_weight(),
        global.num_retained(),
    );
    for (&rank, &estimate) in ranks.iter().zip(&estimates) {
        let idx = ((rank * (exact.len() - 1) as f64).floor()) as usize;
        let truth = exact[idx];
        let rel_err = (estimate - truth).abs() / truth;
        println!(
            "  p{:<5} estimate = {:>9.3} ms   exact = {:>9.3} ms   rel_err = {:.4}%",
            rank * 100.0,
            estimate,
            truth,
            rel_err * 100.0,
        );
        assert!(rel_err <= RELATIVE_ACCURACY);
    }
}
