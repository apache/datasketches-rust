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

//! Reproducible, single-threaded quantile accuracy and cost experiment.
//!
//! Runs with `cargo run --release -p benchmarks --example quantile_comparison`.
//! `QUANTILE_ROWS` overrides the default 100,000 rows. Output is tagged CSV on stdout.
//! Timing is the median/min/max of seven samples after one warmup, with inputs prepared outside
//! the timer. Build includes finalization; merge includes decoding serialized partials and a
//! final query. Groups match 512 groups of 8/64 rows; shard counts are 1/8/64/256.
//! This measures sketch work, not database scheduling, networking, or storage I/O.

use std::hint::black_box;
use std::time::Instant;

use datasketches::tdigest::TDigestMut;
use datasketches::uddsketch::UddSketch;
use datasketches_v05::tdigest::TDigestMut as OldDigest;

#[derive(Clone, Copy, Debug)]
enum Config {
    Udd(u32, f64),
    Current(u16),
    Old(u16),
}

impl Config {
    fn name(self) -> String {
        match self {
            Self::Udd(m, a) => format!("udd_m{m}_a{a}"),
            Self::Current(k) => format!("td06_k{k}"),
            Self::Old(k) => format!("td05_k{k}"),
        }
    }

    fn empty(self) -> Sketch {
        match self {
            Self::Udd(m, a) => Sketch::Udd(UddSketch::new(m, a).unwrap()),
            Self::Current(k) => Sketch::Current(TDigestMut::new(k).unwrap()),
            Self::Old(k) => Sketch::Old(OldDigest::new(k).unwrap()),
        }
    }

    fn decode(self, bytes: &[u8]) -> Sketch {
        match self {
            Self::Udd(..) => Sketch::Udd(UddSketch::deserialize(bytes).unwrap()),
            Self::Current(..) => Sketch::Current(TDigestMut::deserialize(bytes).unwrap()),
            Self::Old(..) => Sketch::Old(OldDigest::deserialize(bytes).unwrap()),
        }
    }

    fn build(self, values: &[f64]) -> Sketch {
        let mut sketch = self.empty();
        for &x in values {
            sketch.update(x);
        }
        black_box(sketch.query(0.5));
        sketch
    }
}

#[derive(Clone)]
enum Sketch {
    Udd(UddSketch),
    Current(TDigestMut),
    Old(OldDigest),
}

impl Sketch {
    fn update(&mut self, value: f64) {
        match self {
            Self::Udd(s) => s.update(value).unwrap(),
            Self::Current(s) => s.update(value),
            Self::Old(s) => s.update(value),
        }
    }

    fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Self::Udd(s), Self::Udd(t)) => s.merge(t).unwrap(),
            (Self::Current(s), Self::Current(t)) => s.merge(t),
            (Self::Old(s), Self::Old(t)) => s.merge(t),
            _ => unreachable!(),
        }
    }

    fn query(&mut self, q: f64) -> f64 {
        match self {
            Self::Udd(s) => s.quantile(q).unwrap().unwrap(),
            Self::Current(s) => s.quantile(q).unwrap().unwrap(),
            Self::Old(s) => s.quantile(q).unwrap(),
        }
    }

    fn bytes(&mut self) -> Vec<u8> {
        match self {
            Self::Udd(s) => s.serialize(),
            Self::Current(s) => s.serialize(),
            Self::Old(s) => s.serialize(),
        }
    }

    fn memory(&self) -> usize {
        match self {
            Self::Udd(s) => s.estimated_size(),
            // The current API estimates the heap; include the inline state as in the other two.
            Self::Current(s) => s.estimated_size() + size_of::<TDigestMut>(),
            Self::Old(s) => s.estimated_size(),
        }
    }

    fn bound(&self) -> f64 {
        match self {
            Self::Udd(s) => s.relative_error(),
            _ => f64::NAN,
        }
    }

    fn level(&self) -> u32 {
        match self {
            Self::Udd(s) => s.compaction_level(),
            _ => 0,
        }
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn uniform(seed: &mut u64) -> f64 {
    ((next(seed) >> 11) as f64 + 0.5) / ((1u64 << 53) as f64)
}

fn shuffle<T>(values: &mut [T], seed: &mut u64) {
    for i in (1..values.len()).rev() {
        values.swap(i, next(seed) as usize % (i + 1));
    }
}

fn data(name: &str, n: usize) -> Vec<f64> {
    let mut seed = 42;
    (0..n)
        .map(|i| {
            let u = uniform(&mut seed);
            match name {
                "uniform" => u * 1000.0,
                "lognormal" => (2.0
                    * (-2.0 * u.ln()).sqrt()
                    * (std::f64::consts::TAU * uniform(&mut seed)).cos())
                .exp(),
                "pareto" => (1.0 - u).powf(-1.0 / 1.2),
                "outliers" => {
                    if i % 1000 == 0 {
                        1e12
                    } else {
                        1.0 + u
                    }
                }
                "repeated" => (next(&mut seed) % 8) as f64,
                "signed_zero" => {
                    if i % 4 == 0 {
                        0.0
                    } else {
                        (u - 0.5) * 2000.0
                    }
                }
                "wide" => 10.0f64.powf(u * 600.0 - 300.0),
                _ => unreachable!(),
            }
        })
        .collect()
}

fn measure(mut run: impl FnMut()) -> (f64, f64, f64) {
    run();
    let mut ns = Vec::new();
    for _ in 0..7 {
        let start = Instant::now();
        run();
        ns.push(start.elapsed().as_secs_f64() * 1e9);
    }
    ns.sort_by(f64::total_cmp);
    (ns[3], ns[0], ns[6])
}

fn timing(work: &str, cfg: Config, rows: usize, units: usize, result: (f64, f64, f64)) {
    println!(
        "time,{work},{},{rows},{units},{:.3},{:.3},{:.3}",
        cfg.name(),
        result.0 / units as f64,
        result.1 / units as f64,
        result.2 / units as f64
    );
}

const RANKS: [f64; 6] = [0.0, 0.5, 0.9, 0.99, 0.999, 1.0];

// A rounded floating product can cross an integer even for small sample sizes (e.g. 0.99
// and 100,001 rows). Evaluate the binary rational rank exactly for the discrete reference.
fn lower_discrete_index(rank: f64, count: usize) -> usize {
    let bits = rank.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as u32;
    if exponent == 0 {
        return 0;
    }
    let significand = (bits & ((1 << 52) - 1)) | (1 << 52);
    let product = u128::from(significand) * (count - 1) as u128;
    let shift = 1075 - exponent;
    if shift >= 128 {
        0
    } else {
        (product >> shift) as usize
    }
}

fn metrics(name: &str, cfg: Config, values: &[f64]) {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mut sketch = cfg.build(values);
    let memory = sketch.memory();
    let bytes = sketch.bytes().len();
    println!(
        "state,{name},{},{},{memory},{bytes},{},{:.10e}",
        cfg.name(),
        values.len(),
        sketch.level(),
        sketch.bound()
    );
    for q in RANKS {
        let discrete = sorted[lower_discrete_index(q, values.len())];
        let position = q * (values.len() - 1) as f64;
        let lower = sorted[position.floor() as usize];
        let upper = sorted[position.ceil() as usize];
        let fraction = position.fract();
        let continuous = lower * (1.0 - fraction) + upper * fraction;
        let estimate = sketch.query(q);
        let relative = if discrete == 0.0 {
            if estimate == 0.0 { 0.0 } else { f64::NAN }
        } else {
            ((estimate - discrete) / discrete).abs()
        };
        let continuous_relative = if continuous == 0.0 {
            if estimate == 0.0 { 0.0 } else { f64::NAN }
        } else {
            ((estimate - continuous) / continuous).abs()
        };
        // Ties occupy a rank interval. Distance to that interval avoids treating an exact repeated
        // value as erroneous solely because F(x) jumps at it.
        let below = sorted.partition_point(|x| *x < estimate) as f64 / values.len() as f64;
        let at_or_below = sorted.partition_point(|x| *x <= estimate) as f64 / values.len() as f64;
        let rank_error = (below - q).max(q - at_or_below).max(0.0);
        println!(
            "accuracy,{name},{},{q},{estimate:.10e},{discrete:.10e},{continuous:.10e},{relative:.10e},{continuous_relative:.10e},{rank_error:.10e}",
            cfg.name()
        );
    }
}

fn partials(cfg: Config, values: &[f64], shards: usize, layout: usize) -> Vec<Vec<u8>> {
    let mut states = (0..shards).map(|_| cfg.empty()).collect::<Vec<_>>();
    for (i, &x) in values.iter().enumerate() {
        let shard = if layout == 0 {
            i * shards / values.len()
        } else {
            i % shards
        };
        states[shard].update(x);
    }
    states.iter_mut().map(Sketch::bytes).collect()
}

fn reduce(cfg: Config, partials: &[Vec<u8>], tree: usize, seed: &mut u64) -> Sketch {
    let mut states = partials
        .iter()
        .map(|bytes| cfg.decode(bytes))
        .collect::<Vec<_>>();
    if tree == 1 {
        states.reverse();
    }
    if tree >= 2 {
        shuffle(&mut states, seed);
    }
    if tree == 3 {
        while states.len() > 1 {
            let mut next_level = Vec::new();
            for pair in states.chunks(2) {
                let mut a = pair[0].clone();
                if pair.len() == 2 {
                    a.merge(&pair[1]);
                }
                next_level.push(cfg.decode(&a.bytes()));
            }
            states = next_level;
        }
        states.pop().unwrap()
    } else {
        let mut result = cfg.empty();
        for s in states {
            result.merge(&s);
        }
        result
    }
}

fn stability(name: &str, cfg: Config, values: &[f64]) {
    let mut base = cfg.build(values);
    let reference = RANKS.map(|q| base.query(q));
    let base_bytes = base.bytes();
    let mut min = reference;
    let mut max = reference;
    let mut different = [0; RANKS.len()];
    let mut states_different = 0;
    let mut cases = 0;
    let mut seed = 2026;
    let mut shuffled = values.to_vec();
    shuffle(&mut shuffled, &mut seed);
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let reversed = sorted.iter().rev().copied().collect::<Vec<_>>();
    let orders = [values, &shuffled, &sorted, &reversed];
    for order in orders {
        for shards in [1, 8, 64, 256] {
            for layout in 0..2 {
                let parts = partials(cfg, order, shards, layout);
                for tree in 0..4 {
                    let mut result = reduce(cfg, &parts, tree, &mut seed);
                    let answers = RANKS.map(|q| result.query(q));
                    states_different += usize::from(result.bytes() != base_bytes);
                    for i in 0..RANKS.len() {
                        different[i] += usize::from(answers[i].to_bits() != reference[i].to_bits());
                        min[i] = min[i].min(answers[i]);
                        max[i] = max[i].max(answers[i]);
                    }
                    cases += 1;
                }
            }
        }
    }
    for i in 0..RANKS.len() {
        println!(
            "stability,{name},{},{},{cases},{},{states_different},{:.10e},{:.10e}",
            cfg.name(),
            RANKS[i],
            different[i],
            min[i],
            max[i]
        );
    }
    // Isolate merge order/tree variation with exactly the same partial bytes.
    let parts = partials(cfg, values, 64, 0);
    let mut first = reduce(cfg, &parts, 0, &mut seed);
    let expected = RANKS.map(|q| first.query(q));
    let mut changed = 0;
    for tree in 1..4 {
        let mut result = reduce(cfg, &parts, tree, &mut seed);
        changed += usize::from(RANKS.map(|q| result.query(q)) != expected);
    }
    println!("fixed_partials,{name},{},3,{changed}", cfg.name());
}

fn costs(cfg: Config, values: &[f64]) {
    timing(
        "insert",
        cfg,
        values.len(),
        values.len(),
        measure(|| {
            let mut sketch = cfg.empty();
            for &value in black_box(values) {
                sketch.update(value);
            }
            black_box(sketch);
        }),
    );

    timing(
        "build_finalize",
        cfg,
        values.len(),
        values.len(),
        measure(|| {
            black_box(cfg.build(black_box(values)));
        }),
    );
    let mut sketch = cfg.build(values);
    timing(
        "query_p99",
        cfg,
        values.len(),
        10_000,
        measure(|| {
            for _ in 0..10_000 {
                black_box(sketch.query(black_box(0.99)));
            }
        }),
    );
    timing(
        "serialize",
        cfg,
        values.len(),
        100,
        measure(|| {
            for _ in 0..100 {
                black_box(sketch.bytes());
            }
        }),
    );
    let bytes = sketch.bytes();
    timing(
        "deserialize",
        cfg,
        values.len(),
        100,
        measure(|| {
            for _ in 0..100 {
                black_box(cfg.decode(black_box(&bytes)));
            }
        }),
    );
    for shards in [1, 8, 64, 256] {
        let parts = partials(cfg, values, shards, 0);
        timing(
            &format!("merge_serialized_{shards}"),
            cfg,
            values.len(),
            1,
            measure(|| {
                let mut result = reduce(cfg, black_box(&parts), 0, &mut 42);
                black_box(result.query(0.99));
            }),
        );
        let decoded = parts
            .iter()
            .map(|bytes| cfg.decode(bytes))
            .collect::<Vec<_>>();
        timing(
            &format!("merge_memory_{shards}"),
            cfg,
            values.len(),
            1,
            measure(|| {
                let mut result = cfg.empty();
                for part in black_box(&decoded) {
                    result.merge(part);
                }
                black_box(result.query(0.99));
            }),
        );
        println!(
            "wire,{},{},{},{}",
            cfg.name(),
            values.len(),
            shards,
            parts.iter().map(Vec::len).sum::<usize>()
        );
    }
    for rows in [8, 64] {
        let inputs = (0..512)
            .map(|group| {
                (0..rows)
                    .map(|row| row as f64 + group as f64 / 512.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        timing(
            "groups_512",
            cfg,
            rows,
            512 * rows,
            measure(|| {
                for input in &inputs {
                    black_box(cfg.build(black_box(input)).bytes());
                }
            }),
        );
        timing(
            "partial_final_64",
            cfg,
            rows,
            64 * rows,
            measure(|| {
                let mut merged = cfg.empty();
                for group in 0..64 {
                    let mut partial = cfg.empty();
                    for row in 0..rows {
                        partial.update(row as f64 + group as f64 / 64.0);
                    }
                    merged.merge(&cfg.decode(&partial.bytes()));
                }
                black_box(merged.query(0.9));
            }),
        );
        let mut states = inputs.iter().map(|v| cfg.build(v)).collect::<Vec<_>>();
        let memory = states.iter().map(Sketch::memory).sum::<usize>();
        let bytes = states.iter_mut().map(|s| s.bytes().len()).sum::<usize>();
        println!("groups_state,{},{rows},512,{memory},{bytes}", cfg.name());
    }
}

fn main() {
    let n = std::env::var("QUANTILE_ROWS")
        .ok()
        .map(|x| x.parse().unwrap())
        .unwrap_or(100_000);
    assert!(n >= 1024);
    println!(
        "# seed=42; rows={n}; timing=median/min/max of 7 after warmup; nan means undefined/not provided"
    );
    println!("# time,work,config,rows,units,median_ns_per_unit,min_ns_per_unit,max_ns_per_unit");
    println!(
        "# state,dataset,config,rows,estimated_memory_bytes,serialized_bytes,level,relative_error_bound"
    );
    println!(
        "# accuracy,dataset,config,q,estimate,lower_discrete,type7_continuous,relative_value_error,continuous_relative_error,rank_interval_distance"
    );
    println!(
        "# stability,dataset,config,q,cases,changed_answers,changed_states,min_estimate,max_estimate"
    );
    let configs = [
        Config::Udd(128, 0.01),
        Config::Udd(512, 0.01),
        Config::Udd(2048, 0.001),
        Config::Current(100),
        Config::Current(200),
        Config::Current(500),
        Config::Old(200),
    ];
    for name in [
        "uniform",
        "lognormal",
        "pareto",
        "outliers",
        "repeated",
        "signed_zero",
        "wide",
    ] {
        let values = data(name, n);
        for cfg in configs {
            metrics(name, cfg, &values);
        }
        for cfg in [
            Config::Udd(512, 0.01),
            Config::Current(200),
            Config::Old(200),
        ] {
            stability(name, cfg, &values);
        }
    }
    let values = data("lognormal", n);
    for cfg in configs {
        costs(cfg, &values);
    }
}
