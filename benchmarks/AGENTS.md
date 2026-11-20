# Benchmarks

Benchmarks live in the standalone `benchmarks` workspace crate. Files are grouped first by sketch and then by workload, for example `benchmarks/cpc/serde.rs` and `benchmarks/tdigest/update.rs`. Each workload module registers its scenarios and fixed round counts with [harness.rs](harness.rs). Keep distinct workloads in separate modules and use names such as `cpc::serde::serialize/200` that identify the sketch, operation, and input.

## Running Benchmarks

Run, compile, list, or smoke-test benchmarks through `cargo x bench`:

```shell
cargo x bench
cargo x bench --no-run
cargo x bench -- --list
cargo x bench -- --test
```

Smoke mode (`--test`) executes every selected implementation for one outer round, preserving input sizes, inner loops, and correctness checks. CI runs this mode without asserting elapsed times. The runner's unit tests are included in `cargo x test`.

Select workloads and inspect repeated measurements with filters and runner flags:

```shell
cargo x bench -- cpc::serde --repeat 10 --raw
cargo x bench -- xor::build::8 --repeat 5
cargo x bench -- 'kll::query::quantile/0.5' --exact --rounds 100000
```

Filters are substrings; a scenario must match all supplied filters. Use `--exact` with one complete name to select only that scenario. A filter that matches nothing fails. Runner options can also be passed directly with `cargo bench --package benchmarks --bench benchmarks -- ...`.

## Fixed Workloads and Timing

[Benchmark In Milliseconds](https://matklad.github.io/2026/10/05/benchmark-milliseconds.html) recommends sizing useful work to take roughly 300 ms. Choose a fixed round count on a reference revision and preserve it across comparisons; the runner never calibrates rounds automatically. Hardware and toolchains affect duration, so 300 ms is a sizing guideline, not a performance assertion.

Each repetition times the entire workload, including fixture construction, input preparation, allocations, output destruction, validation, and cleanup. A round invokes the workload closure once and may process a batch of items. Define the workload and fixture reuse to reflect the operation being measured, and keep input sizes, parameters, timing boundaries, and round counts identical when comparing implementations or revisions.

The runner reports the median and full minimum-to-maximum range in integer milliseconds over five repetitions by default. Use `--raw` to retain individual measurements, including the first run and outliers. Comparison groups interleave implementations and rotate their execution order. `vs ref` divides each median by the first implementation's median; `0.80x` means 20% less elapsed time for the same workload. `--baseline-only` runs just the first implementation. Different scenario rows can represent different amounts of work and are not a throughput ranking.

When adding or resizing a scenario, use `--rounds` to select a suitable fixed count, then record it in the workload registration. If the workload or timing boundaries change, rerun both sides under the new definition. Explain scenario-specific assumptions in the benchmark source, and report the measured revisions, environment, commands, and results in the pull request. Inspect the measured range before drawing conclusions; changing rounds alone does not eliminate noise.
