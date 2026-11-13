# Contributing

Thank you for contributing to Apache DataSketches!

The goal of this document is to provide everything you need to start contributing to this core Rust library.

## Your First Contribution

1. [Fork the DataSketches repository](https://github.com/apache/datasketches-rust/fork) in your own GitHub account.
2. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
3. Make your changes.
4. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the upstream repo. A DataSketches team member should comment and/or review your pull request within a few days. Although, depending on the circumstances, it may take longer.

## Setup

This repo develops Apache® DataSketches™ Core Rust Library Component. To build this project, you will need to set up Rust development first. We highly recommend using [rustup](https://rustup.rs/) for the setup process.

For Linux or macOS users, use the following command:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

For Windows users, download `rustup-init.exe` from [here](https://win.rustup.rs/x86_64) instead.

This project declares its minimum supported Rust version (MSRV) with `rust-version` in the workspace `Cargo.toml`; CI verifies it explicitly. Any toolchain at or above the MSRV works for building and testing.

The lint and check commands additionally require a nightly toolchain:

```shell
rustup toolchain install --component rustfmt,clippy nightly
```

To keep code style consistent, run `cargo x lint --fix` to automatically fix any style issues before committing your changes.

## Build and Test

We recommend using `cargo x` as a single entrypoint (provided by the workspace `xtask` crate). This repo defines the `cargo x` alias in `.cargo/config.toml`, which maps to `cargo run --package x -- ...`.

Build:

```shell
cargo build --workspace
```

Prepare the cross-language serialization test data:

```shell
cargo x prepare-testdata
```

Generate deterministic snapshots from the Rust implementations:

```shell
cargo x generate-snapshots
```

The command writes local files under `serialization/rust/snapshots`. These
generated files are ignored by Git, so the command can be rerun without
changing the working tree.

Test:

```shell
cargo x test
```

Lint:

```shell
cargo x lint
```

Benchmark:

```shell
cargo x bench
```

Benchmarks live in the standalone `benchmarks` crate. Files are grouped first by sketch and then by workload, for example `benchmarks/cpc/serde.rs` and `benchmarks/tdigest/update.rs`. Each workload module registers its scenarios and fixed round counts with the [runner](benchmarks/harness.rs). Keep distinct workloads in separate modules and use names such as `cpc::serde::serialize/200` that identify the sketch, operation, and input.

Compile, list, or smoke-test the benchmarks:

```shell
cargo x bench --no-run
cargo x bench -- --list
cargo x bench -- --test
```

Smoke mode executes every selected implementation for one outer round, preserving input sizes, inner loops, and correctness checks. CI runs this mode without asserting elapsed times. The runner's tests are included in `cargo x test`.

To select workloads and inspect repeated measurements:

```shell
cargo x bench -- cpc::serde --repeat 10 --raw
cargo x bench -- xor::build::8 --repeat 5
cargo x bench -- 'kll::query::quantile/0.5' --exact --rounds 100000
```

Filters are substrings; a scenario must match all supplied filters. Use `--exact` with one complete name to select only that scenario. A filter that matches nothing fails. Runner options can also be passed directly with `cargo bench --package benchmarks --bench benchmarks -- ...`.

### Fixed workloads and timing

[Benchmark In Milliseconds](https://matklad.github.io/2026/10/05/benchmark-milliseconds.html) recommends sizing useful work to take roughly 300 ms. Choose a fixed round count on a reference revision and preserve it across comparisons; the runner never calibrates rounds automatically. Hardware and toolchains affect duration, so 300 ms is a sizing guideline, not a performance assertion.

Each repetition times the entire workload, including fixture construction, input preparation, allocations, output destruction, validation, and cleanup. A round invokes the workload closure once and may process a batch of items. Define the workload and fixture reuse to reflect the operation being measured, and keep input sizes, parameters, timing boundaries, and round counts identical when comparing implementations or revisions.

The runner reports the median and full minimum-to-maximum range in integer milliseconds over five repetitions by default. Use `--raw` to retain individual measurements, including the first run and outliers. Comparison groups interleave implementations and rotate their execution order. `vs ref` divides each median by the first implementation's median; `0.80x` means 20% less elapsed time for the same workload. `--baseline-only` runs just the first implementation. Different scenario rows can represent different amounts of work and are not a throughput ranking.

When adding or resizing a scenario, use `--rounds` to select a suitable fixed count, then record it in the workload registration. If the workload or timing boundaries change, rerun both sides under the new definition. Explain scenario-specific assumptions in the benchmark source, and report the measured revisions, environment, commands, and results in the pull request. Inspect the measured range before drawing conclusions; changing rounds alone does not eliminate noise.

## Public API documentation

- Describe types with noun phrases and API behavior with third-person present-tense verbs such as `Creates`, `Updates`, and `Returns`.
- End summary sentences with punctuation, and format Rust identifiers, literals, and numeric ranges as inline code.
- Put contract sections and compatibility notes before examples. When applicable, order sections as `# Errors`, `# Panics`, and `# Examples`. Include only sections that describe an actual contract.
- Document return values and error conditions on the relevant methods rather than repeating them in crate or module overviews.
- Describe shared capabilities in language-neutral terms. Name another implementation when it clarifies interoperability requirements, a specific source, a format variant, or a behavior difference. Retain useful implementation references in internal comments.
- Keep imports visible in doctests. A trailing hidden `Ok(...)` may be used to support `?`.

## Query semantics

- Validate query arguments before handling an empty sketch. Invalid arguments return `InvalidArgument` regardless of whether the sketch is empty.
- Return a meaningful result when one exists, such as zero, `false`, or an empty collection. Otherwise, use `Option` for an absent result, combined with `Result` when argument validation can fail.

## Visibility

- Let module visibility define the boundary for implementation items. Inside a private module, use `pub` when an item must be available outside its defining module. Inside a `pub(crate)` module, use `pub` when the item should be available wherever that module is visible. Do not repeat an enclosing restriction when it adds no narrower boundary.
- Keep items private when they are used only by their defining module and its descendants.
- For items reachable through a public module or a re-exported public type, use the narrowest visibility that supports their internal callers. Reserve unrestricted `pub` for intentional public API.

## Changelog

- Update `CHANGELOG.md` in the same pull request for significant user-visible changes. Compare the final behavior with the latest release tag rather than recording the sequence of commits that produced it.
- Include public API migrations, new capabilities, correctness or compatibility changes, and meaningful performance improvements. Exclude tests, internal refactors, documentation, CI, tooling, and dependency maintenance unless they change supported or observable behavior.
- Keep the permanent `## Unreleased` section at the top. Group entries under user-facing categories consistent with earlier releases, and add only categories that contain entries.
- Write one bullet for each coherent behavior. Combine related commits, describe the observable impact, and give the required migration for breaking changes. Do not include pull request numbers, issue numbers, discarded intermediate APIs, or implementation history.
- Write from the user's perspective: name the affected public API or workload and its observable result. Omit implementation mechanics unless users need them to migrate, understand compatibility, or assess risk.
- Scope performance claims to the workload supported by evidence. Distinguish broad improvements from scenario-specific benchmark results, and do not generalize a microbenchmark into a library-wide claim.
- During release preparation, insert `## vX.Y.Z` without a date immediately below `## Unreleased` and move the accumulated entries into it. Add the actual UTC release date in `YYYY-MM-DD` format after the release; do not guess it in advance or remove `## Unreleased`.

## Integration test layout

End-to-end tests live in the standalone `tests-integration` crate, which depends on `datasketches` with every sketch feature enabled. Unit tests that require private implementation access remain next to the library code.

### Sketch behavior tests

Non-serialization tests are grouped into one integration-test target per sketch. The target entry point is `tests-integration/tests/<sketch>_test/main.rs`, with operation-specific modules such as `update.rs`, `union.rs`, or `intersection.rs` alongside it. Cargo discovers these directory-style targets automatically.

When adding a case to an existing sketch target, add it to the appropriate module and declare any new module from that target's `main.rs`. To add a sketch target, create its directory and `main.rs`; no Cargo manifest entry or feature gate is needed because `tests-integration` enables every sketch feature.

### Serialization compatibility tests

Cargo automatically discovers `tests-integration/tests/serde_tests.rs`, which aggregates the sketch-specific modules under `tests-integration/tests/serde_tests`.

To add serialization tests for another sketch, add `serde_tests/<sketch>.rs` and its module declaration in `serde_tests.rs`. Shared path handling belongs in `serde_tests.rs`, and serialization fixtures belong in the appropriate subdirectory under `serde_tests`.

## Manual workflow (without xtask)

`cargo x lint` runs the following steps. Use these directly when you need more control or want to isolate failures:

```shell
cargo +nightly clippy --tests --all-features --all-targets --workspace -- -D warnings
cargo +nightly fmt --all --check
taplo format --check
typos
hawkeye check
```

Automatic fix commands:

```shell
cargo +nightly clippy --tests --all-features --all-targets --workspace --allow-staged --allow-dirty --fix
cargo +nightly fmt --all
taplo format
hawkeye format
```

Install the extra tools with:

```shell
cargo install taplo-cli typos-cli hawkeye
```

## Serialization snapshots

Serialization compatibility tests use snapshots from a pinned revision of [`apache/datasketches-tck`](https://github.com/apache/datasketches-tck).

The `cargo x prepare-testdata` command downloads the TCK archive and synchronizes `.sk` files from `serialization_test_data/<language>_generated_files` into:

- `tests-integration/tests/serde_tests/cpp_generated_files`
- `tests-integration/tests/serde_tests/go_generated_files`
- `tests-integration/tests/serde_tests/java_generated_files`

You can synchronize them separately:

```shell
cargo x prepare-testdata cpp
cargo x prepare-testdata go
cargo x prepare-testdata java
```

If no language is specified, all languages are prepared. These directories are not stored in Git. Run the command before the first test run and again whenever the pinned TCK revision changes. It requires network access and replaces the selected generated directories.

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
