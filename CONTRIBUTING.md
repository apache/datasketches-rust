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

Rustup will read the `rust-toolchain.toml` file and set up everything else automatically. To ensure that everything works correctly, run `cargo version` under the root directory:

```shell
cargo version
# cargo 1.86.0 (<hash> <date>)
```

To keep code style consistent, run `cargo x lint --fix` to automatically fix any style issues before committing your changes.

## Build and Test

We recommend using `cargo x` as a single entrypoint (provided by the workspace `xtask` crate). This repo defines the `cargo x` alias in `.cargo/config.toml`, which maps to `cargo run --package x -- ...`.

Build:

```shell
cargo build --workspace
```

Test:

```shell
cargo x test
# or
cargo test --workspace --all-features
```

Lint:

```shell
cargo x lint
```

## Integration test layout

Integration tests for the `datasketches` crate live under `datasketches/tests` and use two entry-point patterns.

### Sketch behavior tests

Non-serialization tests are grouped into one integration-test target per sketch. The target entry point is `datasketches/tests/<sketch>_test/main.rs`, with operation-specific modules such as `update.rs`, `union.rs`, or `intersection.rs` alongside it.

Because these entry points are nested below `tests`, Cargo does not discover them automatically. Each new sketch target must also be registered in `datasketches/Cargo.toml` with its required feature:

```toml
[[test]]
name = "tuple_test"
path = "tests/tuple_test/main.rs"
required-features = ["tuple"]
```

When adding a case to an existing sketch target, add it to the appropriate module and declare any new module from that target's `main.rs`; no Cargo manifest change is needed. Add another `[[test]]` entry only when introducing a new sketch target.

### Serialization compatibility tests

Cargo automatically discovers `datasketches/tests/serde_tests.rs`, which aggregates the sketch-specific modules under `datasketches/tests/serialization_tests`. Each module is gated by its corresponding sketch feature in `serde_tests.rs`.

To add serialization tests for another sketch, add `serialization_tests/<sketch>.rs` and a feature-gated module declaration in `serde_tests.rs`. Do not add a separate `[[test]]` entry. Shared path handling belongs in `serialization_tests/support.rs`, and serialization fixtures belong in the appropriate subdirectory under `serialization_tests`.

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
hawkeye format --fail-if-updated=false
```

Install the extra tools with:

```shell
cargo install taplo-cli typos-cli hawkeye
```

## Serialization snapshots

Some tests depend on snapshot files under `datasketches/tests/serialization_tests`. If they are missing, tests will fail. Download them with:

```shell
python3 ./tools/download_serialization_test_data.py --all
```

The script downloads the latest snapshots from the `main` branch of
[`apache/datasketches-tck`](https://github.com/apache/datasketches-tck) and writes them to:

- `datasketches/tests/serialization_tests/java_generated_files`
- `datasketches/tests/serialization_tests/cpp_generated_files`

You can download them separately:

```shell
python3 ./tools/download_serialization_test_data.py --java
python3 ./tools/download_serialization_test_data.py --cpp
```

The script requires Python 3 and network access.

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
