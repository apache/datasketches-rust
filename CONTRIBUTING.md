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
# cargo 1.85.0 (<hash> 2024-12-31)
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
cargo test --workspace --no-default-features
```

Lint:

```shell
cargo x lint
```

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

## Serialization snapshots and test data generation

Some tests depend on snapshot files under `datasketches/tests/serialization_test_data`. If they are missing, tests will fail. Regenerate them with:

```shell
python3 ./tools/generate_serialization_test_data.py --all
```

The script pulls `datasketches-java` and `datasketches-cpp` and writes files to:

- `datasketches/tests/serialization_test_data/java_generated_files`
- `datasketches/tests/serialization_test_data/cpp_generated_files`

You can generate them separately:

```shell
python3 ./tools/generate_serialization_test_data.py --java
python3 ./tools/generate_serialization_test_data.py --cpp
```

The script requires these commands on PATH (and network access):

- Java data: `git`, `java`, `mvn`
- C++ data: `git`, `cmake`, `ctest`

The current `datasketches-java` generation flow requires JDK >= 25 and Maven >= 3.9.11, otherwise Maven Enforcer will fail.

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
