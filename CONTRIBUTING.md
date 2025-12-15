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

To keep code style consistent, we use the following tools:

* Nightly `rustfmt` for code formatting: `cargo +nightly fmt --all -- --check`
* Nightly `clippy` for linting: `cargo +nightly clippy --all-targets --all-features -- -D warnings`
* [`typos`](https://github.com/crate-ci/typos) for spell checking: `cargo install typos-cli` and then `typos`
* [`taplo`](https://taplo.tamasfe.dev/) for checking `toml` files: `cargo install taplo-cli` and then `taplo check`
* [`hawkeye`](https://github.com/korandoru/hawkeye) for checking license header: `cargo install hawkeye` and then `hawkeye check`

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
