# Contributing

Thank you for contributing to Apache® DataSketches™ Core Rust Library Component!

Build, test, API, documentation, and changelog guidelines for all contributors (human and AI) live in [AGENTS.md](AGENTS.md). Read it before planning or submitting changes.

## Your First Contribution

1. [Fork the DataSketches repository](https://github.com/apache/datasketches-rust/fork) in your own GitHub account.
2. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
3. Make your changes following [AGENTS.md](AGENTS.md), and run `cargo x check`, `cargo x test`, and `cargo x lint`.
4. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the upstream repository.

## Setup

We recommend [rustup](https://rustup.rs/) to install Rust. On Linux or macOS:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

On Windows, download `rustup-init.exe` from [win.rustup.rs](https://win.rustup.rs/x86_64).

The workspace declares its minimum supported Rust version (MSRV) via `rust-version` in the root `Cargo.toml`; CI verifies it explicitly. Any toolchain at or above the MSRV works for building and testing.

Check, lint, and documentation commands additionally require a nightly toolchain:

```shell
rustup toolchain install --component rustfmt,clippy nightly
```

`cargo x lint` installs extra formatting and check tools (`taplo-cli`, `typos-cli`, `hawkeye`) automatically when missing.

## Code of Conduct

All community participation is governed by the [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
