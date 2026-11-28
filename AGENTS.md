# Repository Guide

This document is the single source of truth for workspace structure, development commands, testing, API conventions, and changelog rules for both human contributors and coding agents. Keep documented workflows synchronized with structural changes, and run the applicable `cargo x check`, `cargo x test`, and `cargo x lint` commands before submitting or handing work back.

For first-time setup (`rustup`, MSRV, nightly toolchain) and community policies, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Workspace Layout

| Crate               | Purpose                                                                                             |
| ------------------- | --------------------------------------------------------------------------------------------------- |
| `datasketches`      | Core library crate (`--no-default-features` by default; each sketch family is an opt-in feature).   |
| `tests-integration` | End-to-end sketch behavior and cross-language serialization compatibility tests.                    |
| `benchmarks`        | Fixed-workload benchmark suite and runner; see [benchmarks/AGENTS.md](benchmarks/AGENTS.md).        |
| `examples`          | Standalone runnable end-to-end scenarios; see [examples/AGENTS.md](examples/AGENTS.md).             |
| `datagen`           | Local Rust serialization snapshot generator for cross-language compatibility fixtures.              |
| `xtask`             | Workspace automation entrypoint exposed via the `cargo x` alias defined in `.cargo/config.toml`.    |

## Development Workflow

Use `cargo x` as the single entrypoint for repository checks and automation:

```shell
cargo build --workspace
cargo x check              # Check datasketches across --no-default-features, each feature, and --all-features
cargo x prepare-testdata   # Populate cross-language and local Rust serialization snapshots
cargo x test               # Run the workspace test suite with all sketch features enabled (--no-capture supported)
cargo x lint               # Run nightly clippy, rustfmt, docsrs rustdoc check, taplo, typos, and hawkeye
cargo x lint --fix         # Automatically apply clippy, rustfmt, taplo, and hawkeye fixes
cargo x docs               # Build and open crate documentation
cargo x bench -- --test    # Smoke-test benchmarks (see benchmarks/AGENTS.md for benchmark usage)
```

`cargo x check`, `cargo x lint`, and `cargo x docs` require a nightly toolchain with `rustfmt` and `clippy`. CI also verifies the workspace MSRV declared in `Cargo.toml`. To isolate an individual lint or check step without `xtask`, inspect the underlying commands directly in `xtask/src/`.

## Testing

End-to-end tests live in the standalone `tests-integration` crate, which depends on `datasketches` with every sketch feature enabled. Unit tests that require private implementation access remain next to the library code.

### Test Design and Restraint

- Reserve `#[cfg(test)]` modules in `datasketches/src/` for low-level mathematical, bit-packing, lookup-table, hash, or codec invariants that require private access and cannot be isolated cleanly through public APIs. Place all public-API behavior tests in `tests-integration/tests/<sketch>_test/` and all binary wire-format or serialization tests in `tests-integration/tests/serde_tests/<sketch>.rs`.
- Test observable contracts (estimates, bounds, ranks, quantiles, extrema, retained counts, serialization round-trips, `ErrorKind`, and error atomicity), never private struct fields, internal caches or laziness flags (`OnceLock`, sorted flags, or hash slots), `Vec` capacities, internal promotion thresholds, or uncontracted `estimated_size` side effects. Refactoring internal data structures or fast paths without changing observable behavior must not break tests.
- Validate pure performance optimizations against the existing test suite and workloads in `benchmarks/` rather than adding white-box unit tests or new integration test functions. Add a behavior assertion during performance work only when it uncovers an untested public input shape (such as unsorted or duplicate batch query slices), extending an existing integration test rather than duplicating setup.
- Audit existing `<sketch>_test` and `serde_tests` coverage before adding tests. Fold related input cases into an existing test or parameterized loop instead of adding near-duplicate test functions.
- Omit tests that restate literal constants or trivial formulas, check compiler-derived traits (`Clone`, `Debug`, `Send`, `Sync`), or match non-contract `Debug` or `ErrorKind::InvalidData` diagnostic message substrings (assert `ErrorKind` instead, checking message parameters only where compatibility error contracts require expected and actual values).

### Sketch Behavior Tests

Non-serialization tests are grouped into one directory-style integration-test target per sketch at `tests-integration/tests/<sketch>_test/main.rs`, with operation-specific modules such as `update.rs`, `union.rs`, or `intersection.rs` alongside it. Cargo discovers these targets automatically, so adding a target requires no Cargo manifest entry or feature gate. When adding cases to an existing sketch target, place them in the appropriate module and declare any new module in that target's `main.rs`.

### Serialization Compatibility and Snapshots

Cargo discovers `tests-integration/tests/serde_tests.rs`, which aggregates per-sketch modules under `tests-integration/tests/serde_tests/<sketch>.rs`. Shared path handling belongs in `serde_tests.rs`, and fixtures belong in subdirectories under `tests-integration/tests/serde_tests`.

Serialization tests use cross-language snapshots from a pinned revision of [`apache/datasketches-tck`](https://github.com/apache/datasketches-tck) (`cpp`, `go`, `java`) together with locally generated Rust snapshots from `datagen` (`rust`). Run `cargo x prepare-testdata` (or specify individual languages such as `cargo x prepare-testdata rust`) to populate the gitignored `tests-integration/tests/serde_tests/<lang>_generated_files` directories. Run it before the first test run and whenever the pinned TCK revision or Rust serialization behavior changes. Preparing `cpp`, `go`, or `java` requires network access; `cargo x prepare-testdata rust` runs offline.

To run `datagen` directly with a custom output directory or sketch filter:

```shell
cargo run --release --package datagen -- --output-dir target/snapshots --sketch hll --sketch theta
```

## API Conventions

### Naming

- Use `total_weight` for accumulated stream weight (`u64` for unsigned streams, or the counter type `T` for signed or scaled counters); do not use `n`.
- Use `num_retained` for the number of entries or centroids retained by a sketch. Reserve `len` and `is_empty` for slice-like snapshot views (such as sorted views).
- Pair unit-weight and weighted ingestion as `update` and `update_with_weight`.
- Use `min_item` / `max_item` returning `Option<&T>` for generic item extrema, and `min_value` / `max_value` returning `Option<f64>` for numeric floating-point extrema.
- Prefer `new` (and `with_*` for alternate required configuration such as `with_seed` or `with_kernel`) plus `Default` when a standard parameterless default exists. Use a dedicated `*Builder` type with `.build() -> Result<_, Error>` when construction has multiple optional settings or staged input collection.

### Errors, Panics, and Query Semantics

- Return `Result<_, Error>` for three failure classes: invalid caller domain arguments (`ErrorKind::InvalidArgument`), runtime incompatibility between combined sketches (`ErrorKind::InvalidArgument`), and malformed or invariant-violating serialized inputs (`ErrorKind::InvalidData`).
- Validate query arguments and multi-sketch compatibility before taking an empty-sketch fast path. Invalid arguments and incompatible peer sketches return `InvalidArgument` regardless of whether a sketch is empty.
- Include expected and actual parameter values in compatibility error messages rather than exposing separate `is_compatible` pre-check methods.
- Keep failing mutations and merges atomic with respect to observable state: returning `Err` must leave `self` unchanged.
- For queries, return a meaningful value when one exists (`0`, `false`, or an empty collection). Otherwise, use `Option` for an absent result, combined with `Result` when argument validation can fail.
- Reserve `Result` on mutations, merges, and set operations for domain argument validation or peer configuration incompatibility rather than making otherwise infallible streaming operations fallible solely for counter or stream-weight exhaustion (`u64::MAX` or `T::MAX`). Infallible updates and merges return `()`, panic without modifying `self` on overflow, and document the condition under `# Panics`.

### Traits and Attributes

- Omit `PartialEq` and `Eq` on probabilistic sketches, filters, and types whose equality would reflect internal layout, stream history, random seeds, or floating-point bounds rather than a well-defined domain equality contract. Keep `PartialEq` and `Eq` on configuration enums, structured result rows, and input wrappers.
- Derive the applicable standard traits (`Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`) on configuration enums and plain value types. Implement `Debug` on custom iterators and view types without adding unnecessary generic bounds (such as `T: Debug`).
- Avoid routine `#[must_use]` annotations on getters, queries, or consuming builders. Reserve explicit `#[must_use = "..."]` messages for lazy iterators and `&self` imperative methods that callers could mistake for in-place mutators (such as `compact`).

### Public API Documentation

- Describe types with noun phrases and API behavior with third-person present-tense verbs such as `Creates`, `Updates`, and `Returns`.
- End summary sentences with punctuation, and format Rust identifiers, literals, and numeric ranges as inline code.
- Put contract and compatibility sections before examples. When applicable, order sections as `# Errors`, `# Panics`, `# Cross-language compatibility`, and `# Examples`, including only sections that apply.
- Document return values and error conditions on the relevant methods rather than repeating them in crate or module overviews.
- Describe this crate's own capabilities and contracts in language-neutral terms first. Name another implementation (`DataSketches Java`, `DataSketches C++`, `DataSketches Go`, or `DataSketches Python`) only when documenting interoperability requirements, format variants, or behavior differences; retain repository references such as `datasketches-java` in internal code comments.
- Keep imports visible in doctests. A trailing hidden `Ok(...)` may be used to support `?`.

#### Cross-language compatibility

Use consistent patterns for the three kinds of cross-language guidance:

- **Serialization interchange**: On `serialize` methods, state `The serialized data can be read by other DataSketches implementations [when ...].` On `deserialize` methods, state `Also accepts data serialized by other DataSketches implementations [when ...].` State any format-specific caveats (such as unstored configuration or incompatible counter mappings) immediately after that sentence.
- **Hash input encoding**: On modules whose operations hash caller inputs, add a `# Cross-language hashing` section listing the required `hash::value` wrappers (`raw_bytes`, `canonical_float`, and `sign_extend` or `natural_extend`) and any empty-input handling needed to match peer implementations.
- **API behavior and range differences**: When a method or type intentionally differs from peer implementations in accepted inputs, edge-case behavior, or value bounds, keep the summary and `# Errors` or `# Panics` sections focused on the Rust contract and record the difference under `# Cross-language compatibility`. Name the peer implementations, contrast their behavior or domain with this crate's behavior, and state any narrower bound callers must stay within when exchanging serialized images (for example, keeping `u64` stream weights at or below `i64::MAX as u64` for DataSketches Java readers).

### Visibility

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
