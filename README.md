<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache® DataSketches™ Core Rust Library Component

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MSRV 1.86.0][msrv-badge]](https://www.whatrustisit.com)
[![Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/datasketches.svg
[crates-url]: https://crates.io/crates/datasketches
[docs-badge]: https://img.shields.io/docsrs/datasketches
[docs-url]: https://docs.rs/datasketches
[msrv-badge]: https://img.shields.io/badge/MSRV-1.86.0-green?logo=rust
[license-badge]: https://img.shields.io/crates/l/datasketches
[license-url]: https://www.apache.org/licenses/LICENSE-2.0
[actions-badge]: https://github.com/apache/datasketches-rust/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/apache/datasketches-rust/actions/workflows/ci.yml

Apache DataSketches Rust provides stochastic streaming algorithms for answering queries over large data sets with compact, mergeable summaries. It is the core Rust component of Apache DataSketches and currently implements a subset of the algorithms available in the other language components.

## Getting started

Sketch implementations are opt-in Cargo features; the crate enables none by default. For example, add the HyperLogLog implementation with:

```shell
cargo add datasketches --features hll
```

Then build a sketch and query its distinct-count estimate:

```rust
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;

let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
for user in ["alice", "bob", "alice", "carol"] {
    sketch.update(user);
}

assert!(sketch.estimate() >= 3.0);
```

Enable multiple algorithms by listing their features together, such as `features = ["hll", "theta"]` in `Cargo.toml`.

## Available sketches

| Feature       | Main types                            | Use case                                                                                          |
| ------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `bloom`       | `BloomFilter`                         | Space-efficient probabilistic set membership with a configurable false-positive rate.             |
| `countmin`    | `CountMinSketch`                      | Approximate point-frequency queries over a stream.                                                |
| `cpc`         | `CpcSketch`, `CpcUnion`, `CpcWrapper` | Highly compact distinct-count estimation and unions.                                              |
| `frequencies` | `FrequentItemsSketch`                 | Heavy-hitter discovery with upper and lower frequency bounds.                                     |
| `hll`         | `HllSketch`, `HllUnion`               | Fast distinct-count estimation and unions.                                                        |
| `req`         | `ReqSketch`                           | Relative-error quantile, rank, PMF, and CDF queries with configurable high- or low-rank accuracy. |
| `tdigest`     | `TDigestMut`, `TDigest`               | Quantile and rank estimation, with high accuracy near distribution tails.                         |
| `theta`       | `ThetaSketch` and set operations      | Distinct counts, set expressions, and Jaccard similarity.                                         |
| `tuple`       | `TupleSketch` and set operations      | Theta-style keys with user-defined summaries attached to retained entries.                        |

See the [API documentation](https://docs.rs/datasketches) for configuration, accuracy guarantees, serialization, and examples for each algorithm.

## Compatibility

The minimum supported Rust version is 1.86.0. The crate currently supports little-endian targets only.

Supported serialization formats are tested with fixtures produced by Apache DataSketches Java, C++, and Go through the [DataSketches TCK](https://github.com/apache/datasketches-tck). When values must hash identically across language implementations, use the compatibility wrappers in `hash::value`.

See the [changelog](CHANGELOG.md) for release notes and migration guidance.

## Other language implementations

Apache DataSketches also provides core library components for other languages:

- [Java](https://github.com/apache/datasketches-java)
- [C++](https://github.com/apache/datasketches-cpp)
- [Python](https://github.com/apache/datasketches-python)
- [Go](https://github.com/apache/datasketches-go)

Visit the [Apache DataSketches website](https://datasketches.apache.org) for algorithm documentation, research background, and project-wide resources.

## Community and contributing

Questions, bug reports, and feature requests are welcome through [GitHub issues](https://github.com/apache/datasketches-rust/issues) and [GitHub discussions](https://github.com/apache/datasketches-rust/discussions). The [Apache DataSketches community page](https://datasketches.apache.org/docs/Community/) lists the public mailing lists and other ways to participate.

See [CONTRIBUTING.md](CONTRIBUTING.md) to build, test, and contribute to the Rust component. All project participation is governed by the [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

To report a security vulnerability, follow the [ASF security reporting process](https://www.apache.org/security/) instead of opening a public issue.

## License

Licensed under the [Apache License, Version 2.0][license-url].
