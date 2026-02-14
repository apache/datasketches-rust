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
[![MSRV 1.85.0][msrv-badge]](https://www.whatrustisit.com)
[![Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/datasketches.svg
[crates-url]: https://crates.io/crates/datasketches
[docs-badge]: https://img.shields.io/docsrs/datasketches
[docs-url]: https://docs.rs/datasketches
[msrv-badge]: https://img.shields.io/badge/MSRV-1.85.0-green?logo=rust
[license-badge]: https://img.shields.io/crates/l/datasketches
[license-url]: LICENSE
[actions-badge]: https://github.com/apache/datasketches-rust/workflows/CI/badge.svg
[actions-url]: https://github.com/apache/datasketches-rust/actions?query=workflow%3ACI

This is the core Rust component of the DataSketches library.  It contains a subset of the sketching algorithms and can be accessed directly from user applications.

Note that we have parallel core library components for Java, C++, Python, and Go implementations of many of the same sketch algorithms:

- [datasketches-java](https://github.com/apache/datasketches-java)
- [datasketches-cpp](https://github.com/apache/datasketches-cpp)
- [datasketches-python](https://github.com/apache/datasketches-python)
- [datasketches-go](https://github.com/apache/datasketches-go)

Please visit the main [DataSketches website](https://datasketches.apache.org) for more information.

If you are interested in making contributions to this site, please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.
