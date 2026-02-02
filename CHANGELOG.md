# CHANGELOG

All significant changes to this project will be documented in this file.

## Unreleased

### Breaking changes

* `CountMinSketch` now has a type parameter for the count type. Possible values are `u8` to `u64` and `i8` to `i64`.
* `HllUnion::get_result` is renamed to `HllUnion::to_sketch`.

### New features

* `CountMinSketch` with unsigned values now supports `halve` and `decay` operations.
* `CpcSketch` and `CpcUnion` are now available for cardinality estimation.
* `FrequentItemsSketch` now supports serde for `u64` value.

## v0.2.0 (2026-01-14)

This is the initial release. It includes the following sketches:

* BloomFilter
* CountMinSketch
* FrequentItemsSketch
* HllSketch
* T-Digest
* ThetaSketch
