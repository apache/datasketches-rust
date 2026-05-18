# CHANGELOG

All significant changes to this project will be documented in this file.

## Unreleased

## v0.3.0 (2026-05-18)

### Breaking changes

* `CountMinSketch` now has a type parameter for the count type. Possible values are `u8` to `u64` and `i8` to `i64`.
* `HllUnion::get_result` is renamed to `HllUnion::to_sketch`.
* `update_f32` and `update_f64` are removed from `ThetaSketch`. Use `hash_value`'s wrapper instead.
* All sketches are now gated by a feature flag. You need to enable the feature flag to use the sketch. For example, to use `CountMinSketch`, you need to enable the `countmin` feature.

### Notable changes

* The MSRV (Minimum Supported Rust Version) is now 1.86.0.

### New features

* New module `hash_value` provides several value wrappers for matching concrete hashing strategies.
* `CountMinSketch` with unsigned values now supports `halve` and `decay` operations.
* `CpcSketch` and `CpcUnion` are now available for cardinality estimation.
* `CpcWrapper` is now available for reading estimation from a serialized CpcSketch without full deserialization.
* `FrequentItemsSketch` now supports serde for any value implement `FrequentItemValue` (builtin supports for `i64`, `u64`, and `String`).
* Expose `codec::SketchBytes`, `codec::SketchSlice`, and `FrequentItemValue` as public API.
* `hll::Coupon` is now public. You can calculate the coupon and reuse it multiple times avoiding the overhead of hashing multiple times.

## v0.2.0 (2026-01-14)

This is the initial release. It includes the following sketches:

* BloomFilter
* CountMinSketch
* FrequentItemsSketch
* HllSketch
* T-Digest
* ThetaSketch
