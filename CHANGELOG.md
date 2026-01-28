# CHANGELOG

All significant changes to this project will be documented in this file.

## Unreleased

### Breaking changes

* `CountMinSketch` now has a type parameter for the count type. Possible values are `u8` to `u64` and `i8` to `i64`.

### New features

* `CountMinSketch` with unsigned values now supports `halve` and `decay` operations.

## v0.2.0 (2025-01-14)

This is the initial release. It includes the following sketches:

* BloomFilter
* CountMinSketch
* FrequentItemsSketch
* HllSketch
* T-Digest
* ThetaSketch
