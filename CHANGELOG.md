# CHANGELOG

All significant changes to this project will be documented in this file.

## Unreleased

### Breaking changes

* Change `ThetaIntersection::to_sketch` and `TupleIntersection::to_sketch` to return `Option`. Callers must handle `None` before the first successful update; after that, the methods return `Some` even when the intersection is empty.

### New features

* Add Relative Error Quantiles (REQ) sketches behind the `req` feature, including configurable high- or low-rank accuracy, rank, quantile, PMF, and CDF queries, merging and unions, and C++/Java-compatible serialization.

### Performance improvements

* Reduce T-Digest allocation overhead and retained memory across updates, compression, merges, serialization, deserialization, and freezing while preserving the serialized format.

### Bug fixes

* `FrequentItemsSketch::deserialize` now rejects headers whose `lg_max_map_size` or `lg_cur_map_size` fields are out of range instead of panicking (debug builds) or requesting an oversized allocation (release builds) on corrupt input. It also bounds the initial backing-map allocation to the payload: an empty header builds the map at the minimum size instead of honoring `lg_cur_map_size` (so an 8-byte empty header claiming `lg_cur = 30` no longer drives a `1 << 30`-slot allocation), and a non-empty header is rejected when `active_items` is inconsistent with `lg_cur_map_size` or with the remaining serialized bytes.
* T-Digest compression now handles `k = u16::MAX` without overflowing the scale normalization input.
* T-Digest deserialization now validates declared payload lengths before allocating. Updating a deserialized digest whose unmerged buffer already exceeds the compression threshold now compresses it instead of allowing the buffer to grow without bound.

## v0.4.0 (2026-08-18)

### Breaking changes

* Move the `hash_value` module to `hash::value`.
* Rename `Coupon::from_hash` to `Coupon::from_value` to reflect that the method hashes the supplied value itself.
* Remove `ThetaSketch::builder`; construct `ThetaSketchBuilder` with `Default::default` instead.
* Replace the sealed `ThetaSketchView` trait with a concrete borrowed `ThetaSketchView<'a>`. Call `as_view()` when a view value is needed; Theta set operations continue to accept references to mutable and compact sketches directly.
* Change `ThetaSketch` and `CompactThetaSketch` iterators to yield `ThetaEntry` instead of raw `u64` hashes. Call `ThetaEntry::hash` to access a retained hash.
* Replace `ThetaIntersection::new(seed)` and `new_with_default_seed()` with `with_seed(seed)` and `Default::default()`, respectively. Replace `result()` and `result_with_ordered(ordered)` with `to_sketch(ordered)`.
* Make `CountMinValue` and `UnsignedCountMinValue` marker traits. Their previously exposed numeric constants and helper methods have been removed; use the corresponding primitive integer operations and conversions directly.

### New features

* Add Tuple sketches behind the `tuple` feature, including custom summary update and combination policies, serialization, compact sketches, union, intersection, A-not-B, Jaccard similarity, and exact sketch-state equality checks that ignore summary values.
* Add Theta union, A-not-B, Jaccard similarity, and exact sketch-state equality checks.
* `FrequentItemsSketch` now supports borrowed-key updates via `update_ref` and `update_with_count_ref`, allowing sketches such as `FrequentItemsSketch<String>` to update from `&str` without allocating on existing-key hits. Frequency queries also accept borrowed key forms matching `Borrow<Q>`.
* `FrequentItemsSketch` no longer requires item types to implement `Clone` for core updates, queries, and serialization. Custom `FrequentItemValue` implementations can now be non-`Clone`; APIs that return or merge owned items still require `Clone`.
* Add `estimated_size()` to `BloomFilter`, `CountMinSketch`, `CpcSketch`, `FrequentItemsSketch`, `HllSketch`, `TDigestMut`, `TDigest`, mutable and compact Theta and Tuple sketches, and the stateful `HllUnion`, `CpcUnion`, `ThetaUnion`, `ThetaIntersection`, `TupleUnion`, and `TupleIntersection` operators.

### Bug fixes

* HLL serialization now emits the compact auxiliary-map flag required for Java to read HLL4 images and matches Java and C++ coupon ordering for compact Set images.
* `FrequentItemsSketch::serialize` now writes the full 8-byte preamble for an empty sketch, matching the Java and C++ encoding. Empty sketches previously serialized to 6 bytes, which `FrequentItemsSketch::deserialize` rejected with an insufficient-data error.
* `FrequentItemsSketch` now preserves total weight and error state across serialization and merge when a purge removes every active item.
* T-Digest interpolation now keeps quantiles finite for extreme finite inputs. Deserialization rejects non-finite extrema, invalid centroid weights, and total-weight overflow as invalid data instead of allowing invalid state or panicking.
* Legacy Theta version 2 exact images with retained entries now deserialize as non-empty sketches and preserve their entries and exact estimates.
* Bloom filter deserialization now rejects inconsistent cached bit counts, preventing malformed images from hiding populated bits and violating the no-false-negative guarantee.
* `CpcSketch` and `CpcWrapper` now classify out-of-range fields in serialized images as `InvalidData` rather than `InvalidArgument`.

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
