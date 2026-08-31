# CHANGELOG

All significant changes to this project will be documented in this file.

## Unreleased

### Breaking changes

* `BloomFilter::union` and `BloomFilter::intersect` now return `Result`. Callers must handle incompatible filter configurations instead of relying on a panic.
* `CountMinSketch::merge` now returns `Result`. Callers must handle incompatible sketch configurations instead of relying on a panic.
* `CountMinSketch::{suggest_num_buckets, suggest_num_hashes}` now return `Result`. Callers must handle invalid or unsupported targets; successful suggestions are valid inputs to `CountMinSketch::new`.
* `CpcUnion::update` now returns `Result`. Callers must handle seed mismatches instead of relying on a panic.
* Remove `BloomFilterBuilder::suggest_num_bits`, `suggest_num_hashes_from_accuracy`, and `suggest_num_hashes_from_fpp`. Use `with_accuracy(...).build()` for target-based sizing or `with_size(...).build()` for an explicit precomputed configuration.
* `CpcSketch::max_serialized_bytes` now returns `Result` and reports an invalid `lg_k` instead of panicking.
* `FrequentItemsSketch::new` now rejects map sizes below the minimum of 8 instead of silently rounding them up.
* Replace `FrequentItemsSketch::epsilon_for_lg` with the fallible `epsilon_for_max_map_size`, and change `apriori_error` to accept the same maximum map size plus an unsigned stream weight. These helpers now match the constructor's units, and `max_map_size` exposes the configured value.
* Replace the `is_f32` flag on `TDigestMut::deserialize` with separate `deserialize` and `deserialize_f32` entry points, making the serialized precision explicit at the call site.
* Remove `CpcUnion::num_coupons`, which exposed internal union state solely for tests. Inspect the resulting `CpcSketch` when diagnostics are needed.
* Tuple sketch iterators now yield `&TupleEntry<_>` values instead of `(hash, &summary)` pairs. Use `entry.hash()` and `entry.summary()` to inspect each retained entry.
* `ThetaIntersection::to_sketch` and `TupleIntersection::to_sketch` now return `Option`. Callers must handle `None` until the intersection receives its first successful update.
* `BloomFilterBuilder`, `ThetaSketchBuilder`, `ThetaUnionBuilder`, `TupleSketchBuilder`, and `TupleUnionBuilder` now validate their configuration when `build` is called, and `build` returns `Result`. Callers must propagate or handle construction errors.
* `BloomFilterBuilder::{MIN_NUM_BITS, MAX_NUM_BITS, MIN_NUM_HASHES, MAX_NUM_HASHES}` are no longer public. Callers should pass configurations to `build` and handle `InvalidArgument` instead of prevalidating against these constants.
* Fallible sketch and operator constructors now return `Result` directly from `new` or `with_seed`. `TDigestMut` no longer provides `try_new`.

### New features

* `TDigest` can now be serialized and deserialized directly without converting through `TDigestMut` at the call site.
* Add Relative Error Quantiles (REQ) sketches behind the `req` feature, including configurable high- or low-rank accuracy, rank, quantile, PMF, and CDF queries, merging, totally ordered custom item types, the `ReqFloat` adapter for non-NaN floating-point values, and C++/Java-compatible serialization.

### Performance improvements

* Speed up T-Digest workloads that deserialize and merge many Rust-generated partial states, while reducing temporary allocations and retained memory.
* Reduce CPC serialization and deserialization allocations for nontrivial sketches. Local benchmarks show faster processing for larger sketches and roughly unchanged serialization performance for small sparse sketches.

### Bug fixes

* Bloom filter accuracy construction now rejects targets that exceed the maximum serialized filter size instead of silently reducing capacity and violating the requested false-positive probability.
* T-Digest CDF and PMF queries now accept an empty split-point slice and return the single all-values bin instead of panicking.
* Bloom filter deserialization now rejects malformed images with inconsistent counts or payload lengths, while valid images with a dirty cached count are restored correctly.
* `FrequentItemsSketch` now enforces the cross-language map-size limit of `2^30` consistently. Oversized construction returns `InvalidArgument`, and malformed or oversized serialized images return `InvalidData` instead of panicking or attempting excessive allocation.
* `FrequentItemsSketch<String>` now rejects an encoded string length that exceeds the remaining input before allocating the string buffer.
* T-Digest compression now supports `k = u16::MAX` without overflowing.
* T-Digest rejects truncated serialized payloads before allocating, and updating a deserialized digest no longer allows its buffered state to grow without bound.
* Compact HLL4 images now restore all register values correctly.
* `HllSketch::lower_bound` now uses the number of non-zero registers as a floor in HLL mode, matching Java, C++, and Go and avoiding a bound below the distinct count already proven by register hits.
* `HllUnion` now keeps a single HLL-mode input's estimate stable when copying or downsampling it and keeps confidence bounds consistent across HLL4, HLL6, and HLL8 result types, matching Java and C++.
* `ThetaJaccardSimilarity` and `TupleJaccardSimilarity` now report an exact similarity of `1.0` for two non-empty sketches that share a theta and retain no entries, matching Java and C++ and agreeing with `exactly_equal` on the same pair. Such pairs, which arise from a low sampling probability, previously returned the uncertain `{0.0, 0.5, 1.0}` interval, so a sketch was not similar to itself.
* HLL, Theta, and Tuple deserializers now return `InvalidData` for malformed payload sizes and entry counts instead of risking oversized allocations or decoding failures.
* Malformed CPC images now return `InvalidData` instead of panicking.
* Seeded deserializers now return `InvalidData` rather than panicking when the caller supplies a seed whose hash is the reserved zero value.
* Fix T-Digest interpolation and tail calculations that could produce non-monotonic or out-of-range quantiles and invalid rank, CDF, or PMF values.

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
