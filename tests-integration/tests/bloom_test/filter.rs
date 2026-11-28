// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datasketches::bloom::BloomFilter;
use datasketches::bloom::BloomFilterBuilder;
use datasketches::error::ErrorKind;
use datasketches::hash::value::raw_bytes;
use googletest::assert_that;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::le;

const NUM_BITS: u64 = 65_536;
const NUM_HASHES: u16 = 5;
const SEED: u64 = 123;

fn filter() -> BloomFilter {
    BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
        .seed(SEED)
        .build()
        .unwrap()
}

fn filter_snapshot(filter: &BloomFilter) -> (u64, u16, u64, u64, Vec<u8>) {
    (
        filter.capacity(),
        filter.num_hashes(),
        filter.seed(),
        filter.bits_used(),
        filter.serialize(),
    )
}

#[test]
fn lifecycle_statistics_and_reset() {
    let mut filter = filter();
    let empty = filter.clone();
    assert_eq!(filter_snapshot(&filter), filter_snapshot(&empty));
    assert_eq!(filter.capacity(), NUM_BITS);
    assert_eq!(filter.num_hashes(), NUM_HASHES);
    assert_eq!(filter.seed(), SEED);
    assert!(filter.is_empty());
    assert_eq!(filter.bits_used(), 0);
    assert_eq!(filter.load_factor(), 0.0);
    assert_eq!(filter.estimated_fpp(), 0.0);
    assert!(!filter.contains("apple"));
    assert_that!(filter.estimated_size(), ge((NUM_BITS / 8) as usize));

    let larger = BloomFilterBuilder::with_size(NUM_BITS * 2, NUM_HASHES)
        .build()
        .unwrap();
    assert_that!(larger.estimated_size(), gt(filter.estimated_size()));

    assert!(!filter.contains_and_insert("apple"));
    assert!(filter.contains_and_insert("apple"));
    assert!(filter.contains("apple"));
    assert!(!filter.is_empty());
    assert_ne!(filter_snapshot(&filter), filter_snapshot(&empty));
    assert_that!(filter.bits_used(), gt(0));
    assert_that!(filter.load_factor(), gt(0.0));
    assert_that!(filter.estimated_fpp(), gt(0.0));

    filter.reset();
    assert_eq!(filter_snapshot(&filter), filter_snapshot(&empty));
    assert!(filter.is_empty());
    assert_eq!(filter.bits_used(), 0);
    assert_eq!(filter.load_factor(), 0.0);
    assert_eq!(filter.estimated_fpp(), 0.0);
    assert!(!filter.contains("apple"));
}

#[test]
fn insert_and_contains_and_insert_are_equivalent() {
    let mut inserted = filter();
    let mut checked = inserted.clone();

    for value in 0..128_u64 {
        let was_present = inserted.contains(value);
        assert_eq!(checked.contains_and_insert(value), was_present);
        inserted.insert(value);
        assert!(inserted.contains(value));
        assert_eq!(filter_snapshot(&checked), filter_snapshot(&inserted));
    }
}

#[test]
fn equivalent_borrowed_and_value_inputs_match() {
    let owned_text = String::from("apple");
    let owned_bytes = vec![1_u8, 2, 3, 4];
    let ids = [7_u64, 11_u64];

    let mut from_borrows = filter();
    from_borrows.insert(&owned_text);
    from_borrows.insert(&owned_bytes);
    for id in &ids {
        from_borrows.insert(id);
    }
    from_borrows.insert(raw_bytes::from_string(String::from("payload")));

    let mut from_slices_and_values = filter();
    assert!(!from_slices_and_values.contains_and_insert(owned_text.as_str()));
    assert!(!from_slices_and_values.contains_and_insert(owned_bytes.as_slice()));
    for id in ids {
        assert!(!from_slices_and_values.contains_and_insert(id));
    }
    assert!(!from_slices_and_values.contains_and_insert(raw_bytes::from_str("payload")));

    assert_eq!(
        filter_snapshot(&from_borrows),
        filter_snapshot(&from_slices_and_values)
    );

    assert!(from_borrows.contains(&owned_text));
    assert!(from_borrows.contains(owned_text.as_str()));
    assert!(from_borrows.contains(&owned_bytes));
    assert!(from_borrows.contains(owned_bytes.as_slice()));
    for id in &ids {
        assert!(from_borrows.contains(id));
        assert!(from_borrows.contains(*id));
    }
    assert!(from_borrows.contains(raw_bytes::from_slice(b"payload")));

    assert!(from_slices_and_values.contains_and_insert(&owned_text));
    assert!(from_slices_and_values.contains_and_insert(&owned_bytes));
    assert_eq!(
        filter_snapshot(&from_slices_and_values),
        filter_snapshot(&from_borrows)
    );
}

#[test]
fn within_item_collisions_and_repeated_inserts_preserve_bit_count() {
    // Exercise one hash and force within-item collisions: 65 hashes cannot
    // select distinct positions in a 64-bit filter.
    for num_hashes in [1, 65] {
        let mut inserted = BloomFilterBuilder::with_size(64, num_hashes)
            .build()
            .unwrap();
        let mut checked = inserted.clone();
        inserted.insert(42_u64);
        assert!(!checked.contains_and_insert(42_u64));

        for filter in [&mut inserted, &mut checked] {
            let before_repeat = filter.clone();
            filter.insert(42_u64);
            assert_eq!(filter_snapshot(filter), filter_snapshot(&before_repeat));
            assert!(filter.contains_and_insert(42_u64));
            assert_eq!(filter_snapshot(filter), filter_snapshot(&before_repeat));

            // Recount the backing bits independently of either insertion path.
            let restored = BloomFilter::deserialize(&filter.serialize()).unwrap();
            assert_eq!(filter_snapshot(&restored), filter_snapshot(filter));
            assert_eq!(restored.bits_used(), filter.bits_used());
        }
    }
}

#[test]
fn union_intersect_and_difference_preserve_set_semantics() {
    let mut left = filter();
    left.insert("shared");
    left.insert("left");

    let mut right = filter();
    right.insert("shared");
    right.insert("right");

    let left_bits = left.bits_used();
    let right_bits = right.bits_used();

    let mut intersection = left.clone();
    intersection.intersect(&right).unwrap();
    assert!(intersection.contains("shared"));
    assert_that!(intersection.bits_used(), le(left_bits));
    assert_that!(intersection.bits_used(), le(right_bits));
    assert_eq!(
        filter_snapshot(&BloomFilter::deserialize(&intersection.serialize()).unwrap()),
        filter_snapshot(&intersection)
    );

    let mut union = left.clone();
    union.union(&right).unwrap();
    assert!(union.contains("shared"));
    assert!(union.contains("left"));
    assert!(union.contains("right"));
    assert_that!(union.bits_used(), ge(left_bits));
    assert_that!(union.bits_used(), ge(right_bits));
    assert_eq!(
        filter_snapshot(&BloomFilter::deserialize(&union.serialize()).unwrap()),
        filter_snapshot(&union)
    );

    let mut diff_disjoint = left.clone();
    diff_disjoint.difference(&right).unwrap();
    assert!(diff_disjoint.contains("left"));
    assert!(!diff_disjoint.contains("shared"));
    assert!(!diff_disjoint.contains("right"));

    let mut diff_empty = left.clone();
    diff_empty.difference(&filter()).unwrap();
    assert_eq!(filter_snapshot(&diff_empty), filter_snapshot(&left));

    let mut diff_self = left.clone();
    diff_self.difference(&left).unwrap();
    assert!(diff_self.is_empty());
    assert_eq!(diff_self.bits_used(), 0);
    assert!(!diff_self.contains("shared"));
    assert!(!diff_self.contains("left"));
    assert_eq!(
        filter_snapshot(&BloomFilter::deserialize(&diff_self.serialize()).unwrap()),
        filter_snapshot(&diff_self)
    );

    let mut bulk_left = filter();
    let mut bulk_right = filter();
    for i in 0..100_u64 {
        bulk_left.insert(i);
        bulk_right.insert(i);
    }
    for i in 100..200_u64 {
        bulk_left.insert(i);
    }
    let bulk_left_bits = bulk_left.bits_used();
    bulk_left.difference(&bulk_right).unwrap();
    for i in 0..100_u64 {
        assert!(!bulk_left.contains(i));
    }
    assert_that!(bulk_left.bits_used(), le(bulk_left_bits));
    assert_eq!(
        filter_snapshot(&BloomFilter::deserialize(&bulk_left.serialize()).unwrap()),
        filter_snapshot(&bulk_left)
    );
}

#[test]
fn incompatible_set_operations_are_rejected() {
    let baseline = filter();

    let incompatible = [
        BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
            .seed(SEED + 1)
            .build()
            .unwrap(),
        BloomFilterBuilder::with_size(NUM_BITS * 2, NUM_HASHES)
            .seed(SEED)
            .build()
            .unwrap(),
        BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES + 1)
            .seed(SEED)
            .build()
            .unwrap(),
    ];

    for other in &incompatible {
        let mut union = baseline.clone();
        assert_eq!(
            union.union(other).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(filter_snapshot(&union), filter_snapshot(&baseline));

        let mut intersect = baseline.clone();
        assert_eq!(
            intersect.intersect(other).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(filter_snapshot(&intersect), filter_snapshot(&baseline));

        let mut difference = baseline.clone();
        assert_eq!(
            difference.difference(other).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(filter_snapshot(&difference), filter_snapshot(&baseline));
    }
}

#[test]
fn builder_validation_and_word_rounding() {
    let size_seeded = BloomFilterBuilder::with_size(65, 3)
        .seed(42)
        .build()
        .unwrap();
    assert_eq!(size_seeded.capacity(), 128);
    assert_eq!(size_seeded.num_hashes(), 3);
    assert_eq!(size_seeded.seed(), 42);

    let rounded = BloomFilterBuilder::with_size(65, 3).build().unwrap();
    assert_eq!(rounded.capacity(), 128);
    assert_eq!(rounded.num_hashes(), 3);
    assert_eq!(rounded.seed(), 9001);

    let trivial = BloomFilterBuilder::with_accuracy(100, 1.0).build().unwrap();
    assert_eq!(trivial.capacity(), 64);
    assert_eq!(trivial.num_hashes(), 1);
    assert_eq!(trivial.seed(), 9001);

    let acc = BloomFilterBuilder::with_accuracy(100, 0.01)
        .build()
        .unwrap();
    assert_eq!(acc.seed(), 9001);
    let acc_seeded = BloomFilterBuilder::with_accuracy(100, 0.01)
        .seed(42)
        .build()
        .unwrap();
    assert_eq!(acc_seeded.seed(), 42);
    assert_eq!(acc_seeded.capacity(), acc.capacity());
    assert_eq!(acc_seeded.num_hashes(), acc.num_hashes());
    assert_eq!(
        filter_snapshot(&acc_seeded),
        filter_snapshot(
            &BloomFilterBuilder::with_size(acc.capacity(), acc.num_hashes())
                .seed(42)
                .build()
                .unwrap()
        )
    );

    for (max_items, fpp) in [
        (0, 0.01),
        (100, 0.0),
        (100, 1.5),
        (100, f64::NAN),
        (u64::MAX, 0.01),
    ] {
        assert_eq!(
            BloomFilterBuilder::with_accuracy(max_items, fpp)
                .build()
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }

    for (num_bits, num_hashes) in [(0, 3), (u64::MAX, 3), (128, 0), (128, 32_768)] {
        assert_eq!(
            BloomFilterBuilder::with_size(num_bits, num_hashes)
                .build()
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }
}

#[test]
fn accuracy_sizing_accounts_for_integer_hash_count() {
    for fpp in [
        f64::from_bits(1),
        0.01,
        0.1,
        0.5,
        0.9,
        1.0 - f64::EPSILON,
        1.0,
    ] {
        let filter = BloomFilterBuilder::with_accuracy(10_000, fpp)
            .build()
            .unwrap();
        let k = f64::from(filter.num_hashes());
        let occupancy = -(-k * 10_000.0 / filter.capacity() as f64).exp_m1();
        let modeled_fpp = occupancy.powi(i32::from(filter.num_hashes()));
        assert!(
            modeled_fpp <= fpp * (1.0 + 1e-12),
            "{filter:?}, target {fpp}"
        );
    }
}
