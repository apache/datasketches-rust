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

//! An edge node folds the origin's purge list into its cache picture with
//! `BloomFilter::difference`, producing one "everything it still serves" filter to ship to
//! sibling nodes. Purged objects are excluded exactly — a takedown is never served again —
//! while a few still-valid objects drop out of the filter and become origin refetches.

use datasketches::bloom::BloomFilter;
use datasketches::bloom::BloomFilterBuilder;

/// Returns the shared filter configuration published by the origin.
///
/// Both sides of a difference must use identical capacity, hash count, and seed, so the
/// parameters are distributed rather than chosen independently by each node.
fn published_config() -> BloomFilterBuilder {
    BloomFilterBuilder::with_accuracy(CACHED_OBJECTS, 0.01)
}

/// Number of objects cached by the edge node.
const CACHED_OBJECTS: u64 = 100_000;

/// Object keys purged in this epoch: every 200th cached object, 500 in total.
fn purged_objects() -> impl Iterator<Item = u64> {
    (0..CACHED_OBJECTS).step_by(200)
}

fn main() {
    // The edge node records every object it caches.
    let mut cached = published_config().build().unwrap();
    for object in 0..CACHED_OBJECTS {
        cached.insert(object);
    }

    // The origin records every purged object using the same published configuration.
    let mut purged = published_config().build().unwrap();
    for object in purged_objects() {
        purged.insert(object);
    }

    // Fold the purge list into the cache picture once, producing one artifact to store or
    // ship instead of keeping both filters and querying both per lookup. A cached object
    // survives only while none of its hash positions is occupied in the purge filter, so
    // difference suits a subtracted set that is sparse in the shared shape — a purge list
    // is naturally tiny next to a whole cache.
    cached.difference(&purged).unwrap();

    // The guarantee that matters for takedowns: a purged object is never served again.
    for object in purged_objects() {
        assert!(!cached.contains(&object));
    }

    // Count how many still-valid objects survived; dropped ones cost an origin refetch,
    // never a stale serve.
    let valid = CACHED_OBJECTS - purged_objects().count() as u64;
    let mut retained = 0_u64;
    for object in (0..CACHED_OBJECTS).filter(|object| object % 200 != 0) {
        if cached.contains(&object) {
            retained += 1;
        }
    }
    println!("Cached objects:  {CACHED_OBJECTS}");
    println!("Purged objects:  {}", purged_objects().count());
    println!(
        "Still served:    {retained} / {valid} valid objects ({:.1}% retained; drops become cache misses)",
        retained as f64 / valid as f64 * 100.0
    );
    assert!(retained as f64 >= valid as f64 * 0.9);

    // Ship the resulting filter to a sibling node; the purge filter is discarded.
    let bytes = cached.serialize();
    let shipped = BloomFilter::deserialize(&bytes).unwrap();
    assert_eq!(shipped, cached);
    println!("Shipped filter:  {} bytes", bytes.len());
}
