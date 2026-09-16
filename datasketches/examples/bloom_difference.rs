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

//! A CDN edge-cache purge scenario for [`BloomFilter::difference`].
//!
//! An edge node caches objects and records them in a filter. The origin publishes a purge
//! list (takedowns, stale content) as another filter with the same configuration. The edge
//! node folds the purge list into its cache picture once, producing a single filter that
//! represents "everything I still serve", which it ships to sibling nodes.
//!
//! Two properties make the difference a good fit here:
//!
//! * Purged objects are excluded *exactly*: a taken-down object is never served again. That is the
//!   compliance-critical direction.
//! * The cost falls on the other direction: a still-valid object is dropped from the filter when
//!   one of its hash positions collides with the purge filter. Dropped objects are treated as cache
//!   misses and refetched from the origin — extra origin load, never a stale serve.
//!
//! The drop rate falls as the purge filter gets sparser relative to the shared filter shape,
//! so difference works best when the subtracted set is much smaller than the shape the
//! filters were sized for. A purge list is naturally tiny next to a whole cache.

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

    // Fold the purge list into the cache picture once. The result is one artifact that can
    // be stored or shipped, instead of keeping both filters and querying them per lookup.
    cached.difference(&purged).unwrap();

    // The guarantee that matters for takedowns: a purged object is never served again.
    for object in purged_objects() {
        assert!(!cached.contains(&object));
    }

    // Count how many still-valid objects survived. Dropped ones cost an origin refetch.
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
