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

const MIN_BUCKET_SORT_LEN: usize = 256;
const MAX_BUCKET_LEN: u32 = 32;

/// Sorts uniform 64-bit MurmurHash3 entries in ascending hash order using a single top-bit bucket
/// pass followed by a linear insertion pass, falling back to `sort_unstable_by_key` for small or
/// skewed inputs.
pub fn with_bucket_starts<T>(
    entries: &mut Vec<T>,
    hash_of: impl Fn(&T) -> u64,
    scatter: impl FnOnce(&mut Vec<T>, &mut [u32], u32, usize),
) {
    let len = entries.len();
    if len < MIN_BUCKET_SORT_LEN {
        if len > 1 {
            entries.sort_unstable_by_key(&hash_of);
        }
        return;
    }

    // The highest set bit of the bitwise OR equals the highest set bit of the maximum hash,
    // avoiding a branch-dependent max reduction before computing the top-bit bucket shift.
    let bits = entries.iter().fold(0u64, |acc, entry| acc | hash_of(entry));
    let lg_buckets = len.ilog2();
    let num_buckets = 1usize << lg_buckets;
    let mask = num_buckets - 1;
    let shift = (u64::BITS - bits.leading_zeros()).saturating_sub(lg_buckets);

    let mut heap_starts;
    let mut stack_10;
    let mut stack_12;
    let starts: &mut [u32] = if lg_buckets <= 10 {
        stack_10 = [0u32; (1 << 10) + 1];
        &mut stack_10[..=num_buckets]
    } else if lg_buckets <= 12 {
        stack_12 = [0u32; (1 << 12) + 1];
        &mut stack_12[..=num_buckets]
    } else {
        heap_starts = vec![0u32; num_buckets + 1];
        &mut heap_starts
    };

    for entry in entries.iter() {
        starts[((hash_of(entry) >> shift) as usize & mask) + 1] += 1;
    }
    let mut largest = 0u32;
    for i in 1..starts.len() {
        largest = largest.max(starts[i]);
        starts[i] += starts[i - 1];
    }
    if largest > MAX_BUCKET_LEN {
        entries.sort_unstable_by_key(&hash_of);
        return;
    }

    scatter(entries, starts, shift, mask);
}

pub fn sort_by_hash_key<T: Copy>(entries: &mut Vec<T>, hash_of: impl Fn(&T) -> u64) {
    with_bucket_starts(entries, &hash_of, |entries, starts, shift, mask| {
        let len = entries.len();
        let mut sorted = vec![entries[0]; len];
        for &entry in entries.iter() {
            let next = &mut starts[(hash_of(&entry) >> shift) as usize & mask];
            sorted[*next as usize] = entry;
            *next += 1;
        }
        for i in 1..len {
            let entry = sorted[i];
            let hash = hash_of(&entry);
            let mut j = i;
            while j > 0 && hash_of(&sorted[j - 1]) > hash {
                sorted[j] = sorted[j - 1];
                j -= 1;
            }
            sorted[j] = entry;
        }
        *entries = sorted;
    });
}
