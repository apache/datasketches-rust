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

use datasketches::tdigest::TDigest;
use datasketches::tdigest::TDigestMut;

pub(super) const DEFAULT_DIGEST_K: u16 = 200;
pub(super) const PARTIAL_GROUPS: usize = 512;
pub(super) const ROWS_PER_PARTIAL: usize = 64;
pub(super) const SMALL_ROWS_PER_PARTIAL: usize = 8;

pub(super) fn prepared_digest() -> TDigest {
    build_digest(&values(100_000))
}

pub(super) fn build_digest(values: &[f64]) -> TDigest {
    build_mut_digest(values).freeze()
}

pub(super) fn build_mut_digest(values: &[f64]) -> TDigestMut {
    let mut digest = TDigestMut::default();
    for &value in values {
        digest.update(value);
    }
    digest
}

pub(super) fn partial_digests(groups: usize, rows_per_group: usize) -> Vec<TDigestMut> {
    partial_digests_with(DEFAULT_DIGEST_K, groups, rows_per_group)
}

pub(super) fn partial_digests_with(
    k: u16,
    groups: usize,
    rows_per_group: usize,
) -> Vec<TDigestMut> {
    (0..groups)
        .map(|group| {
            let mut digest = TDigestMut::new(k);
            for row in 0..rows_per_group {
                digest.update(partial_value(group, row, rows_per_group));
            }
            digest
        })
        .collect()
}

pub(super) fn serialized_partial_digests(groups: usize, rows_per_group: usize) -> Vec<Vec<u8>> {
    let bytes = serialized_partial_digests_with(DEFAULT_DIGEST_K, groups, rows_per_group);
    assert!(
        bytes
            .iter()
            .all(|bytes| bytes.len() == 32 + rows_per_group * 16)
    );
    assert!(bytes.iter().all(|bytes| {
        u32::from_le_bytes(bytes[8..12].try_into().unwrap()) == rows_per_group as u32
    }));
    bytes
}

pub(super) fn serialized_partial_digests_with(
    k: u16,
    groups: usize,
    rows_per_group: usize,
) -> Vec<Vec<u8>> {
    partial_digests_with(k, groups, rows_per_group)
        .into_iter()
        .map(|mut digest| digest.serialize())
        .collect()
}

pub(super) fn serialized_state_shape(k: u16, values: &[f64]) -> (usize, u32) {
    let mut digest = TDigestMut::new(k);
    for &value in values {
        digest.update(value);
    }
    let bytes = digest.serialize();
    let centroids = match values.len() {
        0 => 0,
        1 => 1,
        _ => u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
    };
    (bytes.len(), centroids)
}

pub(super) fn partial_value(group: usize, row: usize, rows_per_group: usize) -> f64 {
    (group * rows_per_group + row) as f64
}

pub(super) fn values(len: usize) -> Vec<f64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 * (1.0 / ((1_u64 << 53) as f64))
        })
        .collect()
}
