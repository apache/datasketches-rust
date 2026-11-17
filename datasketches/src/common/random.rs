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

//! A thread-local source of uniform random bits.
//!
//! The generator is SplitMix64, the fixed-increment form of the splittable
//! generator introduced in <https://doi.org/10.1145/2714064.2660195> and shipped
//! in the JDK as `java.util.SplittableRandom`. The constants and shift amounts
//! used here match Sebastiano Vigna's public-domain reference
//! implementation, <https://prng.di.unimi.it/splitmix64.c>.

use std::cell::Cell;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

/// Advances a SplitMix64 state by one step, returning `(next_state, output)`.
///
/// The state advances by the fixed odd increment `0x9E37_79B9_7F4A_7C15`, the
/// 64-bit approximation of 2^64/φ, which makes the state a Weyl sequence of
/// full period 2^64. The output is that new state run through Stafford's
/// variant-13 mixer, a bijection, so the outputs inherit both the period and
/// the equidistribution of the state.
///
/// The step is bit-for-bit identical to `next()` in the reference
/// `splitmix64.c`, so a given seed yields the same stream as that
/// implementation.
fn next_u64(state: u64) -> (u64, u64) {
    let state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (state, z ^ (z >> 31))
}

/// Returns the seed for the calling thread's stream.
///
/// The seed is derived from `RandomState`'s randomized hash keys and the current
/// thread's id so that threads in one process start at unrelated points of the
/// cycle.
fn random_seed() -> u64 {
    RandomState::new().hash_one(std::thread::current().id())
}

thread_local! {
    /// The calling thread's SplitMix64 state, seeded on first use.
    static STATE: Cell<u64> = Cell::new(random_seed());
}

/// Returns a uniformly distributed `u64` from the calling thread's stream.
///
/// The call is a handful of arithmetic operations on thread-local state: it
/// never allocates, locks, or blocks. The first call on a thread seeds that
/// thread's state.
#[inline]
pub fn random_u64() -> u64 {
    STATE.with(|state| {
        let (next, value) = next_u64(state.get());
        state.set(next);
        value
    })
}

/// Returns `true` or `false`, each with probability 1/2.
///
/// The bit is the low bit of the next output of the calling thread's stream.
/// The call is a handful of arithmetic operations on thread-local state: it
/// never allocates, locks, or blocks. The first call on a thread seeds that
/// thread's state.
#[cfg(any(test, feature = "density", feature = "kll", feature = "req"))]
#[inline]
pub fn random_bit() -> bool {
    random_u64() & 1 == 1
}

/// Returns a uniformly distributed `usize` in `0..bound`.
///
/// Uses Lemire's nearly divisionless 128-bit multiplication rejection method to avoid modulo bias.
///
/// # Panics
///
/// Panics if `bound == 0` or `bound` cannot be represented as `u64`.
#[cfg(any(test, feature = "density"))]
#[inline]
pub fn random_below(bound: usize) -> usize {
    assert!(bound > 0, "bound must be positive");
    if bound == 1 {
        return 0;
    }
    let range = u64::try_from(bound).expect("bound exceeds u64::MAX");
    rejection_sample(range, random_u64) as usize
}

#[cfg(any(test, feature = "density"))]
#[inline]
fn rejection_sample(range: u64, mut next: impl FnMut() -> u64) -> u64 {
    let mut product = u128::from(next()) * u128::from(range);
    let mut low = product as u64;
    if low < range {
        let threshold = range.wrapping_neg() % range;
        while low < threshold {
            product = u128::from(next()) * u128::from(range);
            low = product as u64;
        }
    }
    (product >> 64) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_split_mix64_matches_the_reference_output() {
        // Expected outputs for seed 0, taken from the reference splitmix64.c.
        let expected = [
            0xe220_a839_7b1d_cdaf,
            0x6e78_9e6a_a1b9_65f4,
            0x06c4_5d18_8009_454f,
            0xf88b_b8a8_724c_81ec,
        ];
        let mut state = 0;
        for want in expected {
            let (next, got) = next_u64(state);
            state = next;
            assert_eq!(got, want);
        }
    }

    /// A coin that stops moving is the failure this module is most exposed to:
    /// dropping the write-back in `random_bit` pins every flip to one value, and
    /// the sketches keep compacting and keep passing their own tests while
    /// always promoting the same half.
    #[test]
    fn assert_random_bit_yields_both_values() {
        let mut seen = [false; 2];
        for _ in 0..64 {
            seen[usize::from(random_bit())] = true;
        }
        assert_eq!(seen, [true, true], "the stream stopped advancing");
    }

    /// The KLL and REQ error bounds hold only for a fair coin, so pin the
    /// balance. One million draws have a standard deviation of 500 heads; the
    /// bound below is six of them, which a correct generator exceeds about twice
    /// in a billion runs.
    #[test]
    fn assert_random_bit_is_unbiased() {
        const DRAWS: u32 = 1_000_000;
        const TOLERANCE: u32 = 3_000;

        let mut heads = 0;
        for _ in 0..DRAWS {
            heads += u32::from(random_bit());
        }
        assert!(heads.abs_diff(DRAWS / 2) <= TOLERANCE, "heads = {heads}");
    }

    /// Threads must not share a starting point, otherwise sketches filled on
    /// different threads make identical compaction choices and their errors
    /// correlate. Two independent streams agree on 64 consecutive bits with
    /// probability 2^-64.
    #[test]
    fn assert_each_thread_draws_its_own_stream() {
        fn draw_word() -> u64 {
            (0..64).fold(0, |word, _| (word << 1) | u64::from(random_bit()))
        }

        let other = std::thread::spawn(draw_word)
            .join()
            .expect("the drawing thread should not panic");
        assert_ne!(draw_word(), other, "two threads shared one stream");
    }

    #[test]
    fn assert_random_u64_advances_state() {
        let first = random_u64();
        let mut saw_distinct = false;
        for _ in 0..8 {
            if random_u64() != first {
                saw_distinct = true;
                break;
            }
        }
        assert!(saw_distinct, "random_u64 stream stopped advancing");
    }

    #[test]
    #[should_panic(expected = "bound must be positive")]
    fn assert_random_below_zero_panics() {
        let _ = random_below(0);
    }

    #[test]
    fn assert_random_below_one_returns_zero() {
        for _ in 0..16 {
            assert_eq!(random_below(1), 0);
        }
    }

    #[test]
    fn assert_random_below_respects_bounds() {
        for bound in [2, 3, 5, 7, 17, 64, 100] {
            for _ in 0..256 {
                let value = random_below(bound);
                assert!(value < bound, "value {value} >= bound {bound}");
            }
        }

        let mut seen = [false; 7];
        for _ in 0..512 {
            seen[random_below(7)] = true;
        }
        assert!(seen.into_iter().all(|flag| flag));
    }

    #[test]
    fn assert_random_below_rejects_biased_low_product() {
        // For range = 3, 2^64 mod 3 = 1, so low == 0 must be rejected.
        // First draw: 0 -> product = 0, low = 0 < 1 (rejected).
        // Second draw: 2^63 -> product = 3 * 2^63 = 1 * 2^64 + 2^63, low = 2^63 >= 1 (accepted,
        // returns 1).
        let mut draws = [0_u64, 1_u64 << 63].into_iter();
        let sampled = rejection_sample(3, || draws.next().expect("enough draws"));
        assert_eq!(sampled, 1);
        assert!(draws.next().is_none());
    }

    #[test]
    fn assert_random_below_is_unbiased() {
        const DRAWS: usize = 300_000;
        const TOLERANCE: usize = 1_600;

        let mut counts = [0_usize; 3];
        for _ in 0..DRAWS {
            counts[random_below(3)] += 1;
        }
        let expected = DRAWS / 3;
        for (bucket, count) in counts.into_iter().enumerate() {
            assert!(
                count.abs_diff(expected) <= TOLERANCE,
                "bucket {bucket} count {count} deviated from {expected}"
            );
        }
    }
}
