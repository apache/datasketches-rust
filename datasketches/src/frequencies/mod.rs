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

//! Frequency sketches for finding heavy hitters in data streams.
//!
//! # Overview
//!
//! This sketch is based on the paper ["A High-Performance Algorithm for Identifying Frequent Items
//! in Data Streams"](https://arxiv.org/abs/1705.07001) by Daniel Anderson, Pryce Bevan, Kevin Lang,
//! Edo Liberty, Lee Rhodes, and Justin Thaler.
//!
//! This sketch is useful for tracking approximate frequencies of items of type `T` that implements
//! [`FrequentItemValue`], with optional associated counts (`T` item, `u64` count) that are members
//! of a multiset of such items. The true frequency of an item is defined to be the sum of
//! associated counts.
//!
//! This implementation provides the following capabilities:
//! * Estimate the frequency of an item.
//! * Return upper and lower bounds of any item, such that the true frequency is always between the
//!   upper and lower bounds.
//! * Return a global maximum error that holds for all items in the stream.
//! * Return an array of frequent items that qualify either [`ErrorType::NoFalsePositives`] or
//!   [`ErrorType::NoFalseNegatives`].
//! * Merge itself with another sketch created from this module.
//! * Serialize to bytes, or deserialize from bytes, for storage or transmission.
//!
//! # Accuracy
//!
//! If fewer than `0.75 * max_map_size` different items are inserted into the sketch the estimated
//! frequencies returned by the sketch will be exact.
//!
//! The logic of the frequent items sketch is such that the stored counts and true counts are never
//! too different. More specifically, for any item, the sketch can return an estimate of the true
//! frequency of item, along with upper and lower bounds on the frequency (that hold
//! deterministically).
//!
//! For this implementation and for a specific active item, it is guaranteed that the true frequency
//! will be between the Upper Bound (UB) and the Lower Bound (LB) computed for that item.
//! Specifically, `(UB - LB) ≤ W * epsilon`, where `W` denotes the sum of all item counts, and
//! `epsilon = 3.5/M`, where `M` is the `max_map_size`.
//!
//! This is the worst case guarantee that applies to arbitrary inputs. [^1]
//! For inputs typically seen in practice (`UB - LB`) is usually much smaller.
//!
//! [^1]: For speed we do employ some randomization that introduces a small probability that our
//! proof of the worst-case bound might not apply to a given run. However, we have ensured that this
//! probability is extremely small. For example, if the stream causes one table purge (rebuild),
//! our proof of the worst case bound applies with probability at least `1 - 1E-14`. If the stream
//! causes `1E9` purges, our proof applies with probability at least `1 - 1E-5`.
//!
//! # Background
//!
//! This code implements a variant of what is commonly known as the "Misra-Gries algorithm".
//! Variants of it were discovered and rediscovered and redesigned several times over the years:
//! * "Finding repeated elements", Misra, Gries, 1982
//! * "Frequency estimation of Internet packet streams with limited space" Demaine, Lopez-Ortiz,
//!   Munro, 2002
//! * "A simple algorithm for finding frequent elements in streams and bags" Karp, Shenker,
//!   Papadimitriou, 2003
//! * "Efficient Computation of Frequent and Top-k Elements in Data Streams" Metwally, Agrawal,
//!   Abbadi, 2006
//!
//! # Examples
//!
//! ```
//! # use datasketches::frequencies::ErrorType;
//! # use datasketches::frequencies::FrequentItemsSketch;
//! let mut sketch = FrequentItemsSketch::<i64>::new(64);
//! sketch.update_with_count(1, 3);
//! sketch.update(2);
//! let rows = sketch.frequent_items(ErrorType::NoFalseNegatives);
//! assert!(rows.iter().any(|row| *row.item() == 1));
//! ```
//!
//! # Serialization
//!
//! ```
//! # use datasketches::frequencies::FrequentItemsSketch;
//! let mut sketch = FrequentItemsSketch::<i64>::new(64);
//! sketch.update_with_count(42, 2);
//!
//! let bytes = sketch.serialize();
//! let decoded = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
//! assert!(decoded.estimate(&42) >= 2);
//! ```

mod reverse_purge_item_hash_map;
mod serialization;
mod sketch;

pub use self::serialization::FrequentItemValue;
pub use self::sketch::ErrorType;
pub use self::sketch::FrequentItemsSketch;
pub use self::sketch::Row;
