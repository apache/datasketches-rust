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

//! Tuple sketch implementation.
//!
//! A Tuple sketch is an extension of the [Theta sketch](crate::theta): in addition to the retained
//! hash values it keeps a user-defined summary associated with every retained key. The hash table
//! mechanics (theta screening, resize, rebuild to nominal size k) mirror the Theta sketch, with the
//! added requirement that colliding keys merge their summaries.
//!
//! The behavior of a summary (how to create, update, and combine it) is supplied externally through
//! policy objects ([`SummaryUpdatePolicy`] and [`SummaryCombinePolicy`]) rather than being baked
//! into the summary type.
//!
//! # Usage
//!
//! ```
//! # use datasketches::tuple::UpdatableTupleSketch;
//! let mut sketch = UpdatableTupleSketch::<u64>::builder().build();
//! sketch.update("apple", 1);
//! assert!(sketch.estimate() >= 1.0);
//! ```

mod a_not_b;
mod hash_table;
mod intersection;
mod policy;
mod serde;
mod serialization;
mod sketch;
mod union;

pub use self::a_not_b::TupleAnotB;
pub use self::intersection::TupleIntersection;
pub use self::policy::DefaultUnionPolicy;
pub use self::policy::DefaultUpdatePolicy;
pub use self::policy::SummaryCombinePolicy;
pub use self::policy::SummaryUpdatePolicy;
pub use self::serde::PrimitiveSummarySerde;
pub use self::serde::SummarySerde;
pub use self::sketch::CompactTupleSketch;
pub use self::sketch::TupleSketchView;
pub use self::sketch::UpdatableTupleSketch;
pub use self::sketch::UpdatableTupleSketchBuilder;
pub use self::union::TupleUnion;
pub use self::union::TupleUnionBuilder;
