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
//! A Tuple sketch is an extension of the Theta sketch: in addition to the retained
//! hash values it keeps a user-defined summary associated with every retained key. The hash table
//! mechanics (theta screening, resize, rebuild to nominal size k) mirror the Theta sketch, with the
//! added requirement that colliding keys merge their summaries.
//!
//! Custom summary behavior is supplied externally through policy objects: [`SummaryPolicy`]
//! creates summaries, while [`SummaryUpdatePolicy`] folds update values into them. Summaries that
//! implement `Default` and `AddAssign` can use [`DefaultUpdatePolicy`]. Set operations combine the
//! summaries of shared keys through [`SummaryCombinePolicy`]; the union defaults to
//! [`DefaultUnionPolicy`].
//!
//! # Usage
//!
//! ```
//! # use datasketches::tuple::{DefaultUpdatePolicy, TupleSketchBuilder};
//! let policy = DefaultUpdatePolicy::<u64>::default();
//! let mut sketch = TupleSketchBuilder::new(policy).build();
//! sketch.update("apple", 1_u64);
//! assert!(sketch.estimate() >= 1.0);
//! ```

mod hash_table;
mod intersection;
mod policy;
mod serialization;
mod sketch;
mod union;

pub use self::hash_table::TupleEntry;
pub use self::intersection::TupleIntersection;
pub use self::policy::DefaultUnionPolicy;
pub use self::policy::DefaultUpdatePolicy;
pub use self::policy::SummaryCombinePolicy;
pub use self::policy::SummaryPolicy;
pub use self::policy::SummaryUpdatePolicy;
pub use self::serialization::TupleSummaryValue;
pub use self::sketch::CompactTupleSketch;
pub use self::sketch::TupleSketch;
pub use self::sketch::TupleSketchBuilder;
pub use self::sketch::TupleSketchView;
pub use self::union::TupleUnion;
pub use self::union::TupleUnionBuilder;
