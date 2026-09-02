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

use std::cmp::Ordering;

/// Defines the ordering used by a KLL sketch.
///
/// Accepted values must form a total order. Sketches can be merged only when their comparators are
/// compatible: they must accept the same values and order every pair of accepted values
/// identically.
pub trait KllComparator<T>: Clone {
    /// Compares two accepted values.
    fn compare(&self, left: &T, right: &T) -> Ordering;

    /// Returns whether `item` belongs to this comparator's ordered domain.
    ///
    /// Updates with rejected values are ignored. The default accepts every value.
    fn accepts(&self, _item: &T) -> bool {
        true
    }

    /// Returns whether `other` defines the same ordered domain and comparison semantics.
    fn is_compatible(&self, other: &Self) -> bool;
}

/// Uses the value's natural partial ordering and rejects unordered values such as NaN.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NaturalOrder;

impl<T: PartialOrd> KllComparator<T> for NaturalOrder {
    fn compare(&self, left: &T, right: &T) -> Ordering {
        left.partial_cmp(right)
            .expect("accepted KLL values must be totally ordered")
    }

    fn accepts(&self, item: &T) -> bool {
        item.partial_cmp(item).is_some()
    }

    fn is_compatible(&self, _other: &Self) -> bool {
        true
    }
}
