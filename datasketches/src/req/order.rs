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

//! Item ordering policies for REQ sketches.

use std::cmp::Ordering;

/// Defines the ordered domain of items stored in a REQ sketch.
///
/// [`compare`](Self::compare) must define a total order over items accepted by
/// [`accepts`](Self::accepts), and both results must remain stable for the lifetime of a sketch.
/// [`ReqSketch::merge`](crate::req::ReqSketch::merge) treats equal policy values as compatible
/// when the policy also implements `PartialEq`.
///
/// # Examples
///
/// ```
/// use std::cmp::Ordering;
///
/// use datasketches::req::ReqOrder;
/// use datasketches::req::ReqSketch;
///
/// #[derive(Clone)]
/// struct Reading(i64);
///
/// #[derive(Clone, PartialEq)]
/// struct ByValue;
///
/// impl ReqOrder<Reading> for ByValue {
///     fn compare(&self, left: &Reading, right: &Reading) -> Ordering {
///         left.0.cmp(&right.0)
///     }
/// }
///
/// let mut sketch = ReqSketch::with_order(ByValue);
/// sketch.update(Reading(2));
/// sketch.update(Reading(1));
/// assert_eq!(sketch.min_item().unwrap().0, 1);
/// ```
pub trait ReqOrder<T> {
    /// Compares two accepted items.
    fn compare(&self, left: &T, right: &T) -> Ordering;

    /// Returns whether an item belongs to this ordering's domain.
    #[inline(always)]
    fn accepts(&self, _item: &T) -> bool {
        true
    }
}

/// Default REQ ordering for the built-in numeric item types.
///
/// Integers use their natural order. Floating-point values use numeric order,
/// treat signed zeros as equal, and reject NaN.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DefaultReqOrder;

impl ReqOrder<i32> for DefaultReqOrder {
    #[inline(always)]
    fn compare(&self, left: &i32, right: &i32) -> Ordering {
        left.cmp(right)
    }
}

impl ReqOrder<i64> for DefaultReqOrder {
    #[inline(always)]
    fn compare(&self, left: &i64, right: &i64) -> Ordering {
        left.cmp(right)
    }
}

impl ReqOrder<u32> for DefaultReqOrder {
    #[inline(always)]
    fn compare(&self, left: &u32, right: &u32) -> Ordering {
        left.cmp(right)
    }
}

impl ReqOrder<u64> for DefaultReqOrder {
    #[inline(always)]
    fn compare(&self, left: &u64, right: &u64) -> Ordering {
        left.cmp(right)
    }
}

impl ReqOrder<f32> for DefaultReqOrder {
    #[inline(always)]
    fn compare(&self, left: &f32, right: &f32) -> Ordering {
        left.partial_cmp(right).unwrap()
    }

    #[inline(always)]
    fn accepts(&self, item: &f32) -> bool {
        !item.is_nan()
    }
}

impl ReqOrder<f64> for DefaultReqOrder {
    #[inline(always)]
    fn compare(&self, left: &f64, right: &f64) -> Ordering {
        left.partial_cmp(right).unwrap()
    }

    #[inline(always)]
    fn accepts(&self, item: &f64) -> bool {
        !item.is_nan()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floating_point_order_is_numeric() {
        let order = DefaultReqOrder;
        assert_eq!(order.compare(&-0.0_f32, &0.0_f32), Ordering::Equal);
        assert_eq!(
            order.compare(&f64::NEG_INFINITY, &f64::INFINITY),
            Ordering::Less
        );
    }

    #[test]
    fn floating_point_order_rejects_nan() {
        let order = DefaultReqOrder;
        assert!(!<DefaultReqOrder as ReqOrder<f32>>::accepts(
            &order,
            &f32::NAN
        ));
        assert!(!<DefaultReqOrder as ReqOrder<f64>>::accepts(
            &order,
            &f64::NAN
        ));
    }
}
