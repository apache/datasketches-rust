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

use crate::error::Error;
use crate::tuple::SummaryCombinePolicy;
use crate::tuple::SummaryPolicy;
use crate::tuple::SummaryUpdatePolicy;

/// Combines the value arrays of keys shared by two ArrayOfDoubles sketches.
pub trait ArrayOfDoublesCombiner {
    /// Combines `incoming` into `existing` in place.
    fn combine(&self, existing: &mut [f64], incoming: &[f64]);
}

#[derive(Debug, Clone)]
pub struct ArrayOfDoublesPolicy {
    pub num_values: u8,
}

impl SummaryPolicy for ArrayOfDoublesPolicy {
    type Summary = Vec<f64>;

    fn create(&self) -> Self::Summary {
        vec![0.0; usize::from(self.num_values)]
    }
}

impl SummaryUpdatePolicy<&[f64]> for ArrayOfDoublesPolicy {
    fn update(&self, summary: &mut Self::Summary, value: &[f64]) {
        debug_assert_eq!(summary.len(), value.len());
        for (summary, value) in summary.iter_mut().zip(value) {
            *summary += value;
        }
    }
}

impl SummaryCombinePolicy for ArrayOfDoublesPolicy {
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
        debug_assert_eq!(summary.len(), other.len());
        for (summary, other) in summary.iter_mut().zip(other) {
            *summary += other;
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArrayOfDoublesIntersectionPolicy<C> {
    pub num_values: u8,
    pub combiner: C,
}

impl<C> SummaryPolicy for ArrayOfDoublesIntersectionPolicy<C>
where
    C: ArrayOfDoublesCombiner,
{
    type Summary = Vec<f64>;

    fn create(&self) -> Self::Summary {
        vec![0.0; usize::from(self.num_values)]
    }
}

impl<C> SummaryCombinePolicy for ArrayOfDoublesIntersectionPolicy<C>
where
    C: ArrayOfDoublesCombiner,
{
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
        debug_assert_eq!(summary.len(), other.len());
        self.combiner.combine(summary, other);
    }
}

pub fn validate_num_values(num_values: u8) -> Result<(), Error> {
    if num_values == 0 {
        return Err(Error::invalid_argument(
            "num_values must be greater than 0, got 0",
        ));
    }
    Ok(())
}

pub fn check_num_values(expected: u8, actual: usize, operation: &str) -> Result<(), Error> {
    if actual != usize::from(expected) {
        return Err(Error::invalid_argument(format!(
            "num_values mismatch for {operation}: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}
