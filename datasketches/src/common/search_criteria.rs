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

/// Selects the rank definition used by rank, quantile, PMF, and CDF queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchCriteria {
    /// Define rank as the fraction of values less than or equal to the boundary.
    Inclusive,
    /// Define rank as the fraction of values strictly less than the boundary.
    Exclusive,
}
