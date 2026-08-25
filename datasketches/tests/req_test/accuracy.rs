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

//! End-to-end accuracy checks for ReqSketch.

use datasketches::error::Error;
use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;
use googletest::assert_that;
use googletest::prelude::le;

#[test]
fn rank_space_error_is_bounded() -> Result<(), Error> {
    let mut sketch = ReqSketch::new();
    let n = 50_000;

    for i in 0..n {
        sketch.update(i as f64);
    }

    assert_eq!(sketch.n(), n as u64);

    for rank in [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99] {
        let quantile = sketch.quantile(rank, SearchCriteria::Inclusive)?;
        let estimated_rank = sketch.rank(&quantile, SearchCriteria::Inclusive)?;
        let abs_rank_error = (estimated_rank - rank).abs();
        let max_abs_rank_error = if rank >= 0.9 { 0.01 } else { 0.02 };

        assert_that!(abs_rank_error, le(max_abs_rank_error), "rank: {rank}");
    }

    assert!(!sketch.is_empty());
    Ok(())
}
