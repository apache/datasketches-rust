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

//! Tests for the user-managed SortedView API (ported from reqsketch-rs #25):
//! distribution queries take `&self`, and `sorted_view()` returns an owned
//! snapshot instead of relying on an internal cache.

use datasketches::error::ErrorKind;
use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;
use datasketches::req::SortedView;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::anything;
use googletest::prelude::contains_substring;
use googletest::prelude::err;
use googletest::prelude::ge;
use googletest::prelude::lt;
use googletest::prelude::near;

fn populated_sketch(n: u64) -> ReqSketch<f64> {
    let mut sketch = ReqSketch::default();
    for i in 0..n {
        sketch.update(i as f64);
    }
    sketch
}

/// All distribution queries must work through a shared (`&self`) reference.
fn query_through_shared_ref(sketch: &ReqSketch<f64>) {
    sketch
        .quantile(0.5, SearchCriteria::Inclusive)
        .expect("quantile");
    sketch
        .quantiles(&[0.25, 0.5, 0.75], SearchCriteria::Inclusive)
        .expect("quantiles");
    sketch.rank(&50.0, SearchCriteria::Inclusive).expect("rank");
    sketch
        .pmf(&[10.0, 50.0], SearchCriteria::Inclusive)
        .expect("pmf");
    sketch
        .cdf(&[10.0, 50.0], SearchCriteria::Inclusive)
        .expect("cdf");
    assert!(!sketch.sorted_view().is_empty());
}

#[test]
fn queries_work_through_shared_reference() {
    let sketch = populated_sketch(100);
    query_through_shared_ref(&sketch);
}

#[test]
fn sorted_view_is_an_owned_snapshot() {
    let mut sketch = populated_sketch(100);

    let view: SortedView<f64> = sketch.sorted_view();
    assert_eq!(view.total_weight(), 100);

    // Updating the sketch while the view is alive must compile (owned view)
    // and must not affect the snapshot.
    for i in 100..200 {
        sketch.update(i as f64);
    }
    assert_eq!(view.total_weight(), 100);

    // A fresh view reflects the new state.
    let fresh = sketch.sorted_view();
    assert_eq!(fresh.total_weight(), 200);
}

#[test]
fn sorted_view_on_empty_sketch_is_an_empty_view() {
    let sketch: ReqSketch<f64> = ReqSketch::default();
    let view = sketch.sorted_view();
    assert!(view.is_empty());
    assert_eq!(view.len(), 0);
    assert_eq!(view.total_weight(), 0);
    // Queries on the empty view still report an error.
    assert_that!(
        view.quantile(0.5, SearchCriteria::Inclusive),
        err(anything())
    );
}

#[test]
fn empty_sketch_pmf_cdf_report_error() {
    let sketch: ReqSketch<f64> = ReqSketch::default();
    assert_that!(
        sketch.pmf(&[1.0], SearchCriteria::Inclusive),
        err(anything())
    );
    assert_that!(
        sketch.cdf(&[1.0], SearchCriteria::Inclusive),
        err(anything())
    );
}

#[test]
fn view_rank_is_primary_query_name() {
    let sketch = populated_sketch(10);
    let view = sketch.sorted_view();
    let r = view.rank(&5.0, SearchCriteria::Inclusive).expect("rank");
    assert_that!(r, near(0.6, 1e-10));
}

#[test]
fn nan_query_items_are_rejected() {
    let sketch = populated_sketch(100);
    let error = sketch
        .rank(&f64::NAN, SearchCriteria::Inclusive)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let view = sketch.sorted_view();
    assert_that!(
        view.rank(&f64::NAN, SearchCriteria::Inclusive),
        err(anything())
    );
}

#[test]
fn error_precedence_empty_before_invalid_rank() {
    // On an empty sketch the emptiness is reported before the out-of-range rank.
    let empty: ReqSketch<f64> = ReqSketch::default();
    let empty_err = empty.quantile(2.0, SearchCriteria::Inclusive).unwrap_err();
    assert_that!(empty_err.message(), contains_substring("empty"));

    // On a populated sketch the out-of-range rank is reported.
    let sketch = populated_sketch(10);
    let range_err = sketch.quantile(2.0, SearchCriteria::Inclusive).unwrap_err();
    assert_eq!(range_err.kind(), ErrorKind::InvalidArgument);
    assert_that!(range_err.message(), contains_substring("must be in"));
}

#[test]
fn view_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SortedView<f64>>();
    assert_send_sync::<ReqSketch<f64>>();
}

#[test]
fn concurrent_readers_share_the_sketch() {
    let sketch = std::sync::Arc::new(populated_sketch(1_000));
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let sketch = std::sync::Arc::clone(&sketch);
            std::thread::spawn(move || {
                let rank = 0.2 * (i + 1) as f64;
                sketch
                    .quantile(rank, SearchCriteria::Inclusive)
                    .expect("quantile from shared sketch")
            })
        })
        .collect();
    for handle in handles {
        let q = handle.join().expect("thread");
        assert_that!(q, all!(ge(0.0), lt(1_000.0)));
    }
}
