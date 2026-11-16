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

use datasketches::common::SearchCriteria;
use datasketches::error::ErrorKind;
use datasketches::req::ReqSketch;
use datasketches::req::SortedView;

use super::ReqF64;
use super::req_f64;

fn populated_sketch(n: i64) -> ReqSketch<ReqF64> {
    let mut sketch = ReqSketch::default();
    for i in 0..n {
        sketch.update(req_f64(i as f64));
    }
    sketch
}

#[test]
fn sorted_view_is_an_owned_snapshot() {
    let mut sketch = populated_sketch(100);

    let view: SortedView<ReqF64> = sketch.sorted_view();
    assert_eq!(view.total_weight(), 100);

    // Updating the sketch while the view is alive must compile (owned view)
    // and must not affect the snapshot.
    for i in 100..200 {
        sketch.update(req_f64(i as f64));
    }
    assert_eq!(view.total_weight(), 100);

    // A fresh view reflects the new state.
    let fresh = sketch.sorted_view();
    assert_eq!(fresh.total_weight(), 200);
}

#[test]
fn sorted_view_on_empty_sketch_is_an_empty_view() {
    let sketch: ReqSketch<ReqF64> = ReqSketch::default();
    let view = sketch.sorted_view();
    assert!(view.is_empty());
    assert_eq!(view.len(), 0);
    assert_eq!(view.total_weight(), 0);
    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.rank(&req_f64(0.0), criteria), None);
        assert_eq!(view.rank(&req_f64(0.0), criteria), None);
        for rank in [0.0, 0.5, 1.0] {
            assert_eq!(sketch.quantile(rank, criteria).unwrap(), None);
            assert_eq!(view.quantile(rank, criteria).unwrap(), None);
        }
        assert_eq!(sketch.quantiles(&[0.0, 0.5, 1.0], criteria).unwrap(), None);
        assert_eq!(sketch.pmf(&[req_f64(0.0)], criteria).unwrap(), None);
        assert_eq!(sketch.cdf(&[req_f64(0.0)], criteria).unwrap(), None);
        assert_eq!(view.pmf(&[req_f64(0.0)], criteria).unwrap(), None);
        assert_eq!(view.cdf(&[req_f64(0.0)], criteria).unwrap(), None);
    }
}

#[test]
fn empty_argument_slices_preserve_the_sketch_empty_state() {
    for n in [0, 1] {
        let sketch = populated_sketch(n);
        let view = sketch.sorted_view();
        let quantiles = (n != 0).then(Vec::new);
        let distribution = (n != 0).then(|| vec![1.0]);
        for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
            assert_eq!(sketch.quantiles(&[], criteria).unwrap(), quantiles);
            assert_eq!(sketch.cdf(&[], criteria).unwrap(), distribution);
            assert_eq!(sketch.pmf(&[], criteria).unwrap(), distribution);
            assert_eq!(view.cdf(&[], criteria).unwrap(), distribution);
            assert_eq!(view.pmf(&[], criteria).unwrap(), distribution);
        }
    }
}

#[test]
fn invalid_arguments_are_rejected_regardless_of_sketch_state() {
    for n in [0, 1, 1_000] {
        let sketch = populated_sketch(n);
        let view = sketch.sorted_view();
        for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
            for rank in [f64::NAN, f64::NEG_INFINITY, -0.1, 1.1, f64::INFINITY] {
                for error in [
                    sketch.quantile(rank, criteria).unwrap_err(),
                    view.quantile(rank, criteria).unwrap_err(),
                    sketch.quantiles(&[0.5, rank], criteria).unwrap_err(),
                ] {
                    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
                }
            }
            for splits in [[1.0, 1.0], [2.0, 1.0]] {
                let splits = splits.map(req_f64);
                for error in [
                    sketch.cdf(&splits, criteria).unwrap_err(),
                    sketch.pmf(&splits, criteria).unwrap_err(),
                    view.cdf(&splits, criteria).unwrap_err(),
                    view.pmf(&splits, criteria).unwrap_err(),
                ] {
                    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
                }
            }
        }
    }
}

#[test]
fn view_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SortedView<ReqF64>>();
    assert_send_sync::<ReqSketch<ReqF64>>();
}
