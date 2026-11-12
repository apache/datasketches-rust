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

use std::fmt::Debug;
use std::ops::AddAssign;

use datasketches::bloom::BloomFilterBuilder;
use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::countmin::CountMinSketch;
use datasketches::frequencies::FrequentItemsSketch;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;
use datasketches::kll::KllSketch;
use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use datasketches::tdigest::TDigestMut;
use datasketches::theta::ThetaIntersection;
use datasketches::theta::ThetaSketchBuilder;
use datasketches::theta::ThetaUnionBuilder;
use datasketches::tuple::DefaultUnionPolicy;
use datasketches::tuple::DefaultUpdatePolicy;
use datasketches::tuple::SummaryCombinePolicy;
use datasketches::tuple::SummaryPolicy;
use datasketches::tuple::TupleIntersection;
use datasketches::tuple::TupleSketchBuilder;
use datasketches::tuple::TupleUnionBuilder;
use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;

fn assert_concise_debug<T: Debug>(value: &T) {
    let formatted = format!("{value:?}");
    assert!(
        formatted.len() < 256,
        "expected concise diagnostic Debug output (< 256 bytes), got {} bytes: {formatted}",
        formatted.len()
    );
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
struct NonDebugItem(u64);

impl AddAssign<u64> for NonDebugItem {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl AddAssign<&Self> for NonDebugItem {
    fn add_assign(&mut self, rhs: &Self) {
        self.0 += rhs.0;
    }
}

#[derive(Clone, Default)]
struct NonCopyNonDebugSummary(Vec<u64>);

impl AddAssign<u64> for NonCopyNonDebugSummary {
    fn add_assign(&mut self, rhs: u64) {
        self.0.push(rhs);
    }
}

impl AddAssign<&Self> for NonCopyNonDebugSummary {
    fn add_assign(&mut self, rhs: &Self) {
        self.0.extend_from_slice(&rhs.0);
    }
}

#[derive(Clone, Copy)]
struct NonDebugCombinePolicy;

impl SummaryPolicy for NonDebugCombinePolicy {
    type Summary = NonDebugItem;

    fn create(&self) -> Self::Summary {
        NonDebugItem(0)
    }
}

impl SummaryCombinePolicy for NonDebugCombinePolicy {
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
        summary.0 += other.0;
    }
}

#[test]
fn populated_types_format_concise_diagnostic_summaries() {
    let mut bloom = BloomFilterBuilder::with_size(8192, 4).build().unwrap();
    for i in 0..500_u64 {
        bloom.insert(i);
    }
    assert_concise_debug(&bloom);

    let mut xor_builder = XorFilterBuilder::new(XorFilterType::Xor8, 0);
    xor_builder.update_values(0..500_u64);
    let xor = xor_builder.build().unwrap();
    assert_concise_debug(&xor);

    let mut count_min = CountMinSketch::<i64>::new(5, 512).unwrap();
    for i in 0..500_u64 {
        count_min.update(i);
    }
    assert_concise_debug(&count_min);

    let mut freq = FrequentItemsSketch::<u64>::new(256).unwrap();
    for i in 0..500_u64 {
        freq.update(i);
    }
    assert_concise_debug(&freq);

    let mut hll = HllSketch::new(12, HllType::Hll8).unwrap();
    for i in 0..2000_u64 {
        hll.update(i);
    }
    let mut hll_union = HllUnion::new(12).unwrap();
    hll_union.update(&hll);
    assert_concise_debug(&hll_union);

    let mut kll = KllSketch::<u64>::new(200).unwrap();
    for i in 0..2000_u64 {
        kll.update(i);
    }
    assert_concise_debug(&kll);
    assert_concise_debug(&kll.sorted_view());

    let mut req = ReqSketch::<u64>::new(12, RankAccuracy::HighRank).unwrap();
    for i in 0..2000_u64 {
        req.update(i);
    }
    assert_concise_debug(&req);
    assert_concise_debug(&req.sorted_view());

    let mut tdigest_mut = TDigestMut::new(100).unwrap();
    for i in 0..2000_u64 {
        tdigest_mut.update(i as f64);
    }
    assert_concise_debug(&tdigest_mut);
    let tdigest = tdigest_mut.freeze();
    assert_concise_debug(&tdigest);

    let mut theta = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    for i in 0..5000_u64 {
        theta.update(i);
    }
    let compact_theta = theta.compact(true);
    assert_concise_debug(&theta);
    assert_concise_debug(&theta.as_view());
    assert_concise_debug(&compact_theta);
    assert_concise_debug(&compact_theta.as_view());

    let mut tuple = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .lg_k(12)
        .build()
        .unwrap();
    for i in 0..5000_u64 {
        tuple.update(i, 1);
    }
    let compact_tuple = tuple.compact(true);
    assert_concise_debug(&tuple);
    assert_concise_debug(&tuple.as_view());
    assert_concise_debug(&compact_tuple);
    assert_concise_debug(&compact_tuple.as_view());

    let mut bytes = SketchBytes::with_capacity(1024);
    bytes.write(&[0xAB; 512]);
    assert_concise_debug(&bytes);
    let raw = bytes.into_bytes();
    let slice = SketchSlice::new(&raw);
    assert_concise_debug(&slice);
}

#[test]
fn debug_and_clone_do_not_require_debug_on_items_summaries_or_policies() {
    let mut freq = FrequentItemsSketch::new(64).unwrap();
    freq.update(NonDebugItem(7));
    assert_concise_debug(&freq);

    let mut req = ReqSketch::new(12, RankAccuracy::HighRank).unwrap();
    for i in 0..50_u64 {
        req.update(NonDebugItem(i));
    }
    let mut iter = req.iter();
    assert!(iter.next().map(|(item, _)| *item) == Some(NonDebugItem(0)));
    assert_concise_debug(&iter);
    let cloned_iter = iter.clone();
    let remaining_original: Vec<(NonDebugItem, u64)> =
        iter.map(|(item, weight)| (*item, weight)).collect();
    let remaining_cloned: Vec<(NonDebugItem, u64)> =
        cloned_iter.map(|(item, weight)| (*item, weight)).collect();
    assert!(remaining_original == remaining_cloned);

    let update_policy = DefaultUpdatePolicy::<NonCopyNonDebugSummary>::default();
    let copied_update_policy = update_policy;
    assert_concise_debug(&copied_update_policy);
    let union_policy = DefaultUnionPolicy::<NonCopyNonDebugSummary>::default();
    let copied_union_policy = union_policy;
    assert_concise_debug(&copied_union_policy);

    let builder = TupleSketchBuilder::new(DefaultUpdatePolicy::<NonDebugItem>::default()).lg_k(8);
    let mut a = builder.clone().build().unwrap();
    let mut b = builder.build().unwrap();
    a.update("shared", 2_u64);
    a.update("only_a", 1_u64);
    b.update("shared", 5_u64);
    b.update("only_b", 3_u64);

    let cloned_a = a.clone();
    assert_eq!(cloned_a.num_retained(), a.num_retained());
    assert_concise_debug(&a);
    assert_concise_debug(&a.as_view());
    let compact_a = a.compact(true);
    assert_concise_debug(&compact_a);
    assert_concise_debug(&compact_a.as_view());

    let union_builder = TupleUnionBuilder::new(NonDebugCombinePolicy).lg_k(8);
    let mut union = union_builder.clone().build().unwrap();
    union.update(&a).unwrap();
    let mut cloned_union = union.clone();
    cloned_union.update(&b).unwrap();
    assert_concise_debug(&union);
    assert_eq!(union.to_sketch(true).num_retained(), 2);
    assert_eq!(cloned_union.to_sketch(true).num_retained(), 3);

    let mut inter = TupleIntersection::new(NonDebugCombinePolicy);
    inter.update(&a).unwrap();
    let mut cloned_inter = inter.clone();
    cloned_inter.update(&b).unwrap();
    assert_concise_debug(&inter);
    assert_eq!(inter.to_sketch(true).unwrap().num_retained(), 2);
    let shared = cloned_inter.to_sketch(true).unwrap();
    assert_eq!(shared.num_retained(), 1);
    assert!(shared.iter().next().unwrap().summary() == &NonDebugItem(7));
}

#[test]
fn union_intersection_tdigest_and_codec_clone_and_debug_invariants() {
    let builder_large = ThetaSketchBuilder::default().lg_k(12);
    let mut large = builder_large.clone().build().unwrap();
    for i in 0..2000_u64 {
        large.update(i);
    }
    let mut small = ThetaSketchBuilder::default().lg_k(5).build().unwrap();
    for i in 10_000..15_000_u64 {
        small.update(i);
    }

    let mut union = ThetaUnionBuilder::default().lg_k(12).build().unwrap();
    union.update(&large).unwrap();
    let snapshot_before_small = union.clone();
    union.update(&small).unwrap();

    let expected_retained = union.to_sketch(false).num_retained();
    let union_debug = format!("{union:?}");
    assert!(
        union_debug.contains(&format!("num_retained: {expected_retained}")),
        "expected ThetaUnion debug output to reflect theta-screened retained count ({expected_retained}), got: {union_debug}"
    );
    assert_eq!(snapshot_before_small.to_sketch(false).num_retained(), 2000);

    let mut inter = ThetaIntersection::default();
    let uninit_debug = format!("{inter:?}");
    assert!(uninit_debug.contains("has_result: false"));
    assert!(!uninit_debug.contains("is_empty"));

    let mut left = builder_large.build().unwrap();
    left.update("only_left");
    let mut right = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    right.update("only_right");

    inter.update(&left).unwrap();
    let mid_inter = inter.clone();
    inter.update(&right).unwrap();

    assert_eq!(mid_inter.to_sketch(true).unwrap().num_retained(), 1);
    let disjoint_debug = format!("{inter:?}");
    assert!(disjoint_debug.contains("has_result: true"));
    assert!(disjoint_debug.contains("is_empty: true"));

    let mut tdigest_mut = TDigestMut::new(50).unwrap();
    for v in [1.0, 2.0, 3.0, 10.0] {
        tdigest_mut.update(v);
    }
    let tdigest = tdigest_mut.freeze();
    let cloned_tdigest = tdigest.clone();
    assert_eq!(
        tdigest.quantile(0.5).unwrap(),
        cloned_tdigest.quantile(0.5).unwrap()
    );

    let mut bytes = SketchBytes::default();
    bytes.write_u32_le(0x1122_3344);
    let mut cloned_bytes = bytes.clone();
    cloned_bytes.write_u32_le(0x5566_7788);
    let original_buf = bytes.into_bytes();
    let cloned_buf = cloned_bytes.into_bytes();
    assert_eq!(original_buf.len(), 4);
    assert_eq!(cloned_buf.len(), 8);

    let mut slice = SketchSlice::new(&cloned_buf);
    assert_eq!(slice.read_u16_le().unwrap(), 0x3344);
    let mut cloned_slice = slice.clone();
    assert_eq!(cloned_slice.read_u16_le().unwrap(), 0x1122);
    assert_eq!(slice.remaining().len(), 6);
    assert_eq!(cloned_slice.remaining().len(), 4);
}
