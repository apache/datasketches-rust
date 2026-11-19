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

use std::fs;

use datasketches::density::DensitySketch;
use datasketches::density::Kernel;
use datasketches::error::ErrorKind;

use crate::serialization_test_data;

// Generated from datasketches-cpp commit 70b86faa63402d7f62f5a6b1b05109d5fd1f2186.
const CPP_F64_EMPTY: &str = "030113040a00000003000000";

const CPP_F64_SINGLE: &str = concat!(
    "060113000a0000000300000001000000010000000000000001000000000000000000f83f",
    "00000000000002c0000000000000e03f",
);

const CPP_F64_EXACT_N10: &str = concat!(
    "060113000a000000030000000a0000000a000000000000000a0000000000000000000000",
    "00000000000000000000000000000080000000000000f03f000000000000f03f00000000",
    "0000f0bf0000000000000040cd3b7f669ea0f63f00000000000000c00000000000000840",
    "aa4c58e87ab6fb3f00000000000008c00000000000001040000000000000004000000000",
    "000010c00000000000001440a8f4979b77e3014000000000000014c00000000000001840",
    "2e2109148e98034000000000000018c00000000000001c40eaf8d2a97f2a054000000000",
    "00001cc00000000000002040cd3b7f669ea0064000000000000020c00000000000002240",
    "000000000000084000000000000022c0",
);

const CPP_F64_EST_N1031: &str = concat!(
    "060113000a000000030000003b0000000704000000000000080000000000000000f88f40",
    "c0fef7bffffb3f400000000000f88fc00000000000009040000000000000404000000000",
    "000090c0000000000004904060ff03e0ff01404000000000000490c00000000000089040",
    "03f61f80ff03404000000000000890c000000000000c90407bcd6be0fe05404000000000",
    "000c90c000000000001090407060ff00fe07404000000000001090c00000000000149040",
    "b57af2e1fc09404000000000001490c000000000001890404fd95c83fb0b404000000000",
    "001890c0080000000000000000988f406f4acb7ad5cb3f400000000000988fc000000000",
    "00d88f4012f014bcf9eb3f400000000000d88fc00000000000a88f4004f81f96e1d33f40",
    "0000000000a88fc00000000000b88f40f5c317a9ebdb3f400000000000b88fc000000000",
    "00f08f40f9ebbffffef73f400000000000f08fc00000000000888f40a83e8e55c7c33f40",
    "0000000000888fc00000000000788f403062db24b7bb3f400000000000788fc000000000",
    "00c88f4051383cb5f3e33f400000000000c88fc0070000000000000000f88e409580d345",
    "eb7a3f400000000000f88ec00000000000b08e408813a4b73d563f400000000000b08ec0",
    "0000000000988e40ea75a44ffa493f400000000000988ec00000000000d88e4028e86a6c",
    "a36a3f400000000000d88ec00000000000188f40638714b82a8b3f400000000000188fc0",
    "0000000000588f405d8ad49a90ab3f400000000000588fc00000000000388f40507223d0",
    "619b3f400000000000388fc0080000000000000000b08d40f0c7342e73d23e4000000000",
    "00b08dc00000000000388e40e8f93053bc183f400000000000388ec00000000000408d40",
    "f2479b0e18983e400000000000408dc00000000000708c40867c4a7e8c2a3e4000000000",
    "00708cc00000000000e88b40374d513513e23d400000000000e88bc00000000000e88d40",
    "092c4a7677ef3e400000000000e88dc00000000000408c40af5f10390c113e4000000000",
    "00408cc00000000000a88c404791c5aa31483e400000000000a88cc00900000000000000",
    "00408b40b2db5de396873d400000000000408bc00000000000988840e2964481b30d3c40",
    "00000000009888c000000000006089404c6d0e8ae07e3c4000000000006089c000000000",
    "00b08a405648d1c528393d400000000000b08ac00000000000e087407c2d6714fba33b40",
    "0000000000e087c00000000000088b40fd89c3952f693d400000000000088bc000000000",
    "00808740e69335b8306c3b4000000000008087c00000000000d888406779674d1c323c40",
    "0000000000d888c00000000000188a4075f0484778e53c400000000000188ac013000000",
    "0000000000806c405ccf56cc07332e400000000000806cc00000000000b071405ec7532a",
    "96d230400000000000b071c00000000000e07240741d3441ce6031400000000000e072c0",
    "000000000000574016047775ee2e234000000000000057c00000000000406540516699cb",
    "a9132a4000000000004065c00000000000206240263577e24b15284000000000002062c0",
    "0000000000003a40f62f125f656514400000000000003ac0000000000000000000000000",
    "000000000000000000000080000000000020694093153a51da5a2c4000000000002069c0",
    "0000000000607b40fe106bdeaeed34400000000000607bc00000000000708040c6a20cd6",
    "47ef364000000000007080c00000000000307c400bef6a579c3c35400000000000307cc0",
    "0000000000907d401c2402d6a1bf35400000000000907dc000000000006885407c86e243",
    "292c3a4000000000006885c00000000000307440617f1226e2f8314000000000003074c0",
    "000000000080824039dc63b3bf54384000000000008082c0000000000020784057b2db1a",
    "9aa5334000000000002078c00000000000407e4000000000000036400000000000407ec0",
    "00000000007081404dfba9f03c9f374000000000007081c0",
);

const CPP_F32_EMPTY: &str = "030113040a00000003000000";

const CPP_F32_EXACT_N10: &str = concat!(
    "060113000a000000030000000a0000000a000000000000000a0000000000000000000000",
    "000000800000803f0000803f000080bf00000040f304b53f000000c000004040d7b3dd3f",
    "000040c00000804000000040000080c00000a040bd1b0f400000a0c00000c04071c41c40",
    "0000c0c00000e040fd5329400000e0c000000041f3043540000000c10000104100004040",
    "000010c1",
);

const CPP_F32_EST_N1031: &str = concat!(
    "060113000a00000003000000370000000704000000000000070000000000804400000042",
    "000080c400208044ff0f0042002080c400408044fc1f0042004080c400608044f72f0042",
    "006080c400808044f03f0042008080c400a08044e74f004200a080c400c08044dc5f0042",
    "00c080c415000000004079443a9afc41004079c4004078446018fc41004078c400007744",
    "b175fb41000077c400c0754499d2fa4100c075c400807744d1b6fb41008077c400407644",
    "e313fb41004076c400c078445659fc4100c078c400007c44fcfdfd4100007cc400007b44",
    "d87cfd4100007bc400407e449e1fff4100407ec400c07f44fedfff4100c07fc400007a44",
    "72fbfc4100007ac400407f44ee9fff4100407fc400c07c44ac5efe4100c07cc400407d44",
    "0d9ffe4100407dc400c07d445ddffe4100c07dc400807944a7bafc41008079c400807b44",
    "72bdfd4100807bc400007f44e07fff4100007fc400407a44d21bfd4100407ac400407c44",
    "3b1efe4100407cc4000000001300000000c072447849f94100c072c4004067447d4ff341",
    "004067c400806944c67df441008069c400406644abc8f241004066c400005744269bea41",
    "000057c400c06f44e7bdf74100c06fc400005a44b73cec4100005ac400806c44910ef641",
    "00806cc400805f44dc32ef4100805fc40000744491edf941000074c4004061442022f041",
    "004061c4000055442783e941000055c400805c447f96ed4100805cc4000063447610f141",
    "000063c400006e4403d6f64100006ec40080644406dcf141008064c400405d44d9fded41",
    "00405dc4008070440621f841008070c400c06b449caaf54100c06bc40700000000404d44",
    "8b39e54100404dc400403a44855bda4100403ac400401b44e65bc74100401bc400c03e44",
    "c9fadc4100c03ec4000051442f4fe741000051c400404844616ae241004048c400004b44",
    "04f7e34100004bc40100000000009f4312a98e4100009fc3",
);

fn decode_hex(input: &str) -> Vec<u8> {
    assert_eq!(input.len() % 2, 0);
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let digits = std::str::from_utf8(digits).unwrap();
            u8::from_str_radix(digits, 16).unwrap()
        })
        .collect()
}

fn assert_invalid_f64(bytes: &[u8]) {
    let err = DensitySketch::<f64>::deserialize(bytes).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

fn assert_invalid_f32(bytes: &[u8]) {
    let err = DensitySketch::<f32>::deserialize(bytes).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

fn verify_cpp_f64_snapshot(
    hex: &str,
    expected_k: u16,
    expected_dim: u32,
    expected_n: u64,
    expected_est_mode: bool,
    expected_retained: &[(u64, [u64; 3])],
    expected_queries: &[([u64; 3], u64)],
) {
    let bytes = decode_hex(hex);
    let sketch = DensitySketch::<f64>::deserialize(&bytes).unwrap();
    assert_eq!(sketch.k(), expected_k);
    assert_eq!(sketch.dim(), expected_dim);
    assert_eq!(sketch.total_weight(), expected_n);
    assert_eq!(sketch.num_retained(), expected_retained.len());
    assert_eq!(sketch.is_empty(), expected_n == 0);
    assert_eq!(sketch.is_estimation_mode(), expected_est_mode);
    assert_eq!(sketch.serialize(), bytes);

    let actual_retained: Vec<(u64, [u64; 3])> = sketch
        .iter()
        .map(|(pt, weight)| (weight, [pt[0].to_bits(), pt[1].to_bits(), pt[2].to_bits()]))
        .collect();
    assert_eq!(actual_retained, expected_retained);

    if expected_n == 0 {
        assert_eq!(sketch.estimate(&[0.0, 0.0, 0.0]).unwrap(), None);
    } else {
        for &(q_bits, est_bits) in expected_queries {
            let query = [
                f64::from_bits(q_bits[0]),
                f64::from_bits(q_bits[1]),
                f64::from_bits(q_bits[2]),
            ];
            let actual = sketch.estimate(&query).unwrap().unwrap();
            let expected = f64::from_bits(est_bits);
            if expected == 0.0 {
                assert_eq!(actual, 0.0);
            } else {
                let rel_err = (actual - expected).abs() / expected.abs();
                assert!(
                    rel_err <= 1e-12,
                    "estimate mismatch for {query:?}: actual={actual}, expected={expected}"
                );
            }
        }
    }
}

fn verify_cpp_f32_snapshot(
    hex: &str,
    expected_k: u16,
    expected_dim: u32,
    expected_n: u64,
    expected_est_mode: bool,
    expected_retained: &[(u64, [u32; 3])],
    expected_queries: &[([u32; 3], u32)],
) {
    let bytes = decode_hex(hex);
    let sketch = DensitySketch::<f32>::deserialize(&bytes).unwrap();
    assert_eq!(sketch.k(), expected_k);
    assert_eq!(sketch.dim(), expected_dim);
    assert_eq!(sketch.total_weight(), expected_n);
    assert_eq!(sketch.num_retained(), expected_retained.len());
    assert_eq!(sketch.is_empty(), expected_n == 0);
    assert_eq!(sketch.is_estimation_mode(), expected_est_mode);
    assert_eq!(sketch.serialize(), bytes);

    let actual_retained: Vec<(u64, [u32; 3])> = sketch
        .iter()
        .map(|(pt, weight)| (weight, [pt[0].to_bits(), pt[1].to_bits(), pt[2].to_bits()]))
        .collect();
    assert_eq!(actual_retained, expected_retained);

    if expected_n == 0 {
        assert_eq!(sketch.estimate(&[0.0, 0.0, 0.0]).unwrap(), None);
    } else {
        for &(q_bits, est_bits) in expected_queries {
            let query = [
                f32::from_bits(q_bits[0]),
                f32::from_bits(q_bits[1]),
                f32::from_bits(q_bits[2]),
            ];
            let actual = sketch.estimate(&query).unwrap().unwrap();
            let expected = f64::from(f32::from_bits(est_bits));
            assert!(
                (actual - expected).abs() <= 1e-6,
                "estimate mismatch for {query:?}: actual={actual}, expected={expected}"
            );
        }
    }
}

#[test]
fn cpp_f64_empty_and_single_snapshots_round_trip() {
    verify_cpp_f64_snapshot(CPP_F64_EMPTY, 10, 3, 0, false, &[], &[]);

    let single_retained = [(
        1,
        [0x3ff8000000000000, 0xc002000000000000, 0x3fe0000000000000],
    )];
    let single_queries = [
        (
            [0x3ff8000000000000, 0xc002000000000000, 0x3fe0000000000000],
            0x3ff0000000000000,
        ),
        (
            [0x3ff0000000000000, 0xc000000000000000, 0x0],
            0x3fe23ba930c1568b,
        ),
        ([0x0, 0x0, 0x0], 0x3f41068222437d65),
    ];
    verify_cpp_f64_snapshot(
        CPP_F64_SINGLE,
        10,
        3,
        1,
        false,
        &single_retained,
        &single_queries,
    );
}

#[test]
fn cpp_f64_exact_snapshot_round_trip() {
    let retained = [
        (1, [0x0, 0x0, 0x8000000000000000]),
        (
            1,
            [0x3ff0000000000000, 0x3ff0000000000000, 0xbff0000000000000],
        ),
        (
            1,
            [0x4000000000000000, 0x3ff6a09e667f3bcd, 0xc000000000000000],
        ),
        (
            1,
            [0x4008000000000000, 0x3ffbb67ae8584caa, 0xc008000000000000],
        ),
        (
            1,
            [0x4010000000000000, 0x4000000000000000, 0xc010000000000000],
        ),
        (
            1,
            [0x4014000000000000, 0x4001e3779b97f4a8, 0xc014000000000000],
        ),
        (
            1,
            [0x4018000000000000, 0x4003988e1409212e, 0xc018000000000000],
        ),
        (
            1,
            [0x401c000000000000, 0x40052a7fa9d2f8ea, 0xc01c000000000000],
        ),
        (
            1,
            [0x4020000000000000, 0x4006a09e667f3bcd, 0xc020000000000000],
        ),
        (
            1,
            [0x4022000000000000, 0x4008000000000000, 0xc022000000000000],
        ),
    ];
    let queries = [
        ([0x0, 0x0, 0x0], 0x3fbae02e9c165f83),
        (
            [0x4014000000000000, 0x4001e3779b97f4a8, 0xc014000000000000],
            0x3fc019bb4a13fca4,
        ),
        (
            [0x4024000000000000, 0x4008000000000000, 0xc024000000000000],
            0x3f8bc88b7f565279,
        ),
        (
            [0x4022000000000000, 0x4008000000000000, 0xc022000000000000],
            0x3fbcf8be877a44c5,
        ),
    ];
    verify_cpp_f64_snapshot(CPP_F64_EXACT_N10, 10, 3, 10, false, &retained, &queries);
}

#[test]
fn cpp_f64_estimation_snapshot_round_trip() {
    let retained = [
        (
            1,
            [0x408ff80000000000, 0x403ffbffbff7fec0, 0xc08ff80000000000],
        ),
        (
            1,
            [0x4090000000000000, 0x4040000000000000, 0xc090000000000000],
        ),
        (
            1,
            [0x4090040000000000, 0x404001ffe003ff60, 0xc090040000000000],
        ),
        (
            1,
            [0x4090080000000000, 0x404003ff801ff603, 0xc090080000000000],
        ),
        (
            1,
            [0x40900c0000000000, 0x404005fee06bcd7b, 0xc0900c0000000000],
        ),
        (
            1,
            [0x4090100000000000, 0x404007fe00ff6070, 0xc090100000000000],
        ),
        (
            1,
            [0x4090140000000000, 0x404009fce1f27ab5, 0xc090140000000000],
        ),
        (
            1,
            [0x4090180000000000, 0x40400bfb835cd94f, 0xc090180000000000],
        ),
        (
            2,
            [0x408f980000000000, 0x403fcbd57acb4a6f, 0xc08f980000000000],
        ),
        (
            2,
            [0x408fd80000000000, 0x403febf9bc14f012, 0xc08fd80000000000],
        ),
        (
            2,
            [0x408fa80000000000, 0x403fd3e1961ff804, 0xc08fa80000000000],
        ),
        (
            2,
            [0x408fb80000000000, 0x403fdbeba917c3f5, 0xc08fb80000000000],
        ),
        (
            2,
            [0x408ff00000000000, 0x403ff7feffbfebf9, 0xc08ff00000000000],
        ),
        (
            2,
            [0x408f880000000000, 0x403fc3c7558e3ea8, 0xc08f880000000000],
        ),
        (
            2,
            [0x408f780000000000, 0x403fbbb724db6230, 0xc08f780000000000],
        ),
        (
            2,
            [0x408fc80000000000, 0x403fe3f3b53c3851, 0xc08fc80000000000],
        ),
        (
            4,
            [0x408ef80000000000, 0x403f7aeb45d38095, 0xc08ef80000000000],
        ),
        (
            4,
            [0x408eb00000000000, 0x403f563db7a41388, 0xc08eb00000000000],
        ),
        (
            4,
            [0x408e980000000000, 0x403f49fa4fa475ea, 0xc08e980000000000],
        ),
        (
            4,
            [0x408ed80000000000, 0x403f6aa36c6ae828, 0xc08ed80000000000],
        ),
        (
            4,
            [0x408f180000000000, 0x403f8b2ab8148763, 0xc08f180000000000],
        ),
        (
            4,
            [0x408f580000000000, 0x403fab909ad48a5d, 0xc08f580000000000],
        ),
        (
            4,
            [0x408f380000000000, 0x403f9b61d0237250, 0xc08f380000000000],
        ),
        (
            8,
            [0x408db00000000000, 0x403ed2732e34c7f0, 0xc08db00000000000],
        ),
        (
            8,
            [0x408e380000000000, 0x403f18bc5330f9e8, 0xc08e380000000000],
        ),
        (
            8,
            [0x408d400000000000, 0x403e98180e9b47f2, 0xc08d400000000000],
        ),
        (
            8,
            [0x408c700000000000, 0x403e2a8c7e4a7c86, 0xc08c700000000000],
        ),
        (
            8,
            [0x408be80000000000, 0x403de21335514d37, 0xc08be80000000000],
        ),
        (
            8,
            [0x408de80000000000, 0x403eef77764a2c09, 0xc08de80000000000],
        ),
        (
            8,
            [0x408c400000000000, 0x403e110c39105faf, 0xc08c400000000000],
        ),
        (
            8,
            [0x408ca80000000000, 0x403e4831aac59147, 0xc08ca80000000000],
        ),
        (
            16,
            [0x408b400000000000, 0x403d8796e35ddbb2, 0xc08b400000000000],
        ),
        (
            16,
            [0x4088980000000000, 0x403c0db3814496e2, 0xc088980000000000],
        ),
        (
            16,
            [0x4089600000000000, 0x403c7ee08a0e6d4c, 0xc089600000000000],
        ),
        (
            16,
            [0x408ab00000000000, 0x403d3928c5d14856, 0xc08ab00000000000],
        ),
        (
            16,
            [0x4087e00000000000, 0x403ba3fb14672d7c, 0xc087e00000000000],
        ),
        (
            16,
            [0x408b080000000000, 0x403d692f95c389fd, 0xc08b080000000000],
        ),
        (
            16,
            [0x4087800000000000, 0x403b6c30b83593e6, 0xc087800000000000],
        ),
        (
            16,
            [0x4088d80000000000, 0x403c321c4d677967, 0xc088d80000000000],
        ),
        (
            16,
            [0x408a180000000000, 0x403ce5784748f075, 0xc08a180000000000],
        ),
        (
            32,
            [0x406c800000000000, 0x402e3307cc56cf5c, 0xc06c800000000000],
        ),
        (
            32,
            [0x4071b00000000000, 0x4030d2962a53c75e, 0xc071b00000000000],
        ),
        (
            32,
            [0x4072e00000000000, 0x403160ce41341d74, 0xc072e00000000000],
        ),
        (
            32,
            [0x4057000000000000, 0x40232eee75770416, 0xc057000000000000],
        ),
        (
            32,
            [0x4065400000000000, 0x402a13a9cb996651, 0xc065400000000000],
        ),
        (
            32,
            [0x4062200000000000, 0x4028154be2773526, 0xc062200000000000],
        ),
        (
            32,
            [0x403a000000000000, 0x401465655f122ff6, 0xc03a000000000000],
        ),
        (32, [0x0, 0x0, 0x8000000000000000]),
        (
            32,
            [0x4069200000000000, 0x402c5ada513a1593, 0xc069200000000000],
        ),
        (
            32,
            [0x407b600000000000, 0x4034edaede6b10fe, 0xc07b600000000000],
        ),
        (
            32,
            [0x4080700000000000, 0x4036ef47d60ca2c6, 0xc080700000000000],
        ),
        (
            32,
            [0x407c300000000000, 0x40353c9c576aef0b, 0xc07c300000000000],
        ),
        (
            32,
            [0x407d900000000000, 0x4035bfa1d602241c, 0xc07d900000000000],
        ),
        (
            32,
            [0x4085680000000000, 0x403a2c2943e2867c, 0xc085680000000000],
        ),
        (
            32,
            [0x4074300000000000, 0x4031f8e226127f61, 0xc074300000000000],
        ),
        (
            32,
            [0x4082800000000000, 0x403854bfb363dc39, 0xc082800000000000],
        ),
        (
            32,
            [0x4078200000000000, 0x4033a59a1adbb257, 0xc078200000000000],
        ),
        (
            32,
            [0x407e400000000000, 0x4036000000000000, 0xc07e400000000000],
        ),
        (
            32,
            [0x4081700000000000, 0x40379f3cf0a9fb4d, 0xc081700000000000],
        ),
    ];
    let queries = [
        ([0x0, 0x0, 0x0], 0x3f9fc86155aa1659),
        (
            [0x4014000000000000, 0x4001e3779b97f4a8, 0xc014000000000000],
            0x3aa8f78601e450e6,
        ),
        (
            [0x407f400000000000, 0x40365c55827df1d2, 0xc07f400000000000],
            0x1171a9a3d20a9a95,
        ),
        (
            [0x408ff80000000000, 0x403ffbffbff7fec0, 0xc08ff80000000000],
            0x3f5658dbdfc55bfa,
        ),
        (
            [0x408c400000000000, 0x403e110c39105faf, 0xc08c400000000000],
            0x3f7fc86155aa1659,
        ),
        (
            [0x4081700000000000, 0x40379f3cf0a9fb4d, 0xc081700000000000],
            0x3f9fc86155aa1659,
        ),
    ];
    verify_cpp_f64_snapshot(CPP_F64_EST_N1031, 10, 3, 1031, true, &retained, &queries);
}

#[test]
fn cpp_f32_snapshots_round_trip() {
    verify_cpp_f32_snapshot(CPP_F32_EMPTY, 10, 3, 0, false, &[], &[]);

    let exact_retained = [
        (1, [0x0, 0x0, 0x80000000]),
        (1, [0x3f800000, 0x3f800000, 0xbf800000]),
        (1, [0x40000000, 0x3fb504f3, 0xc0000000]),
        (1, [0x40400000, 0x3fddb3d7, 0xc0400000]),
        (1, [0x40800000, 0x40000000, 0xc0800000]),
        (1, [0x40a00000, 0x400f1bbd, 0xc0a00000]),
        (1, [0x40c00000, 0x401cc471, 0xc0c00000]),
        (1, [0x40e00000, 0x402953fd, 0xc0e00000]),
        (1, [0x41000000, 0x403504f3, 0xc1000000]),
        (1, [0x41100000, 0x40400000, 0xc1100000]),
    ];
    let exact_queries = [
        ([0x0, 0x0, 0x0], 0x3dd70175),
        ([0x40a00000, 0x400f1bbd, 0xc0a00000], 0x3e00cdda),
        ([0x41100000, 0x40400000, 0xc1100000], 0x3de7c5f4),
    ];
    verify_cpp_f32_snapshot(
        CPP_F32_EXACT_N10,
        10,
        3,
        10,
        false,
        &exact_retained,
        &exact_queries,
    );

    let est_retained = [
        (1, [0x44800000, 0x42000000, 0xc4800000]),
        (1, [0x44802000, 0x42000fff, 0xc4802000]),
        (1, [0x44804000, 0x42001ffc, 0xc4804000]),
        (1, [0x44806000, 0x42002ff7, 0xc4806000]),
        (1, [0x44808000, 0x42003ff0, 0xc4808000]),
        (1, [0x4480a000, 0x42004fe7, 0xc480a000]),
        (1, [0x4480c000, 0x42005fdc, 0xc480c000]),
        (2, [0x44794000, 0x41fc9a3a, 0xc4794000]),
        (2, [0x44784000, 0x41fc1860, 0xc4784000]),
        (2, [0x44770000, 0x41fb75b1, 0xc4770000]),
        (2, [0x4475c000, 0x41fad299, 0xc475c000]),
        (2, [0x44778000, 0x41fbb6d1, 0xc4778000]),
        (2, [0x44764000, 0x41fb13e3, 0xc4764000]),
        (2, [0x4478c000, 0x41fc5956, 0xc478c000]),
        (2, [0x447c0000, 0x41fdfdfc, 0xc47c0000]),
        (2, [0x447b0000, 0x41fd7cd8, 0xc47b0000]),
        (2, [0x447e4000, 0x41ff1f9e, 0xc47e4000]),
        (2, [0x447fc000, 0x41ffdffe, 0xc47fc000]),
        (2, [0x447a0000, 0x41fcfb72, 0xc47a0000]),
        (2, [0x447f4000, 0x41ff9fee, 0xc47f4000]),
        (2, [0x447cc000, 0x41fe5eac, 0xc47cc000]),
        (2, [0x447d4000, 0x41fe9f0d, 0xc47d4000]),
        (2, [0x447dc000, 0x41fedf5d, 0xc47dc000]),
        (2, [0x44798000, 0x41fcbaa7, 0xc4798000]),
        (2, [0x447b8000, 0x41fdbd72, 0xc47b8000]),
        (2, [0x447f0000, 0x41ff7fe0, 0xc47f0000]),
        (2, [0x447a4000, 0x41fd1bd2, 0xc47a4000]),
        (2, [0x447c4000, 0x41fe1e3b, 0xc47c4000]),
        (8, [0x4472c000, 0x41f94978, 0xc472c000]),
        (8, [0x44674000, 0x41f34f7d, 0xc4674000]),
        (8, [0x44698000, 0x41f47dc6, 0xc4698000]),
        (8, [0x44664000, 0x41f2c8ab, 0xc4664000]),
        (8, [0x44570000, 0x41ea9b26, 0xc4570000]),
        (8, [0x446fc000, 0x41f7bde7, 0xc46fc000]),
        (8, [0x445a0000, 0x41ec3cb7, 0xc45a0000]),
        (8, [0x446c8000, 0x41f60e91, 0xc46c8000]),
        (8, [0x445f8000, 0x41ef32dc, 0xc45f8000]),
        (8, [0x44740000, 0x41f9ed91, 0xc4740000]),
        (8, [0x44614000, 0x41f02220, 0xc4614000]),
        (8, [0x44550000, 0x41e98327, 0xc4550000]),
        (8, [0x445c8000, 0x41ed967f, 0xc45c8000]),
        (8, [0x44630000, 0x41f11076, 0xc4630000]),
        (8, [0x446e0000, 0x41f6d603, 0xc46e0000]),
        (8, [0x44648000, 0x41f1dc06, 0xc4648000]),
        (8, [0x445d4000, 0x41edfdd9, 0xc45d4000]),
        (8, [0x44708000, 0x41f82106, 0xc4708000]),
        (8, [0x446bc000, 0x41f5aa9c, 0xc46bc000]),
        (16, [0x444d4000, 0x41e5398b, 0xc44d4000]),
        (16, [0x443a4000, 0x41da5b85, 0xc43a4000]),
        (16, [0x441b4000, 0x41c75be6, 0xc41b4000]),
        (16, [0x443ec000, 0x41dcfac9, 0xc43ec000]),
        (16, [0x44510000, 0x41e74f2f, 0xc4510000]),
        (16, [0x44484000, 0x41e26a61, 0xc4484000]),
        (16, [0x444b0000, 0x41e3f704, 0xc44b0000]),
        (32, [0x439f0000, 0x418ea912, 0xc39f0000]),
    ];
    let est_queries = [
        ([0x0, 0x0, 0x0], 0x0),
        ([0x40a00000, 0x400f1bbd, 0xc0a00000], 0x0),
        ([0x43fa0000, 0x41b2e2ac, 0xc3fa0000], 0x0),
        ([0x44800000, 0x42000000, 0xc4800000], 0x3ab2c6e0),
        ([0x447c4000, 0x41fe1e3b, 0xc47c4000], 0x3b105fe5),
        ([0x439f0000, 0x418ea912, 0xc39f0000], 0x3cfe430b),
    ];
    verify_cpp_f32_snapshot(
        CPP_F32_EST_N1031,
        10,
        3,
        1031,
        true,
        &est_retained,
        &est_queries,
    );
}

#[derive(Debug, Clone, PartialEq)]
struct CustomScaleKernel {
    scale: f64,
}

impl Kernel<f64> for CustomScaleKernel {
    fn evaluate(&self, a: &[f64], b: &[f64]) -> f64 {
        assert_eq!(a.len(), b.len(), "point dimension mismatch");
        let dist_sq: f64 = a.iter().zip(b).map(|(&x, &y)| (x - y) * (x - y)).sum();
        (-self.scale * dist_sq).exp()
    }
}

#[test]
fn round_trips_for_rust_constructed_sketches() {
    // Empty round-trips for f64 and f32.
    let empty_f64 = DensitySketch::<f64>::new(12, 4).unwrap();
    let bytes = empty_f64.serialize();
    let decoded = DensitySketch::<f64>::deserialize(&bytes).unwrap();
    assert!(decoded.is_empty());
    assert_eq!(decoded.k(), 12);
    assert_eq!(decoded.dim(), 4);
    assert_eq!(decoded.serialize(), bytes);

    let empty_f32 = DensitySketch::<f32>::new(2, 1).unwrap();
    let bytes = empty_f32.serialize();
    let decoded = DensitySketch::<f32>::deserialize(&bytes).unwrap();
    assert!(decoded.is_empty());
    assert_eq!(decoded.k(), 2);
    assert_eq!(decoded.dim(), 1);
    assert_eq!(decoded.serialize(), bytes);

    // Exact and estimation round-trips for f64 and f32.
    let mut est_f64 = DensitySketch::<f64>::new(10, 2).unwrap();
    for i in 0..10 {
        est_f64.update(&[i as f64, -(i as f64)]).unwrap();
    }
    let exact_bytes = est_f64.serialize();
    let exact_decoded = DensitySketch::<f64>::deserialize(&exact_bytes).unwrap();
    assert!(!exact_decoded.is_estimation_mode());
    assert_eq!(exact_decoded.serialize(), exact_bytes);
    assert_eq!(
        exact_decoded.estimate(&[5.0, -5.0]).unwrap(),
        est_f64.estimate(&[5.0, -5.0]).unwrap()
    );

    for i in 10..200 {
        est_f64.update(&[i as f64, -(i as f64)]).unwrap();
    }
    let est_bytes = est_f64.serialize();
    let est_decoded = DensitySketch::<f64>::deserialize(&est_bytes).unwrap();
    assert!(est_decoded.is_estimation_mode());
    assert_eq!(est_decoded.serialize(), est_bytes);
    assert_eq!(
        est_decoded.estimate(&[100.0, -100.0]).unwrap(),
        est_f64.estimate(&[100.0, -100.0]).unwrap()
    );

    let mut est_f32 = DensitySketch::<f32>::new(8, 2).unwrap();
    for i in 0..120 {
        est_f32
            .update(&[i as f32 * 0.25, -(i as f32 * 0.25)])
            .unwrap();
    }
    let bytes_f32 = est_f32.serialize();
    let decoded_f32 = DensitySketch::<f32>::deserialize(&bytes_f32).unwrap();
    assert!(decoded_f32.is_estimation_mode());
    assert_eq!(decoded_f32.serialize(), bytes_f32);
    assert_eq!(
        decoded_f32.estimate(&[10.0, -10.0]).unwrap(),
        est_f32.estimate(&[10.0, -10.0]).unwrap()
    );

    // Reset, round-trip empty, reuse, and round-trip again.
    est_f64.reset();
    let reset_empty_bytes = est_f64.serialize();
    let mut reset_decoded = DensitySketch::<f64>::deserialize(&reset_empty_bytes).unwrap();
    assert!(reset_decoded.is_empty());
    assert_eq!(reset_decoded.k(), 10);
    assert_eq!(reset_decoded.dim(), 2);

    reset_decoded.update(&[3.0, 4.0]).unwrap();
    let reused_bytes = reset_decoded.serialize();
    let reused_decoded = DensitySketch::<f64>::deserialize(&reused_bytes).unwrap();
    assert_eq!(reused_decoded.total_weight(), 1);
    assert_eq!(reused_decoded.num_retained(), 1);
    assert_eq!(reused_decoded.estimate(&[3.0, 4.0]).unwrap(), Some(1.0));
    assert_eq!(reused_decoded.serialize(), reused_bytes);

    // Custom kernel round-trip via deserialize_with_kernel.
    let kernel = CustomScaleKernel { scale: 0.5 };
    let mut custom = DensitySketch::<f64, _>::with_kernel(8, 2, kernel.clone()).unwrap();
    for i in 0..40 {
        custom.update(&[i as f64 * 0.1, -(i as f64 * 0.1)]).unwrap();
    }
    let custom_bytes = custom.serialize();
    let custom_restored =
        DensitySketch::<f64, _>::deserialize_with_kernel(&custom_bytes, kernel.clone()).unwrap();
    assert_eq!(custom_restored.kernel(), &kernel);
    assert_eq!(custom_restored.serialize(), custom_bytes);
    assert_eq!(
        custom_restored.estimate(&[1.0, -1.0]).unwrap(),
        custom.estimate(&[1.0, -1.0]).unwrap()
    );
}

#[test]
fn every_truncated_prefix_is_rejected() {
    for hex in [CPP_F64_EMPTY, CPP_F64_EXACT_N10, CPP_F64_EST_N1031] {
        let bytes = decode_hex(hex);
        for end in 0..bytes.len() {
            assert_invalid_f64(&bytes[..end]);
        }
        assert!(DensitySketch::<f64>::deserialize(&bytes).is_ok());
    }

    for hex in [CPP_F32_EMPTY, CPP_F32_EXACT_N10, CPP_F32_EST_N1031] {
        let bytes = decode_hex(hex);
        for end in 0..bytes.len() {
            assert_invalid_f32(&bytes[..end]);
        }
        assert!(DensitySketch::<f32>::deserialize(&bytes).is_ok());
    }
}

#[test]
fn malformed_headers_and_parameters_are_rejected() {
    let empty = decode_hex(CPP_F64_EMPTY);
    let exact = decode_hex(CPP_F64_EXACT_N10);
    let est = decode_hex(CPP_F64_EST_N1031);

    // Wrong family ID (offset 2).
    for base in [&empty, &exact, &est] {
        for bad_family in [0_u8, 18, 20, 255] {
            let mut corrupted = base.clone();
            corrupted[2] = bad_family;
            assert_invalid_f64(&corrupted);
        }
    }

    // Wrong serial version (offset 1).
    for base in [&empty, &exact, &est] {
        for bad_ver in [0_u8, 2, 255] {
            let mut corrupted = base.clone();
            corrupted[1] = bad_ver;
            assert_invalid_f64(&corrupted);
        }
    }

    // Unknown flag bits (offset 3).
    for bad_empty_flags in [0x00_u8, 0x05, 0x06, 0x0c, 0x84, 0xff] {
        let mut corrupted = empty.clone();
        corrupted[3] = bad_empty_flags;
        assert_invalid_f64(&corrupted);
    }
    for base in [&exact, &est] {
        for bad_non_empty_flags in [0x01_u8, 0x02, 0x04, 0x08, 0x80, 0xff] {
            let mut corrupted = base.clone();
            corrupted[3] = bad_non_empty_flags;
            assert_invalid_f64(&corrupted);
        }
    }

    // Preamble ints mismatch (offset 0).
    for bad_empty_pre in [0_u8, 2, 4, 6] {
        let mut corrupted = empty.clone();
        corrupted[0] = bad_empty_pre;
        assert_invalid_f64(&corrupted);
    }
    for base in [&exact, &est] {
        for bad_non_empty_pre in [0_u8, 2, 3, 5, 7] {
            let mut corrupted = base.clone();
            corrupted[0] = bad_non_empty_pre;
            assert_invalid_f64(&corrupted);
        }
    }

    // k < 2 (offsets 4..6).
    for base in [&empty, &exact] {
        for bad_k in [0_u16, 1] {
            let mut corrupted = base.clone();
            corrupted[4..6].copy_from_slice(&bad_k.to_le_bytes());
            assert_invalid_f64(&corrupted);
        }
    }

    // dim == 0 (offsets 8..12) and huge dim (u32::MAX) on non-empty.
    for base in [&empty, &exact] {
        let mut corrupted = base.clone();
        corrupted[8..12].copy_from_slice(&0_u32.to_le_bytes());
        assert_invalid_f64(&corrupted);
    }
    {
        let mut huge_dim = exact.clone();
        huge_dim[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_invalid_f64(&huge_dim);
    }
}

#[test]
fn malformed_counts_levels_payloads_and_trailing_bytes_are_rejected() {
    let empty = decode_hex(CPP_F64_EMPTY);
    let single = decode_hex(CPP_F64_SINGLE);
    let exact = decode_hex(CPP_F64_EXACT_N10);
    let exact_f32 = decode_hex(CPP_F32_EXACT_N10);
    let est = decode_hex(CPP_F64_EST_N1031);

    // Non-empty with n == 0 (offsets 16..24).
    {
        let mut zero_n = single.clone();
        zero_n[16..24].copy_from_slice(&0_u64.to_le_bytes());
        assert_invalid_f64(&zero_n);
    }

    // Non-empty with num_retained == 0 (offsets 12..16), both with n > 0 and n == 0.
    {
        let mut zero_retained = single.clone();
        zero_retained[12..16].copy_from_slice(&0_u32.to_le_bytes());
        assert_invalid_f64(&zero_retained);

        zero_retained[16..24].copy_from_slice(&0_u64.to_le_bytes());
        assert_invalid_f64(&zero_retained);
    }

    // num_retained > n.
    {
        let mut more_retained_than_n = exact.clone();
        more_retained_than_n[16..24].copy_from_slice(&9_u64.to_le_bytes());
        assert_invalid_f64(&more_retained_than_n);

        let mut huge_retained = exact.clone();
        huge_retained[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        huge_retained[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_invalid_f64(&huge_retained);
    }

    // levels > 64: 64 empty levels followed by 1 non-empty level (65 levels total).
    {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[6, 1, 19, 0]);
        bytes.extend_from_slice(&10_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // dim = 1
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // num_retained = 1
        bytes.extend_from_slice(&2_u64.to_le_bytes()); // n = 2
        for _ in 0..64 {
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&1.0_f64.to_le_bytes());
        assert_invalid_f64(&bytes);
    }

    // level_len > remaining (declared num_retained=2, level_len=3 with 3 points present).
    {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[6, 1, 19, 0]);
        bytes.extend_from_slice(&10_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // dim = 1
        bytes.extend_from_slice(&2_u32.to_le_bytes()); // num_retained = 2
        bytes.extend_from_slice(&4_u64.to_le_bytes()); // n = 4
        bytes.extend_from_slice(&3_u32.to_le_bytes()); // level_len = 3 > remaining(2)
        bytes.extend_from_slice(&1.0_f64.to_le_bytes());
        bytes.extend_from_slice(&2.0_f64.to_le_bytes());
        bytes.extend_from_slice(&3.0_f64.to_le_bytes());
        assert_invalid_f64(&bytes);
    }

    // Truncated/short payload for declared num_retained or level_len (including u32::MAX).
    {
        let mut short_for_retained = single.clone();
        short_for_retained[12..16].copy_from_slice(&100_u32.to_le_bytes());
        short_for_retained[16..24].copy_from_slice(&100_u64.to_le_bytes());
        assert_invalid_f64(&short_for_retained);

        let mut short_for_level_len = single.clone();
        short_for_level_len[24..28].copy_from_slice(&50_u32.to_le_bytes());
        assert_invalid_f64(&short_for_level_len);

        let mut huge_level_len = single.clone();
        huge_level_len[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_invalid_f64(&huge_level_len);
    }

    // Non-finite coordinates (NaN, +inf, -inf) for f64 and f32.
    for bad_f64 in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7ff8_0000_0000_0001),
    ] {
        let mut corrupted = single.clone();
        // Second coordinate of the first point in single starts at 28 + 8 = 36.
        corrupted[36..44].copy_from_slice(&bad_f64.to_le_bytes());
        assert_invalid_f64(&corrupted);
    }
    for bad_f32 in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let mut corrupted = exact_f32.clone();
        // First coordinate of the first point starts at 28.
        corrupted[28..32].copy_from_slice(&bad_f32.to_le_bytes());
        assert_invalid_f32(&corrupted);
    }

    // Trailing extra bytes (empty and non-empty).
    for base in [&empty, &single, &exact, &est] {
        for trailing_len in 1..=3 {
            let mut unaligned = base.clone();
            unaligned.resize(unaligned.len() + trailing_len, 0x00);
            assert_invalid_f64(&unaligned);
        }

        let mut with_many = base.clone();
        with_many.extend_from_slice(&[0xaa; 8]);
        assert_invalid_f64(&with_many);
    }

    // Non-empty trailing non-zero u32 (level_len = 1 after all retained points are consumed).
    {
        let mut nonzero_trailing_u32 = est.clone();
        nonzero_trailing_u32.extend_from_slice(&1_u32.to_le_bytes());
        assert_invalid_f64(&nonzero_trailing_u32);
    }

    // Trailing empty levels pushing total levels past 64 must be rejected.
    {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[6, 1, 19, 0]);
        bytes.extend_from_slice(&2_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // dim = 1
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // num_retained = 1
        bytes.extend_from_slice(&3_u64.to_le_bytes()); // n = 3
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&50.0_f64.to_le_bytes());
        for _ in 0..64 {
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }
        assert_invalid_f64(&bytes);
    }
}

#[test]
fn cpp_trailing_empty_top_level_and_round_trip_single_level_compacted_images() {
    // Generated from datasketches-cpp commit 70b86faa63402d7f62f5a6b1b05109d5fd1f2186 with
    // k=2, dim=1, random_bit seed 0, and inputs [0, 40, 80]. C++ initially emits a trailing
    // empty top level (40B f64 / 36B f32); after one C++ deserialize -> serialize round-trip, C++
    // drops the empty level and emits a 1-level image with num_retained (1) < n (3) (36B / 32B).
    const CPP_F64_WITH_EMPTY_TOP: &str =
        "06011300020000000100000001000000030000000000000001000000000000000000544000000000";
    const CPP_F64_CPP_ROUNDTRIP_1_LEVEL: &str =
        "060113000200000001000000010000000300000000000000010000000000000000005440";
    const CPP_F32_WITH_EMPTY_TOP: &str =
        "060113000200000001000000010000000300000000000000010000000000a04200000000";
    const CPP_F32_CPP_ROUNDTRIP_1_LEVEL: &str =
        "060113000200000001000000010000000300000000000000010000000000a042";

    for hex in [CPP_F64_WITH_EMPTY_TOP, CPP_F64_CPP_ROUNDTRIP_1_LEVEL] {
        let bytes = decode_hex(hex);
        let mut sketch = DensitySketch::<f64>::deserialize(&bytes).unwrap();
        assert!(!sketch.is_empty());
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.k(), 2);
        assert_eq!(sketch.dim(), 1);
        assert_eq!(sketch.total_weight(), 3);
        assert_eq!(sketch.num_retained(), 1);
        let retained: Vec<(&[f64], u64)> = sketch.iter().collect();
        assert_eq!(retained, vec![(&[80.0][..], 1)]);
        assert_eq!(sketch.estimate(&[80.0]).unwrap(), Some(1.0 / 3.0));
        assert_eq!(sketch.serialize(), bytes);

        // Further updates and merges preserve estimation mode and invariants.
        sketch.update(&[120.0]).unwrap();
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.total_weight(), 4);
        let mut other = DensitySketch::<f64>::new(2, 1).unwrap();
        other.update(&[160.0]).unwrap();
        sketch.merge(&other).unwrap();
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.total_weight(), 5);
        assert!(sketch.num_retained() > 0);
    }

    for hex in [CPP_F32_WITH_EMPTY_TOP, CPP_F32_CPP_ROUNDTRIP_1_LEVEL] {
        let bytes = decode_hex(hex);
        let sketch = DensitySketch::<f32>::deserialize(&bytes).unwrap();
        assert!(!sketch.is_empty());
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.k(), 2);
        assert_eq!(sketch.dim(), 1);
        assert_eq!(sketch.total_weight(), 3);
        assert_eq!(sketch.num_retained(), 1);
        let retained: Vec<(&[f32], u64)> = sketch.iter().collect();
        assert_eq!(retained, vec![(&[80.0_f32][..], 1)]);
        assert_eq!(sketch.estimate(&[80.0]).unwrap(), Some(1.0 / 3.0));
        assert_eq!(sketch.serialize(), bytes);
    }
}

#[test]
fn rust_double_snapshots_round_trip() {
    for n in [0_u64, 1, 10, 100, 1000, 10_000] {
        let filename = format!("density_double_n{n}_rust.sk");
        let path = serialization_test_data("rust_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = DensitySketch::<f64>::deserialize(&bytes).unwrap();

        assert_eq!(sketch.k(), 10, "{filename}");
        assert_eq!(sketch.dim(), 3, "{filename}");
        assert_eq!(sketch.total_weight(), n, "{filename}");
        assert_eq!(sketch.is_empty(), n == 0, "{filename}");
        assert_eq!(sketch.is_estimation_mode(), n >= 30, "{filename}");
        let retained_weight: u64 = sketch.iter().map(|(_, weight)| weight).sum();
        if n < 30 {
            assert_eq!(retained_weight, n, "{filename}");
        } else {
            assert!(retained_weight > 0, "{filename}");
        }

        match n {
            0 => {
                assert_eq!(sketch.num_retained(), 0, "{filename}");
                assert_eq!(sketch.estimate(&[0.0, 0.0, 0.0]).unwrap(), None);
                assert_eq!(bytes, decode_hex(CPP_F64_EMPTY), "{filename}");
            }
            10 => {
                assert_eq!(bytes, decode_hex(CPP_F64_EXACT_N10), "{filename}");
            }
            _ => {}
        }

        if n > 0 {
            assert!(sketch.num_retained() > 0, "{filename}");
            let (first_pt, _) = sketch.iter().next().unwrap();
            let (last_pt, _) = sketch.iter().last().unwrap();
            for pt in [first_pt, last_pt] {
                let est = sketch.estimate(pt).unwrap().unwrap();
                assert!(
                    est.is_finite() && est > 0.0 && est <= 1.0,
                    "{filename}: invalid density estimate {est} at retained point {pt:?}"
                );
            }
            let last_x = (n - 1) as f64;
            for sample in [[0.0, 0.0, 0.0], [last_x, last_x.sqrt(), -last_x]] {
                let est = sketch.estimate(&sample).unwrap().unwrap();
                assert!(
                    est.is_finite() && (0.0..=1.0).contains(&est),
                    "{filename}: invalid density estimate {est} at {sample:?}"
                );
                if n <= 10 {
                    assert!(est > 0.0, "{filename}: expected positive exact estimate");
                }
            }
        }

        assert_eq!(sketch.serialize(), bytes, "roundtrip: {filename}");
    }
}

#[test]
fn rust_float_snapshots_round_trip() {
    for n in [0_u64, 1, 10, 100, 1000, 10_000] {
        let filename = format!("density_float_n{n}_rust.sk");
        let path = serialization_test_data("rust_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = DensitySketch::<f32>::deserialize(&bytes).unwrap();

        assert_eq!(sketch.k(), 10, "{filename}");
        assert_eq!(sketch.dim(), 3, "{filename}");
        assert_eq!(sketch.total_weight(), n, "{filename}");
        assert_eq!(sketch.is_empty(), n == 0, "{filename}");
        assert_eq!(sketch.is_estimation_mode(), n >= 30, "{filename}");
        let retained_weight: u64 = sketch.iter().map(|(_, weight)| weight).sum();
        if n < 30 {
            assert_eq!(retained_weight, n, "{filename}");
        } else {
            assert!(retained_weight > 0, "{filename}");
        }

        match n {
            0 => {
                assert_eq!(sketch.num_retained(), 0, "{filename}");
                assert_eq!(sketch.estimate(&[0.0, 0.0, 0.0]).unwrap(), None);
                assert_eq!(bytes, decode_hex(CPP_F32_EMPTY), "{filename}");
            }
            10 => {
                assert_eq!(bytes, decode_hex(CPP_F32_EXACT_N10), "{filename}");
            }
            _ => {}
        }

        if n > 0 {
            assert!(sketch.num_retained() > 0, "{filename}");
            let (first_pt, _) = sketch.iter().next().unwrap();
            let (last_pt, _) = sketch.iter().last().unwrap();
            for pt in [first_pt, last_pt] {
                let est = sketch.estimate(pt).unwrap().unwrap();
                assert!(
                    est.is_finite() && est > 0.0 && est <= 1.0,
                    "{filename}: invalid density estimate {est} at retained point {pt:?}"
                );
            }
            let last_x = (n - 1) as f32;
            for sample in [[0.0, 0.0, 0.0], [last_x, last_x.sqrt(), -last_x]] {
                let est = sketch.estimate(&sample).unwrap().unwrap();
                assert!(
                    est.is_finite() && (0.0..=1.0).contains(&est),
                    "{filename}: invalid density estimate {est} at {sample:?}"
                );
                if n <= 10 {
                    assert!(est > 0.0, "{filename}: expected positive exact estimate");
                }
            }
        }

        assert_eq!(sketch.serialize(), bytes, "roundtrip: {filename}");
    }
}
