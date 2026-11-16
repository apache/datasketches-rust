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

use std::f64::consts::LN_2;

use crate::common::NumStdDev;

const ICON_ERROR_CONSTANT: f64 = LN_2;

static ICON_LOW_SIDE_DATA: [u16; 33] = [
    //1,    2,    3,   kappa
    //                 lgK num trials
    6037, 5720, 5328, // 4 1000000
    6411, 6262, 5682, // 5 1000000
    6724, 6403, 6127, // 6 1000000
    6665, 6411, 6208, // 7 1000000
    6959, 6525, 6427, // 8 1000000
    6892, 6665, 6619, // 9 1000000
    6792, 6752, 6690, // 10 1000000
    6899, 6818, 6708, // 11 1000000
    6871, 6845, 6812, // 12 1046369
    6909, 6861, 6828, // 13 1043411
    6919, 6897, 6842, // 14 1000297
];

static ICON_HIGH_SIDE_DATA: [u16; 33] = [
    //1,    2,    3,   kappa
    //                 lgK num trials
    8031, 8559, 9309, // 4 1000000
    7084, 7959, 8660, // 5 1000000
    7141, 7514, 7876, // 6 1000000
    7458, 7430, 7572, // 7 1000000
    6892, 7141, 7497, // 8 1000000
    6889, 7132, 7290, // 9 1000000
    7075, 7118, 7185, // 10 1000000
    7040, 7047, 7085, // 11 1000000
    6993, 7019, 7053, // 12 1046369
    6953, 7001, 6983, // 13 1043411
    6944, 6966, 7004, // 14 1000297
];

#[allow(clippy::excessive_precision)]
const HIP_ERROR_CONSTANT: f64 = 0.588705011257737332; // (LN_2 / 2.0).sqrt()

static HIP_LOW_SIDE_DATA: [u16; 33] = [
    //1,    2,    3,   kappa
    //                 lgK num trials
    5871, 5247, 4826, // 4 1000000
    5877, 5403, 5070, // 5 1000000
    5873, 5533, 5304, // 6 1000000
    5878, 5632, 5464, // 7 1000000
    5874, 5690, 5564, // 8 1000000
    5880, 5745, 5619, // 9 1000000
    5875, 5784, 5701, // 10 1000000
    5866, 5789, 5742, // 11 1000000
    5869, 5827, 5784, // 12 1046369
    5876, 5860, 5827, // 13 1043411
    5881, 5853, 5842, // 14 1000297
];

static HIP_HIGH_SIDE_DATA: [u16; 33] = [
    //1,    2,    3,   kappa
    //                 lgK num trials
    5855, 6688, 7391, // 4 1000000
    5886, 6444, 6923, // 5 1000000
    5885, 6254, 6594, // 6 1000000
    5889, 6134, 6326, // 7 1000000
    5900, 6072, 6203, // 8 1000000
    5875, 6005, 6089, // 9 1000000
    5871, 5980, 6040, // 10 1000000
    5889, 5941, 6015, // 11 1000000
    5871, 5926, 5973, // 12 1046369
    5866, 5901, 5915, // 13 1043411
    5880, 5914, 5953, // 14 1000297
];

pub fn estimate(merge_flag: bool, hip_est_accum: f64, lg_k: u8, num_coupons: u32) -> f64 {
    if !merge_flag {
        hip_est_accum
    } else {
        icon_estimate(lg_k, num_coupons)
    }
}

pub fn lower_bound(
    merge_flag: bool,
    hip_est_accum: f64,
    lg_k: u8,
    num_coupons: u32,
    kappa: NumStdDev,
) -> f64 {
    if !merge_flag {
        hip_confidence_lb(lg_k, num_coupons, hip_est_accum, kappa)
    } else {
        icon_confidence_lb(lg_k, num_coupons, kappa)
    }
}

pub fn upper_bound(
    merge_flag: bool,
    hip_est_accum: f64,
    lg_k: u8,
    num_coupons: u32,
    kappa: NumStdDev,
) -> f64 {
    if !merge_flag {
        hip_confidence_ub(lg_k, num_coupons, hip_est_accum, kappa)
    } else {
        icon_confidence_ub(lg_k, num_coupons, kappa)
    }
}

fn icon_confidence_lb(lg_k: u8, num_coupons: u32, kappa: NumStdDev) -> f64 {
    if num_coupons == 0 {
        return 0.0;
    }

    let k = (1 << lg_k) as f64;
    let kappa = kappa.as_u8();

    let mut x = ICON_ERROR_CONSTANT;
    if lg_k <= 14 {
        let idx = (3 * (lg_k - 4) + (kappa - 1)) as usize;
        x = (ICON_HIGH_SIDE_DATA[idx] as f64) / 10000.0;
    }
    let rel = x / k.sqrt();
    let eps = (kappa as f64) * rel;
    let est = icon_estimate(lg_k, num_coupons);
    let result = est / (1.0 + eps);
    if result < (num_coupons as f64) {
        num_coupons as f64
    } else {
        result
    }
}

fn icon_confidence_ub(lg_k: u8, num_coupons: u32, kappa: NumStdDev) -> f64 {
    if num_coupons == 0 {
        return 0.0;
    }

    let k = (1 << lg_k) as f64;
    let kappa = kappa.as_u8();

    let mut x = ICON_ERROR_CONSTANT;
    if lg_k <= 14 {
        let idx = (3 * (lg_k - 4) + (kappa - 1)) as usize;
        x = (ICON_LOW_SIDE_DATA[idx] as f64) / 10000.0;
    }
    let rel = x / k.sqrt();
    let eps = (kappa as f64) * rel;
    let est = icon_estimate(lg_k, num_coupons);
    let result = est / (1.0 - eps);
    result.ceil() // slight widening of interval to be conservative
}

fn hip_confidence_lb(lg_k: u8, num_coupons: u32, hip_est_accum: f64, kappa: NumStdDev) -> f64 {
    if num_coupons == 0 {
        return 0.0;
    }

    let k = (1 << lg_k) as f64;
    let kappa = kappa.as_u8();

    let mut x = HIP_ERROR_CONSTANT;
    if lg_k <= 14 {
        let idx = (3 * (lg_k - 4) + (kappa - 1)) as usize;
        x = (HIP_HIGH_SIDE_DATA[idx] as f64) / 10000.0;
    }
    let rel = x / k.sqrt();
    let eps = (kappa as f64) * rel;
    let result = hip_est_accum / (1.0 + eps);
    if result < (num_coupons as f64) {
        num_coupons as f64
    } else {
        result
    }
}

fn hip_confidence_ub(lg_k: u8, num_coupons: u32, hip_est_accum: f64, kappa: NumStdDev) -> f64 {
    if num_coupons == 0 {
        return 0.0;
    }

    let k = (1 << lg_k) as f64;
    let kappa = kappa.as_u8();

    let mut x = HIP_ERROR_CONSTANT;
    if lg_k <= 14 {
        let idx = (3 * (lg_k - 4) + (kappa - 1)) as usize;
        x = (HIP_LOW_SIDE_DATA[idx] as f64) / 10000.0;
    }
    let rel = x / k.sqrt();
    let eps = (kappa as f64) * rel;
    let result = hip_est_accum / (1.0 - eps);
    result.ceil() // widening for coverage
}

// The ICON estimator for FM85 sketches is defined by the arXiv paper.
//
// The current file provides exact and approximate implementations of this estimator. The exact
// version works for any value of K, but is quite slow.
//
// The much faster approximate version works for K values that are powers of two ranging from 2^4
// to 2^32.
//
// At a high-level, this approximation can be described as using an exponential approximation when
// C > K * (5.6 or 5.7), while smaller values of C are handled by a degree-19 polynomial
// approximation of a pre-conditioned version of the true ICON mapping from C to N_hat.
//
// This file also provides a validation procedure that compares its approximate and exact
// implementations of the FM85 ICON estimator.

const ICON_MIN_LOG_K: usize = 4;
const ICON_MAX_LOG_K: usize = 26;
const ICON_POLYNOMIAL_DEGREE: usize = 19;
const ICON_POLYNOMIAL_NUM_COEFFICIENTS: usize = 1 + ICON_POLYNOMIAL_DEGREE;
const ICON_TABLE_SIZE: usize =
    ICON_POLYNOMIAL_NUM_COEFFICIENTS * (1 + (ICON_MAX_LOG_K - ICON_MIN_LOG_K));

#[rustfmt::skip]
#[allow(clippy::excessive_precision)]
static ICON_POLYNOMIAL_COEFFICIENTS: [f64; ICON_TABLE_SIZE] = [
    // log K = 4
    0.9895027971889700513, 0.3319496644645180128, 0.1242818722715769986, -0.03324149686026930256, -0.2985637298081619817,
    1.366555923595830002, -4.705499366260569971, 11.61506432505530029, -21.11254986175579873, 28.89421695078809904,
    -30.1383659011730991, 24.11946778830730054, -14.83391445199539938, 6.983088767267210173, -2.48964120264876998,
    0.6593243603602499947, -0.125493534558034997, 0.01620971672896159843, -0.001271267679036929953, 4.567178653294529745e-05,

    // log K = 5
    0.9947713741300230339, 0.3326559581620939787, 0.1250050661634889981, -0.04130073804472530336, -0.2584095537451129854,
    1.218050389433120051, -4.319106696095399656, 10.87175052045090062, -20.0184979022142997, 27.63210188163320069,
    -28.97950009664030091, 23.26740804691930009, -14.33375703270860058, 6.751281271241110105, -2.406363094133439962,
    0.6367414734718820357, -0.1210468076141379967, 0.01561196698118279963, -0.001222335432128580056, 4.383502970318410206e-05,

    // log K = 6
    0.9973904854982870161, 0.3330148852217920119, 0.125251536589509993, -0.04434075124043219962, -0.2436238890691720116,
    1.163293254754570016, -4.177758779777369647, 10.60301981340099964, -19.6274507428828997, 27.18420839597660077,
    -28.56827214174580121, 22.96268674086600114, -14.15234202220280046, 6.665700662642549901, -2.375043356720739851,
    0.6280993991240929608, -0.119319019358031006, 0.01537674055733759954, -0.001202881695730769916, 4.309894633186929849e-05,

    // log K = 7
    0.9986963310058679655, 0.3331956705633329907, 0.125337696770523005, -0.04546817338088020299, -0.2386752211125199863,
    1.145927328111949972, -4.135694445582720036, 10.52805060502839929, -19.52408322548339825, 27.06921653903929936,
    -28.46207532143190022, 22.88083524357429965, -14.10057147392659971, 6.63958754983273991, -2.364865219283200037,
    0.6251341806425250169, -0.1186991327450530043, 0.0152892726403408008, -0.001195439764873199896, 4.281098416794090072e-05,

    // log K = 8
    0.999348600452531044, 0.3332480372393080148, 0.126666900963325002, -0.06495714694254159371, -0.08376282050638980681,
    0.3760158094643630267, -1.568204791601850001, 4.483117719555970382, -9.119180124379150598, 13.65799293358900002,
    -15.3100211234349004, 12.97546344654869976, -8.351661538536939489, 4.075022612435580172, -1.49387015887069996,
    0.4040976870253379927, -0.07813232681879349328, 0.01020545649538820085, -0.0008063279210812720381, 2.909334976414100078e-05,

    // log K = 9
    0.9996743787297059924, 0.3332925779481850093, 0.1267124599259649986, -0.06550452970936600228, -0.08191738117533520214,
    0.3773034458363569987, -1.604679509609959975, 4.636761898691969641, -9.487348609558699408, 14.25164235443030059,
    -15.99674955529870068, 13.56353219046370029, -8.730194904342459594, 4.259010067932120336, -1.56106689792022002,
    0.4222540912786589828, -0.08165296504921559784, 0.01066878484925220041, -0.0008433887618256910015, 3.045339724886519912e-05,

    // log K = 10
    0.999837191783945034, 0.3333142252339619804, 0.1267759538087240012, -0.06631005632753710077, -0.07692759158286699428,
    0.3568943956395980166, -1.546598721379510044, 4.51595019978557044, -9.298431968763770428, 14.02586858080080034,
    -15.78858959520439953, 13.41484931677589998, -8.647958125130809748, 4.22398017468472009, -1.549708891200570093,
    0.419507410264540026, -0.08117411611046250475, 0.01061202286184199928, -0.000839300527596772007, 3.03185874520205985e-05,

    // log K = 11
    0.9999186020796150265, 0.3333249054574359826, 0.126791713589799987, -0.06662487271699729652, -0.07335552427910230211,
    0.3316370184815959909, -1.434143797561290068, 4.180260309967409604, -8.593906870708760692, 12.95088874800289958,
    -14.56876092520539956, 12.37074367531410068, -7.969152075707960137, 3.888774396648960074, -1.424923326506990051,
    0.385084561785229984, -0.07435541911616409816, 0.009695363567476529554, -0.0007644375960047160388, 2.75156194717188011e-05,

    // log K = 12
    0.9999592955649559967, 0.3333310560725140093, 0.1267379744020450116, -0.06524495415766619344, -0.08854031542298740343,
    0.4244320628874230228, -1.794077789033230008, 5.133875262768450298, -10.40149374917120007, 15.47808115629240078,
    -17.2272296137545986, 14.5002173676463002, -9.274819801602760094, 4.500782540026570189, -1.642359389030050076,
    0.442596113445525019, -0.0853226219238850947, 0.01111969379054169975, -0.0008771614088006969611, 3.161668519459719752e-05,

    // log K = 13
    0.9999796468102559732, 0.3333336602394039727, 0.126728089053198989, -0.06503798598282370391, -0.09050261023823169548,
    0.4350609244189960201, -1.831274835815670077, 5.223387516985289913, -10.55574395269979959, 15.67359470222429962,
    -17.41263416341029924, 14.63297400889229927, -9.346752431221359458, 4.530124905188380069, -1.651245566462089975,
    0.444542549250713015, -0.08561720963336499901, 0.01114805146185449992, -0.0008786251203363140043, 3.16416341644572998e-05,

    // log K = 14
    0.9999898187060970445, 0.3333362579300819806, 0.1266984078369459976, -0.06464561179765909715, -0.09343280886228019777,
    0.4490702549264070087, -1.878087608052450008, 5.338004322057390283, -10.76690603590630069, 15.97069195083200022,
    -17.73440379943459888, 14.90212518309260048, -9.520506013770420495, 4.616238931978830173, -1.68364817877918993,
    0.4536194960681350086, -0.087448605434800597, 0.01139929991331390009, -0.0008995891451622229631, 3.244407259782900338e-05,

    // log K = 15
    0.9999949072549390028, 0.3333376334705290267, 0.126665364358402005, -0.06411790034705669439, -0.09776009134670660128,
    0.4704691112248470253, -1.948021675295769972, 5.497760972696490001, -11.03165645315390009, 16.29703330781000048,
    -18.03851029448010124, 15.11836776139680083, -9.638205179917429533, 4.665122328753120051, -1.698980686525759953,
    0.4571799506245269873, -0.08804011353783609828, 0.01146553155965330043, -0.0009040455800659569869, 3.257931866957050274e-05,

    // log K = 16
    0.9999974544793589493, 0.3333381337614599871, 0.1266524862971120102, -0.06391676499117690535, -0.09929616211306059592,
    0.4771390820378790254, -1.965762451227349938, 5.526802350376460282, -11.05703067024660058, 16.29535848023060041,
    -18.00114005075790047, 15.06214012231560062, -9.58874727382628933, 4.63537541652793017, -1.686222848555620102,
    0.4532602373715179933, -0.08719448925964939923, 0.01134365425717459921, -0.0008934965241274289835, 3.216436244471380105e-05,

    // log K = 17
    0.9999987278278800185, 0.3333383411464330148, 0.126642761751724009, -0.06371042959073920653, -0.1013564516034080043,
    0.4891311195679299839, -2.010971712051409899, 5.644390807952309963, -11.27697253921500042, 16.59957157207080058,
    -18.31808338317799922, 15.31363518393730061, -9.741451446816620674, 4.706207545519429658, -1.711102469010010063,
    0.4597587341089349744, -0.08841670767182820134, 0.01149999225097850068, -0.0009056651366963050422, 3.259910736274500059e-05,

    // log K = 18
    0.9999993637727100371, 0.3333385511608860097, 0.1266341580529160016, -0.06353272828164230335, -0.103139962850642003,
    0.4996216017206500104, -2.05099128585287982, 5.749874086531799655, -11.47727638570349917, 16.88141587810320132,
    -18.61744656177490143, 15.55634230427719977, -9.892350736128680211, 4.778033520984200422, -1.737045483861280104,
    0.4667410882683730167, -0.08977256212421590165, 0.01167940146667079994, -0.0009201381242396030127, 3.313600701586759867e-05,

    // log K = 19
    0.9999996805376010212, 0.3333372324328989778, 0.1267104737214659882, -0.06504749929326139601, -0.0882341962464350954,
    0.4131871162041140244, -1.725190703567099915, 4.900817515593920426, -9.883452720776510603, 14.6657081190816001,
    -16.29398295135089825, 13.69805011761319946, -8.753475239465899449, 4.244072374564439976, -1.547202527706629915,
    0.4164770109614310267, -0.08017596922092029565, 0.01043146101701039954, -0.00082124200571200305, 2.953319493719429935e-05,

    // log K = 20
    0.9999998390037539986, 0.3333365859956040067, 0.1267460211029839967, -0.06569456024647769843, -0.0823070353477164951,
    0.3810826463303410017, -1.611983580241109992, 4.624520077758210057, -9.397308335633589138, 14.03184981378050011,
    -15.6703191315401007, 13.22992718704790072, -8.484216393184780713, 4.125607133488029987, -1.507690650697159906,
    0.4066678517577320129, -0.07842110121777939868, 0.01021780862225150042, -0.0008054065857047439754, 2.899431830426989844e-05,

    // log K = 21
    0.9999999207001479817, 0.3333384953015239849, 0.1266331480396669928, -0.06345750166298599892, -0.1042341210992499961,
    0.5077112908497130039, -2.087398133609810191, 5.858842546192500222, -11.70620319777190055, 17.23103975433669888,
    -19.01462552846669851, 15.89674059836560005, -10.11395134034419918, 4.88760796465891989, -1.777886770904629987,
    0.4780200178339499839, -0.09200895321782050218, 0.01198029553244219989, -0.0009447283875782100165, 3.405716775824710232e-05,

    // log K = 22
    0.9999999606908690497, 0.3333383929524300071, 0.1266456445096819927, -0.06373504294081690225, -0.1012834291081849969,
    0.4893810690172959998, -2.01391428223606983, 5.656430437473649597, -11.3067201537791, 16.64980594135310099,
    -18.3792355790383013, 15.36879753115040081, -9.778831246425049528, 4.725308061988969577, -1.718423596500280093,
    0.4618308177809870019, -0.08883675060799739454, 0.01155766944804260087, -0.0009104695617243750358, 3.278237729674439666e-05,

    // log K = 23
    0.9999999794683379628, 0.3333386441751680085, 0.1266463995182049995, -0.06376031920455070556, -0.1010799540803130059,
    0.488540137426137, -2.012048323537570127, 5.654949475342659682, -11.31023240892979942, 16.66334675284959843,
    -18.40241452866079896, 15.39443572867130072, -9.798844412838670692, 4.736683907539640082, -1.723168363744929987,
    0.463270349018644001, -0.08914619066708899531, 0.01160235936257320022, -0.0009143600818183229709, 3.293669304679140117e-05,

    // log K = 24
    0.9999999911469820146, 0.3333376076934529975, 0.1266944349940530012, -0.06470524278387919381, -0.09189342220283110152,
    0.4359182372694809793, -1.815980282951169977, 5.149474056470340066, -10.37086570678100017, 15.36962686758569951,
    -17.05756384717849983, 14.32755177515199918, -9.149944050025640152, 4.434601894497260055, -1.616478926806520056,
    0.4351979157055039793, -0.08381768225272340223, 0.01091321820476520016, -0.0008600264403629039739, 3.09667800347144002e-05,

    // log K = 25
    0.9999999968592140354, 0.3333379164881000167, 0.1266782495827009913, -0.06434163088961859789, -0.09575258124988890451,
    0.4597843575354370049, -1.911374431241559924, 5.411856661251520428, -10.88850084646090011, 16.12298941380269923,
    -17.88172178487259956, 15.01301780636859995, -9.585542896142529301, 4.645811872761620442, -1.693952293156189892,
    0.4563143308861309921, -0.08795976148455289523, 0.01146560428011200033, -0.0009048442931930629528, 3.26358391497329992e-05,

    // log K = 26
    0.9999999970700530483, 0.333338329556315982, 0.126644753076394001, -0.06372365346512399997, -0.1012760856945769949,
    0.4886852278576360176, -2.009005418394389952, 5.638119224137019714, -11.26276715335160006, 16.57640024218650154,
    -18.29035093605569884, 15.28892246224570073, -9.724916375991760731, 4.6978877652334603, -1.707974125916829955,
    0.4588937864564729963, -0.08824617586088029375, 0.01147732114826570046, -0.00090384524860747295, 3.253252703695579795e-05,
];

fn evaluate_polynomial(coefficients: &[f64], start: usize, num: usize, x: f64) -> f64 {
    let end = start + num - 1;
    let mut total = coefficients[end];
    for i in (start..end).rev() {
        total *= x;
        total += coefficients[i];
    }
    total
}

fn icon_exponential_approximation(k: f64, c: f64) -> f64 {
    0.7940236163830469 * k * 2f64.powf(c / k)
}

fn icon_estimate(lg_k: u8, num_coupons: u32) -> f64 {
    let lg_k = lg_k as usize;
    assert!(
        (ICON_MIN_LOG_K..=ICON_MAX_LOG_K).contains(&lg_k),
        "lg_k out of range; got {lg_k}",
    );

    match num_coupons {
        0 => return 0.0,
        1 => return 1.0,
        _ => {}
    }

    let k = (1 << lg_k) as f64;
    let c = num_coupons as f64;

    // Differing thresholds ensure that the approximated estimator is monotonically increasing.
    let threshold_factor = if lg_k < 14 { 5.7 } else { 5.6 };
    if c > threshold_factor * k {
        return icon_exponential_approximation(k, c);
    }

    let factor = evaluate_polynomial(
        &ICON_POLYNOMIAL_COEFFICIENTS,
        ICON_POLYNOMIAL_NUM_COEFFICIENTS * (lg_k - ICON_MIN_LOG_K),
        ICON_POLYNOMIAL_NUM_COEFFICIENTS,
        // The constant 2.0 is baked into the table ICON_POLYNOMIAL_COEFFICIENTS.
        // This factor, although somewhat arbitrary, is based on extensive characterization studies
        // and is considered a safe conservative factor.
        c / (2.0 * k),
    );
    let ratio = c / k;
    // The constant 66.774757 is baked into the table ICON_POLYNOMIAL_COEFFICIENTS.
    // This factor, although somewhat arbitrary, is based on extensive characterization studies
    // and is considered a safe conservative factor.
    let term = 1.0 + (ratio * ratio * ratio / 66.774757);
    let result = c * factor * term;
    if result >= c { result } else { c }
}

#[cfg(test)]
mod tests {
    // Reference ICON estimator, least-squares polynomial reconstruction, and confidence-table
    // verification.
    //
    // Provenance in Kevin J. Lang's original FM85 / CPC implementation:
    // 1. `iconEstimator.c` / `icon_estimator.hpp` / `IconEstimatorTest.java`: Defines the exact
    //    FM85 ICON (Inverted Coupon Number) estimator by numerically inverting the expected coupon
    //    count `E[C | N] = K * sum_{col=1..=128} (1 - (1 - 1/(K*2^col))^N)`, together with a
    //    validation sweep comparing `icon_estimate` against `exact_icon_estimate`.
    // 2. `icon_estimator.hpp` / `IconPolynomialCoefficients.java`: only the fitted degree-19
    //    coefficient matrix `ICON_POLYNOMIAL_COEFFICIENTS` and the constants `0.7940236163830469`,
    //    `2.0`, and `66.774757` were checked into C/C++/Java; the offline fitting script was not.
    //    What `icon_estimate`'s evaluation formula, the Maclaurin series of `N(C) / C`, and the
    //    error profile beyond `5.7 * K` reveal about that script is documented next to
    //    `fit_icon_polynomial_row` below, including what remains unknown.
    // 3. `cpc_confidence.hpp` / `CpcConfidence.java` (offline Monte Carlo characterization):
    //    Defines the theoretical asymptotic relative standard error constants (`ICON_ERROR_CONSTANT
    //    = ln 2` and `HIP_ERROR_CONSTANT = sqrt((ln 2) / 2)`) alongside the empirical
    //    ~1,000,000-trial quantile tables `ICON_{LOW,HIGH}_SIDE_DATA` and
    //    `HIP_{LOW,HIGH}_SIDE_DATA` measured at `N = 1000 * K` for `lg_k = 4..=14`.

    use std::array::from_fn;
    use std::f64::consts::PI;
    use std::f64::consts::SQRT_2;

    use googletest::assert_that;
    use googletest::prelude::gt;
    use googletest::prelude::lt;
    use googletest::prelude::near;

    use super::*;
    use crate::common::inv_pow2::inv_pow2;

    const EULER_MASCHERONI: f64 = 0.577_215_664_901_532_9;
    const ICON_CUBIC_PRECONDITIONER: f64 = 66.774757;

    // -------------------------------------------------------------------------
    // 1. Exact FM85 ICON estimator (`IconEstimatorTest.java` / `fm85`)
    // -------------------------------------------------------------------------

    const ICON_INVERSION_TOLERANCE: f64 = 1.0e-15;

    /// Precomputes `ln(1 - 1 / (K * 2^col))` for 1-based `col = 1..=128` at fixed `kf`, matching
    /// `qnj` in `IconEstimatorTest.java` bit-for-bit while avoiding repeated `ln_1p` evaluations
    /// inside the binary-search inner loop.
    fn precompute_col_ln1p(kf: f64) -> [f64; 128] {
        from_fn(|idx| (-inv_pow2((idx + 1) as u8) / kf).ln_1p())
    }

    /// Expected coupon count `E[C | N] = K * sum_{col=1..=128} (1 - (1 - 1/(K*2^col))^N)` in the
    /// unclipped 128-column FM85 coupon matrix (`exactCofN` in `IconEstimatorTest.java`).
    fn exact_coupon_count_with_ln1p(kf: f64, col_ln1p: &[f64; 128], nf: f64) -> f64 {
        let mut total = 0.0;
        // Iterating from 128 down to 1 accumulates tiny tail probabilities first before large
        // head probabilities, preserving full `f64` summation precision.
        for &tmp2 in col_ln1p.iter().rev() {
            total += -f64::exp_m1(nf * tmp2);
        }
        kf * total
    }

    /// Exact ICON estimate `N_hat` solving `exact_coupon_count(kf, N_hat) = target_c` by bracket
    /// doubling and binary search (`exactIconEstimator` in `IconEstimatorTest.java`).
    fn exact_icon_estimate(kf: f64, target_c: f64) -> f64 {
        if target_c == 0.0 || target_c == 1.0 {
            return target_c;
        }
        let col_ln1p = precompute_col_ln1p(kf);
        exact_icon_estimate_with_ln1p(kf, &col_ln1p, target_c)
    }

    fn exact_icon_estimate_with_ln1p(kf: f64, col_ln1p: &[f64; 128], target_c: f64) -> f64 {
        if target_c == 0.0 || target_c == 1.0 {
            return target_c;
        }
        let mut lo = target_c;
        let mut hi = 2.0 * target_c;
        while exact_coupon_count_with_ln1p(kf, col_ln1p, hi) <= target_c {
            lo = hi;
            hi *= 2.0;
        }
        for _ in 0..100 {
            let mid = lo + 0.5 * (hi - lo);
            if (hi - lo) / mid < ICON_INVERSION_TOLERANCE || mid <= lo || mid >= hi {
                return mid;
            }
            let mid_c = exact_coupon_count_with_ln1p(kf, col_ln1p, mid);
            if mid_c == target_c {
                return mid;
            } else if mid_c < target_c {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        0.5 * (lo + hi)
    }

    // -------------------------------------------------------------------------
    // 2. Reconstructed generator for `ICON_POLYNOMIAL_COEFFICIENTS`
    // -------------------------------------------------------------------------
    //
    // What the stored coefficients reveal about the offline fit, and what they do not:
    //
    // * Pre-conditioned target function: for `2 <= C <= threshold * K`, `icon_estimate` returns
    //   `C*(1+(C/K)^3/66.774757)*P(x)` with `x = C / (2 * K)`, so row `lg_k` of
    //   `ICON_POLYNOMIAL_COEFFICIENTS` is a degree-19 polynomial approximating the ratio
    //   `g_K(x)=N_exact/(C*(1+(C/K)^3/66.774757))`. Without the cubic pre-conditioner, `N_exact/C`
    //   grows by more than 8x over `C/K` in `[0, 6]` as it transitions into `0.794*K*2^(C/K)`; with
    //   it, `g_K` stays within `[0.89, 1.48]`, and the stored rows reach a maximum relative error
    //   of about `2e-7` on `[2, threshold * K]`.
    //
    //   The argument `x = C / (2 * K)` maps `C in [0, 6 * K]` onto `[0, 3]` instead of `[0, 6]`,
    //   keeping `x^19` bounded by `3^19 ≈ 1.16e9` rather than `6^19 ≈ 6.09e14`. Where the value
    //   `66.774757` comes from is not known.
    //
    // * Maclaurin series check on the leading columns: For `K -> inf` and `lambda = N / K -> 0`,
    //   `E[C] / K = sum_{j>=1} (1 - exp(-lambda * 2^-j)) = lambda - lambda^2/6 + lambda^3/42 -
    //   ...`. Series inversion yields `N / C = 1 + (C/K)/6 + 2*(C/K)^2/63 + ... = 1 + x/3 +
    //   8*x^2/63 + ...`. Dividing by `1 + 8*x^3 / 66.774757` preserves the degree-0..2 Maclaurin
    //   terms, so columns 0, 1, 2 of `ICON_POLYNOMIAL_COEFFICIENTS` should approximate `1`, `1/3`,
    //   and `8/63`. They do so only approximately: `a_0` approaches `1` like `1 - O(1 / K)`, but
    //   `a_1` wanders at the `1e-5` level and `a_2` plateaus near `0.1267` for `lg_k >= 8` instead
    //   of `0.12698`, because least squares optimizes uniform accuracy on `[0, 3]` rather than
    //   Taylor accuracy at `0`.
    //
    // * Fitting domain `C in [1, 6 * K]` (`x in (0, 3]`): past the runtime threshold (`5.6 * K` or
    //   `5.7 * K`) the stored rows keep a relative error below `5e-7` against `exact_icon_estimate`
    //   up to `C = 6 * K`, and diverge within a few percent beyond it (at `6.25 * K` every row is
    //   off by `4e-6` to `1.3e-4`). The end is sharp for small `K`: `lg_k = 4` has at most `8.7e-8`
    //   up to `C = 96` and `2.2e-6` at `C = 97`. For larger `lg_k` an integer step is too fine to
    //   locate it: `lg_k = 8` is still at `-6.6e-8` at `C = 6 * K + 1`. The end at exactly `6 * K`
    //   rests on the grid experiments for `lg_k <= 7` below. Why the fit extends past the runtime
    //   threshold, e.g. as a guard band against endpoint oscillation, is not documented.
    //
    // * Sample grid for `lg_k <= 7` (`6 * K <= 768`): ordinary least squares on `g_K` over every
    //   integer `C = 1 ..= 6 * K` reproduces the stored coefficients within `0.07%` to `1.4%` and
    //   the stored polynomial to `< 2e-8` relative prediction difference, although the unscaled
    //   monomial basis on these grids has a 2-norm condition number of about `1e17` (`1e14` after
    //   scaling its columns). The agreement is discriminating: moving either end of the grid by one
    //   integer, dropping `C = 1` (where the exact estimator returns `N = 1` by definition), or
    //   halving the grid spacing makes the coefficient agreement at least 4x (`lg_k = 6`) to 59x
    //   (`lg_k = 5`) worse. This strongly supports the grid but does not establish the original
    //   procedure: the weighting and the numerics are not identified, and a relative-error-weighted
    //   fit agrees slightly better (`0.63%`) at `lg_k = 4`.
    //
    // * Sample grid for `lg_k >= 8`: not identified. A discontinuity between `lg_k = 7` and `8`
    //   (`a_2`: `0.12534 -> 0.12667`; `a_19`: `4.28e-5 -> 2.91e-5`) and the irregular high-order
    //   coefficients afterwards suggest that the procedure changed, plausibly to a sampled grid,
    //   but neither exhaustive integer grids nor the dense-then-geometric grid of
    //   `icon_fitting_grid` reproduce the stored coefficients (deviations of 14% to 65%). For these
    //   rows the test only demonstrates functional equivalence: a fit of the same form attains the
    //   same `~1e-7` accuracy.

    /// Solves the overdetermined linear least-squares problem `min_a ||V * a - y||_2` (`m >= n`)
    /// via Householder QR factorization without external linear-algebra dependencies.
    fn solve_least_squares_qr(mut v: Vec<f64>, mut y: Vec<f64>, m: usize, n: usize) -> Vec<f64> {
        assert!(m >= n && v.len() == m * n && y.len() == m);

        for k in 0..n {
            let mut norm_sq = 0.0;
            for row in k..m {
                let val = v[row * n + k];
                norm_sq += val * val;
            }
            let norm = norm_sq.sqrt();
            if norm == 0.0 {
                continue;
            }

            let v_kk = v[k * n + k];
            let alpha = if v_kk >= 0.0 { -norm } else { norm };
            let u0 = v_kk - alpha;

            let mut u_norm_sq = u0 * u0;
            for row in (k + 1)..m {
                let val = v[row * n + k];
                u_norm_sq += val * val;
            }
            let beta = 2.0 / u_norm_sq;

            v[k * n + k] = alpha;
            for col in (k + 1)..n {
                let mut dot = u0 * v[k * n + col];
                for row in (k + 1)..m {
                    dot += v[row * n + k] * v[row * n + col];
                }
                let scale = beta * dot;
                v[k * n + col] -= scale * u0;
                for row in (k + 1)..m {
                    v[row * n + col] -= scale * v[row * n + k];
                }
            }

            let mut dot_y = u0 * y[k];
            for row in (k + 1)..m {
                dot_y += v[row * n + k] * y[row];
            }
            let scale_y = beta * dot_y;
            y[k] -= scale_y * u0;
            for row in (k + 1)..m {
                y[row] -= scale_y * v[row * n + k];
            }
        }

        let mut coeffs = vec![0.0; n];
        for i in (0..n).rev() {
            let mut sum = y[i];
            for j in (i + 1)..n {
                sum -= v[i * n + j] * coeffs[j];
            }
            coeffs[i] = sum / v[i * n + i];
        }
        coeffs
    }

    /// Generates the sample grid of coupon counts `C in [1, 6 * K]` for fitting row `lg_k`.
    ///
    /// For `lg_k <= 7` this is the exhaustive integer grid that reproduces the stored
    /// coefficients. For `lg_k >= 8` the original grid is unknown; the dense-then-geometric grid
    /// below is an assumption that reproduces the accuracy of the stored rows, not their
    /// coefficients.
    fn icon_fitting_grid(lg_k: usize) -> Vec<u64> {
        let k = 1_u64 << lg_k;
        let max_c = 6 * k;
        if lg_k <= 7 {
            (1..=max_c).collect()
        } else {
            // Assumed grid: dense small integers, then 4% geometric steps up to `6 * K`.
            let mut grid = Vec::with_capacity(512);
            let mut c = 1_u64;
            while c < max_c {
                grid.push(c);
                c = if c < 64 {
                    c + 1
                } else {
                    (c + 1).max((1040 * c) / 1000)
                };
            }
            grid.push(max_c);
            grid
        }
    }

    /// Fits the 20 degree-19 polynomial coefficients for `lg_k` by least squares against
    /// `exact_icon_estimate` on `C in [1, 6 * K]` (see `icon_fitting_grid` for which rows this
    /// reproduces coefficient by coefficient).
    fn fit_icon_polynomial_row(lg_k: usize) -> Vec<f64> {
        let kf = (1_u64 << lg_k) as f64;
        let col_ln1p = precompute_col_ln1p(kf);
        let grid = icon_fitting_grid(lg_k);
        let m = grid.len();
        let n = ICON_POLYNOMIAL_NUM_COEFFICIENTS;

        let mut vandermonde = vec![0.0; m * n];
        let mut target = vec![0.0; m];

        for (row, &c_int) in grid.iter().enumerate() {
            let cf = c_int as f64;
            let n_exact = exact_icon_estimate_with_ln1p(kf, &col_ln1p, cf);
            let ratio = cf / kf;
            let precond = 1.0 + (ratio * ratio * ratio) / ICON_CUBIC_PRECONDITIONER;
            target[row] = n_exact / (cf * precond);

            let x = cf / (2.0 * kf);
            let mut x_pow = 1.0;
            for col in 0..n {
                vandermonde[row * n + col] = x_pow;
                x_pow *= x;
            }
        }

        solve_least_squares_qr(vandermonde, target, m, n)
    }

    #[test]
    fn exact_icon_estimator_matches_approximation() {
        for lg_k in ICON_MIN_LOG_K..=ICON_MAX_LOG_K {
            let lg_k_u8 = lg_k as u8;
            let k = 1_u64 << lg_k;
            let kf = k as f64;

            assert_eq!(icon_estimate(lg_k_u8, 0), 0.0);
            assert_eq!(icon_estimate(lg_k_u8, 1), 1.0);

            // Verify strict monotonicity across the polynomial-to-exponential splice threshold
            // (`5.7 * K` for `lg_k < 14`, `5.6 * K` for `lg_k >= 14`).
            let threshold_factor = if lg_k < 14 { 5.7 } else { 5.6 };
            let splice_c = (threshold_factor * kf).floor() as u32;
            let mut prev_est = icon_estimate(lg_k_u8, splice_c - 2);
            for c in (splice_c - 1)..=(splice_c + 2) {
                let est = icon_estimate(lg_k_u8, c);
                assert_that!(est, gt(prev_est));
                prev_est = est;
            }

            // Geometric sweep from `C = 2` into the exponential regime (`60 * K`), matching the
            // characterization sweep in `IconEstimatorTest.java`.
            let max_c = (60 * k).min(u32::MAX as u64);
            let mut c = 2_u64;
            while c <= max_c {
                let exact = exact_icon_estimate(kf, c as f64);
                let approx = icon_estimate(lg_k_u8, c as u32);
                let rel_err = ((approx - exact) / exact).abs();
                // In the polynomial region (`<= threshold * K`) the stored rows stay below
                // `2.1e-7` on a dense sweep over every `lg_k`.
                //
                // In the exponential region (`> threshold * K`) two effects add up. The finite-`K`
                // correction makes the Poisson-based approximation overestimate `N` by about
                // `1 / (2 * K * lambda)` with `lambda = N / K`; this decays with `C` and peaks at
                // the splice point, where `lambda ≈ 41.3` gives `1 / (82.6 * K)`. The
                // `1.0 / (80 * k)` bound in `IconEstimatorTest::quickIconEstimatorTest` matches
                // this peak with a small margin. For large `K` the periodic term of FM85 theory
                // dominates instead, with relative amplitude `1.09e-6`; the dense-sweep maximum
                // for `lg_k >= 14` is `1.53e-6`.
                let tol = if (c as f64) <= threshold_factor * kf {
                    3e-7
                } else {
                    (2.5e-6_f64).max(1.0 / (80.0 * kf))
                };
                assert_that!(rel_err, lt(tol));

                c = if c < 32 {
                    c + 1
                } else {
                    (c + 1).max((125 * c) / 100)
                };
            }
        }
    }

    #[test]
    fn icon_exponential_constant_matches_theory() {
        // The coefficient used at runtime, read back from the production function.
        let runtime_constant = icon_exponential_approximation(1.0, 0.0);

        // Asymptotic FM85 theory (Mellin transform of the harmonic sum `E[C] / K`):
        // `E[C]/K = log2(N/K) + gamma/ln(2) - 1/2 + P(log2(N/K)) + o(1)`, where `P` is 1-periodic
        // and `ln(2) * P` has the fundamental amplitude `a = 2 * |Gamma(2*pi*i / ln 2)|`, about
        // `1.09e-6`. Hence `N = D * K * 2^(C/K) * exp(-ln(2) * P)`, where the closed form
        // `D = sqrt(2) * exp(-gamma) = 0.7940236163832829` differs from the runtime constant by
        // `2.4e-13`.
        //
        // The approximation `A * K * 2^(C/K)` has relative error `(A / D) * exp(ln(2) * P) - 1`.
        // Choosing `A` so that this error averages to zero over a period of `C / K` gives
        // `A = D / <exp(ln(2) * P)> = D * (1 - a^2 / 4)` up to `O(a^3)`, where
        // `a^2 / 4 = |Gamma(i * y)|^2 = pi / (y * sinh(pi * y))` with `y = 2 * pi / ln 2`. This
        // value rounds to the runtime constant, which explains its last digits; whether the
        // original fitting script used this criterion is not known.
        let d = SQRT_2 * (-EULER_MASCHERONI).exp();
        let y = 2.0 * PI / LN_2;
        let zero_mean_error_constant = d * (1.0 - PI / (y * (PI * y).sinh()));
        assert_that!(runtime_constant, near(zero_mean_error_constant, 3e-16));

        // The same criterion evaluated numerically: the harmonic mean of `N_exact / (K * 2^(C/K))`
        // over one period of `C / K`. At `C / K in [20, 21)` the finite-`K` bias
        // `1 / (2 * K * lambda)` is below `2e-16` for `K = 2^32`, and the tolerance separates this
        // criterion from `D` (`2.4e-13` away) and from the arithmetic mean (`4.7e-13` away).
        let kf = (1_u64 << 32) as f64;
        let samples = 32;
        let mut inverse_ratio_sum = 0.0;
        for m in 0..samples {
            let c_over_k = 20.0 + (m as f64 + 0.5) / (samples as f64);
            let n_exact = exact_icon_estimate(kf, c_over_k * kf);
            inverse_ratio_sum += kf * 2.0_f64.powf(c_over_k) / n_exact;
        }
        let harmonic_mean = samples as f64 / inverse_ratio_sum;
        assert_that!(runtime_constant, near(harmonic_mean, 1e-14));
    }

    #[test]
    fn reconstruct_icon_polynomial_coefficients() {
        // The highest-`lg_k` stored row (`lg_k = 26`) is consistent with the Maclaurin series
        // `N / C = 1 + (1/3)*x + (8/63)*x^2 + ...` (`x = C / (2 * K)`) only up to the bias of a
        // least-squares fit on `[0, 3]`: the stored `a_0`, `a_1`, `a_2` differ from the series by
        // `2.9e-9`, `5.0e-6`, and `3.4e-4`, and `a_2` does not converge with growing `lg_k`.
        let last_row_start = ICON_POLYNOMIAL_NUM_COEFFICIENTS * (ICON_MAX_LOG_K - ICON_MIN_LOG_K);
        assert_that!(
            ICON_POLYNOMIAL_COEFFICIENTS[last_row_start],
            near(1.0, 1e-7)
        );
        assert_that!(
            ICON_POLYNOMIAL_COEFFICIENTS[last_row_start + 1],
            near(1.0 / 3.0, 1e-5),
        );
        assert_that!(
            ICON_POLYNOMIAL_COEFFICIENTS[last_row_start + 2],
            near(8.0 / 63.0, 4e-4),
        );

        for lg_k in ICON_MIN_LOG_K..=ICON_MAX_LOG_K {
            let fitted = fit_icon_polynomial_row(lg_k);
            let stored_start = ICON_POLYNOMIAL_NUM_COEFFICIENTS * (lg_k - ICON_MIN_LOG_K);
            let stored = &ICON_POLYNOMIAL_COEFFICIENTS
                [stored_start..stored_start + ICON_POLYNOMIAL_NUM_COEFFICIENTS];

            if lg_k <= 7 {
                // On `lg_k <= 7`, the exact exhaustive integer grid `C = 1 ..= 6 * K` reproduces
                // `a_0` to `< 1e-6`, `a_1` to `< 2e-5`, and all 20 monomials within `2.5%`.
                // Closer agreement is not expected: the unscaled degree-19 monomial basis on these
                // grids is ill-conditioned (2-norm condition number `~1e17`), so tiny differences
                // in the target values move the high-degree coefficients by percents while leaving
                // `P(x)` invariant to `< 2e-8`.
                assert_that!(fitted[0], near(stored[0], 1e-6));
                assert_that!(fitted[1], near(stored[1], 2e-5));
                for deg in 0..ICON_POLYNOMIAL_NUM_COEFFICIENTS {
                    let rel_diff = ((fitted[deg] - stored[deg]) / stored[deg]).abs();
                    assert_that!(rel_diff, lt(0.025));
                }
            }

            // Across every `lg_k = 4..=26`, verify that the fitted polynomial agrees with both
            // the checked-in `ICON_POLYNOMIAL_COEFFICIENTS` row and `exact_icon_estimate`
            // throughout the active evaluation interval `[2, threshold * K]`. For `lg_k >= 8` this
            // shows functional equivalence only; the coefficients themselves are not reproduced.
            let k = 1_u64 << lg_k;
            let kf = k as f64;
            let threshold_factor = if lg_k < 14 { 5.7 } else { 5.6 };
            let max_active_c = (threshold_factor * kf).floor() as u64;

            let pred_vs_stored_tol = if lg_k <= 7 { 2e-8 } else { 4e-7 };
            for step in 0..=40 {
                let c_int = 2 + ((max_active_c - 2) * step) / 40;
                let cf = c_int as f64;
                let x = cf / (2.0 * kf);
                let ratio = cf / kf;
                let precond = 1.0 + (ratio * ratio * ratio) / ICON_CUBIC_PRECONDITIONER;

                let n_fitted = cf * precond * evaluate_polynomial(&fitted, 0, fitted.len(), x);
                let n_stored = cf * precond * evaluate_polynomial(stored, 0, stored.len(), x);
                let n_exact = exact_icon_estimate(kf, cf);

                let rel_vs_stored = ((n_fitted - n_stored) / n_stored).abs();
                let rel_vs_exact = ((n_fitted - n_exact) / n_exact).abs();
                assert_that!(rel_vs_stored, lt(pred_vs_stored_tol));
                assert_that!(rel_vs_exact, lt(4e-7));
            }
        }
    }

    // -------------------------------------------------------------------------
    // 3. Confidence interval constants & empirical tables (`cpc_confidence.hpp`)
    // -------------------------------------------------------------------------

    #[test]
    fn confidence_constants_and_bounds() {
        // Both asymptotic (`K -> inf`) relative standard errors, scaled by `sqrt(K)`, follow from
        // the Poisson model of the coupon matrix:
        // - ICON: cells are independent, so `Var(C) ≈ K * sum_j q_j * (1 - q_j)`. Averaged over a
        //   period, the sum equals `(1 / ln 2) * int_0^inf (exp(-u) - exp(-2u)) / u du = 1`
        //   (Frullani), so `sd(C) ≈ sqrt(K)`; since `N_hat` grows like `2^(C/K)`, the relative
        //   error is `ln(2) / sqrt(K)`.
        // - HIP: a new item changes the sketch with probability `p ≈ 1 / (lambda * ln 2)`, and the
        //   HIP variance `int_0^n (1/p - 1) dm ≈ n^2 * ln(2) / (2 * K)` gives `sqrt(ln(2) / 2)`.
        // `HIP_ERROR_CONSTANT` is spelled out as a decimal literal, so check it against that form.
        assert_that!(HIP_ERROR_CONSTANT, near((LN_2 / 2.0).sqrt(), 1e-16));

        // For `lg_k <= 14` the bounds use the Monte Carlo tables instead. Each entry is
        // `10_000 * sqrt(K) * eps / kappa`, where `eps` is the one-sided `Phi(-kappa)` tail
        // quantile of `estimate / N - 1` (high side, used by the lower bound) or of
        // `1 - estimate / N` (low side, used by the upper bound) at `N = 1000 * K`. Being random
        // measurements, the tables can only be re-measured statistically, not regenerated, and
        // this test does not verify their values or the coverage of the resulting intervals. It
        // checks only how the bounds use them: every bound brackets the estimate, respects `C`,
        // and widens strictly as `kappa` grows.
        for lg_k in 4..=16_u8 {
            let c = 3 * (1_u32 << lg_k);
            let icon_est = estimate(true, 0.0, lg_k, c);
            for (merge_flag, hip_est) in [(true, 0.0), (false, icon_est)] {
                let mut prev_lb = icon_est;
                let mut prev_ub = icon_est;
                for kappa in [NumStdDev::One, NumStdDev::Two, NumStdDev::Three] {
                    let lb = lower_bound(merge_flag, hip_est, lg_k, c, kappa);
                    let ub = upper_bound(merge_flag, hip_est, lg_k, c, kappa);
                    assert!(lb >= c as f64 && lb < prev_lb && ub > prev_ub);
                    prev_lb = lb;
                    prev_ub = ub;
                }
            }
        }
    }
}
