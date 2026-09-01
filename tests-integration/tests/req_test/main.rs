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

use datasketches::req::ReqFloat;

type ReqF32 = ReqFloat<f32>;
type ReqF64 = ReqFloat<f64>;

fn req_f32(value: f32) -> ReqF32 {
    ReqF32::new(value).unwrap()
}

fn req_f64(value: f64) -> ReqF64 {
    ReqF64::new(value).unwrap()
}

mod accuracy;
mod bounds;
mod core;
mod generic;
mod merge;
mod property;
mod query;
mod sorted_view_api;
mod structure;
