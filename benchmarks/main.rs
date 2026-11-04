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

mod bloom;
mod countmin;
mod cpc;
mod frequencies;
mod hash_inputs;
mod hll;
mod kll;
mod req;
mod tdigest;
mod theta;
mod tuple;
mod xor;

fn main() {
    let mut runner = benchmarks::Runner::from_args();
    bloom::register(&mut runner);
    countmin::register(&mut runner);
    cpc::register(&mut runner);
    frequencies::register(&mut runner);
    hll::register(&mut runner);
    kll::register(&mut runner);
    req::register(&mut runner);
    tdigest::register(&mut runner);
    theta::register(&mut runner);
    tuple::register(&mut runner);
    xor::register(&mut runner);
    runner.finish();
}
