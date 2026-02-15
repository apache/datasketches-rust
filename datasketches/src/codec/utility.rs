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

use std::collections::Bound;
use std::ops::RangeBounds;

use crate::error::Error;

pub(crate) fn ensure_serial_version_is(expected: u8, actual: u8) -> Result<(), Error> {
    if expected == actual {
        Ok(())
    } else {
        Err(Error::deserial(format!(
            "unsupported serial version: expected {expected}, got {actual}"
        )))
    }
}

pub(crate) fn ensure_preamble_longs_in(expected: &[u8], actual: u8) -> Result<(), Error> {
    if expected.contains(&actual) {
        Ok(())
    } else {
        Err(Error::invalid_preamble_longs(expected, actual))
    }
}

pub(crate) fn ensure_preamble_longs_in_range(
    expected: impl RangeBounds<u8>,
    actual: u8,
) -> Result<(), Error> {
    let start = expected.start_bound();
    let end = expected.end_bound();
    if expected.contains(&actual) {
        Ok(())
    } else {
        Err(Error::deserial(format!(
            "invalid preamble longs: expected {}, got {actual}",
            match (start, end) {
                (Bound::Included(a), Bound::Included(b)) => format!("[{a}, {b}]"),
                (Bound::Included(a), Bound::Excluded(b)) => format!("[{a}, {b})"),
                (Bound::Excluded(a), Bound::Included(b)) => format!("({a}, {b}]"),
                (Bound::Excluded(a), Bound::Excluded(b)) => format!("({a}, {b})"),
                (Bound::Unbounded, Bound::Included(b)) => format!("at most {b}"),
                (Bound::Unbounded, Bound::Excluded(b)) => format!("less than {b}"),
                (Bound::Included(a), Bound::Unbounded) => format!("at least {a}"),
                (Bound::Excluded(a), Bound::Unbounded) => format!("greater than {a}"),
                (Bound::Unbounded, Bound::Unbounded) => unreachable!("unbounded range"),
            }
        )))
    }
}
