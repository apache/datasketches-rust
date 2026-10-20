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

//! Error types for DataSketches operations.
//!
//! # Query results
//!
//! An empty sketch is valid. Queries with no meaningful result use `Option` to represent
//! absence; invalid query arguments use `Result::Err`. A query that can encounter both returns
//! `Result<Option<T>, Error>`: `Ok(None)` means no result, not an invalid request.
//!
//! ## Queries without an empty-data result
//!
//! The following contracts include each sketch's public sorted view, where the query is exposed:
//!
//! | Sketch | Query | Return type | Valid query on empty data |
//! | --- | --- | --- | --- |
//! | KLL, REQ | `min_item`, `max_item` | `Option<&T>` | `None` |
//! | KLL, REQ | `rank` | `Option<f64>` | `None` |
//! | KLL, REQ | `quantile`, `quantiles`, `cdf`, `pmf` | `Result<Option<_>, Error>` | `Ok(None)` |
//! | `TDigest`, `TDigestMut` | `min_value`, `max_value` | `Option<f64>` | `None` |
//! | `TDigest`, `TDigestMut` | `rank`, `quantile`, `quantiles`, `cdf`, `pmf` | `Result<Option<_>, Error>` | `Ok(None)` |
//!
//! - **Validation comes first:** quantile ranks must be in `[0.0, 1.0]` and not NaN; split points
//!   must be strictly increasing. T-Digest also rejects NaN values passed to `rank` or used as
//!   split points. Invalid arguments return [`ErrorKind::InvalidArgument`], even on empty data.
//! - **Ordering determines fallibility:** KLL and REQ rank queries accept `Ord` items, so they
//!   return `Option` directly. T-Digest accepts `f64` and needs `Result` to reject NaN. Infinite
//!   values are valid T-Digest rank arguments and split points, but not quantile ranks.
//! - **Empty argument slices are valid:** on a non-empty sketch, `quantiles(&[])` returns
//!   `Ok(Some(vec![]))`, and `cdf(&[])` / `pmf(&[])` return `Ok(Some(vec![1.0]))`. On an empty
//!   sketch, all return `Ok(None)` because there is no distribution to query.
//! - **Caller migration:** handle infallible queries as an `Option`; for fallible queries, use `?`
//!   to propagate invalid arguments, then handle the `Option` for an empty sketch.
//!
//! ## Queries with meaningful empty-data results
//!
//! These existing APIs keep their return types. A defined zero, false, or empty collection is
//! a result and does not need an additional `Option`.
//!
//! | Sketch or operator | Query | Empty-data result |
//! | --- | --- | --- |
//! | CPC and `CpcWrapper` | `estimate`, `lower_bound`, `upper_bound` | `0.0` |
//! | HLL and `HllUnion` | `estimate`, `composite_estimate`, `lower_bound`, `upper_bound` | `0.0` |
//! | Theta, Tuple, including compact sketches | `estimate`, `lower_bound`, `upper_bound` | `0.0` |
//! | Count-Min, Frequent Items | `estimate`, `lower_bound`, `upper_bound` | `0` |
//! | Frequent Items | `frequent_items`, `frequent_items_with_threshold` | Empty vector |
//! | Bloom | `contains`, `contains_and_insert` | `false` (before insertion for the latter) |
//! | Bloom | `bits_used`, `load_factor`, `estimated_fpp` | Zero |
//! | CPC, HLL, Theta, Tuple unions | `to_sketch` | Empty sketch |
//! | Theta, Tuple | `compact`, `iter` | Empty sketch or iterator |
//! | Theta, Tuple A-not-B | `compute` | Empty left input: empty result; empty right input: left input |
//! | KLL, REQ | `sorted_view`; REQ `iter` | Empty view or iterator |
//! | T-Digest | `freeze`, `unfreeze` | Empty digest |
//! | Theta, Tuple Jaccard | `compute`, `exactly_equal` | Both empty: similarity `1.0`, equal; exactly one empty: similarity `0.0`, unequal |
//!
//! Configuration, size, count, and state accessors remain defined on empty sketches. KLL
//! `normalized_rank_error` / `normalized_pmf_error`, Count-Min `relative_error`, and Frequent
//! Items `epsilon` describe configuration. Frequent Items `maximum_error` is zero when empty.
//! REQ `rank_lower_bound` / `rank_upper_bound` evaluate an error model around a supplied
//! normalized rank; in exact mode, including an empty sketch, both return that rank.
//!
//! Emptiness means logical emptiness, not merely zero retained samples. Theta and Tuple can
//! sample out every input, and Frequent Items can purge all retained items from a non-empty stream.
//!
//! ## Intersection initialization
//!
//! `ThetaIntersection::to_sketch` and `TupleIntersection::to_sketch` already return `Option`:
//! before the first successful update they return `None`, because no result has been established.
//! After an update they return `Some(sketch)`, including `Some(empty_sketch)` for an empty
//! intersection. Updating with an empty input also establishes a result.
//!
//! Malformed serialized data continues to return [`ErrorKind::InvalidData`]; broken internal
//! invariants remain programming errors. Absence of a query result needs no additional error kind.

use std::fmt;

/// Categories of errors returned by DataSketches operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The argument provided is invalid.
    InvalidArgument,
    /// The serialized sketch data is malformed.
    InvalidData,
}

impl ErrorKind {
    /// Converts this error kind into a static string.
    pub const fn into_static(self) -> &'static str {
        match self {
            ErrorKind::InvalidArgument => "InvalidArgument",
            ErrorKind::InvalidData => "InvalidData",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

/// Error returned by a DataSketches operation.
///
/// # Examples
///
/// ```
/// use datasketches::error::Error;
/// use datasketches::error::ErrorKind;
///
/// let err = Error::new(ErrorKind::InvalidArgument, "bad input");
/// assert_eq!(err.kind(), ErrorKind::InvalidArgument);
/// assert_eq!(err.message(), "bad input");
/// ```
pub struct Error {
    kind: ErrorKind,
    message: String,
    context: Vec<(&'static str, String)>,
}

impl Error {
    /// Creates a new `Error` with the given kind and message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            context: vec![],
        }
    }

    /// Adds context to the error.
    pub fn with_context(mut self, key: &'static str, value: impl ToString) -> Self {
        self.context.push((key, value.to_string()));
        self
    }

    /// Returns the error kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        self.message.as_str()
    }
}

#[allow(dead_code)] // some convenient constructors are only used for certain sketches
impl Error {
    pub(crate) fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, msg)
    }

    pub(crate) fn deserial(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidData, msg)
    }

    pub(crate) fn insufficient_data(msg: impl fmt::Display) -> Self {
        Self::deserial(format!("insufficient data: {msg}"))
    }

    pub(crate) fn insufficient_data_of(context: &'static str, msg: impl fmt::Display) -> Self {
        Self::deserial(format!("insufficient data ({context}): {msg}"))
    }

    pub(crate) fn invalid_family(expected: u8, actual: u8, name: &'static str) -> Self {
        Self::deserial(format!(
            "invalid family: expected {expected} ({name}), got {actual}"
        ))
    }

    pub(crate) fn invalid_preamble_longs(expected: &[u8], actual: u8) -> Self {
        Error::deserial(format!(
            "invalid preamble longs: expected one of {expected:?}, got {actual}"
        ))
    }

    pub(crate) fn invalid_preamble_ints(expected: u8, actual: u8) -> Self {
        Self::deserial(format!(
            "invalid preamble ints: expected {expected}, got {actual}"
        ))
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If alternate has been specified, we will print like Debug.
        if f.alternate() {
            let mut de = f.debug_struct("Error");
            de.field("kind", &self.kind);
            de.field("message", &self.message);
            de.field("context", &self.context);
            return de.finish();
        }

        write!(f, "{}", self.kind)?;
        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }
        writeln!(f)?;

        if !self.context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (k, v) in self.context.iter() {
                writeln!(f, "   {k}: {v}")?;
            }
        }

        Ok(())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            for (i, (k, v)) in self.context.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}: {}", k, v)?;
            }
            write!(f, " }}")?;
        }

        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use super::*;

    #[test]
    fn test_format_consistency() {
        let err = Error::new(ErrorKind::InvalidArgument, "something went wrong");
        assert_snapshot!(err, @"InvalidArgument => something went wrong",);
    }

    #[test]
    fn test_format_with_multiple_contexts() {
        let err = Error::new(ErrorKind::InvalidData, "parsing failed")
            .with_context("index", 42)
            .with_context("file", "foo");
        assert_snapshot!(err, @"InvalidData, context: { index: 42, file: foo } => parsing failed");
    }
}
