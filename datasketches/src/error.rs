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

//! Error types for datasketches operations

use std::fmt;

/// ErrorKind is all kinds of Error of datasketches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The config for sketches is invalid.
    ConfigInvalid,
    /// The sketch data deserializing is malformed.
    MalformedDeserializeData,
}

impl ErrorKind {
    /// Convert this error kind instance into static str.
    pub const fn into_static(self) -> &'static str {
        match self {
            ErrorKind::ConfigInvalid => "ConfigInvalid",
            ErrorKind::MalformedDeserializeData => "MalformedDeserializeData",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

/// Error is the error struct returned by all datasketches functions.
pub struct Error {
    kind: ErrorKind,
    message: String,
    context: Vec<(&'static str, String)>,
    source: Option<anyhow::Error>,
}

impl Error {
    /// Create a new Error with error kind and message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            context: Vec::default(),
            source: None,
        }
    }

    /// Add more context in error.
    pub fn with_context(mut self, key: &'static str, value: impl ToString) -> Self {
        self.context.push((key, value.to_string()));
        self
    }

    /// Set source for error.
    ///
    /// # Panics
    ///
    /// Panics if the source has been set.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::error::Error as _;
    /// use datasketches::error::{Error, ErrorKind};
    ///
    /// let mut error = Error::new(ErrorKind::MalformedDeserializeData, "failed to deserialize sketch");
    /// assert!(error.source().is_none());
    /// error = error.set_source(std::io::Error::new(std::io::ErrorKind::Other, "IO error"));
    /// assert!(error.source().is_some());
    /// ```
    pub fn set_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        assert!(self.source.is_none(), "the source error has been set");
        self.source = Some(src.into());
        self
    }

    /// Return error's kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Return error's message.
    pub fn message(&self) -> &str {
        self.message.as_str()
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
            de.field("source", &self.source);
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

        if let Some(source) = &self.source {
            writeln!(f)?;
            writeln!(f, "Source:")?;
            writeln!(f, "   {source:#}")?;
        }

        Ok(())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            write!(
                f,
                "{}",
                self.context
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, " }}")?;
        }

        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref())
    }
}

/// Errors that can occur during sketch serialization or deserialization
#[derive(Debug, Clone)]
pub enum SerdeError {
    /// Insufficient data in buffer
    InsufficientData(String),
    /// Invalid sketch family identifier
    InvalidFamily(String),
    /// Unsupported serialization version
    UnsupportedVersion(String),
    /// Invalid parameter value
    InvalidParameter(String),
    /// Malformed or corrupt sketch data
    MalformedData(String),
}

impl fmt::Display for SerdeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SerdeError::InsufficientData(msg) => write!(f, "insufficient data: {}", msg),
            SerdeError::InvalidFamily(msg) => write!(f, "invalid family: {}", msg),
            SerdeError::UnsupportedVersion(msg) => write!(f, "unsupported version: {}", msg),
            SerdeError::InvalidParameter(msg) => write!(f, "invalid parameter: {}", msg),
            SerdeError::MalformedData(msg) => write!(f, "malformed data: {}", msg),
        }
    }
}

impl std::error::Error for SerdeError {}
