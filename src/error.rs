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
