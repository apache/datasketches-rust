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

/// A simple wrapper around a `Vec<u8>` that provides methods for writing various types of data.
pub struct SketchBytes {
    bytes: Vec<u8>,
}

impl SketchBytes {
    /// Constructs an empty `SketchBytes` with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    /// Consumes the `SketchBytes` and returns the underlying `Vec<u8>`.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Writes the given byte slice to the `SketchBytes`.
    pub fn write(&mut self, buf: &[u8]) {
        self.bytes.extend_from_slice(buf);
    }

    /// Writes a single byte to the `SketchBytes`.
    pub fn write_u8(&mut self, n: u8) {
        self.bytes.push(n);
    }

    /// Writes a single byte to the `SketchBytes`.
    pub fn write_i8(&mut self, n: i8) {
        self.bytes.push(n as u8);
    }

    /// Writes a 16-bit unsigned integer to the `SketchBytes` in little-endian byte order.
    pub fn write_u16_le(&mut self, n: u16) {
        self.write(&n.to_le_bytes());
    }

    /// Writes a 16-bit unsigned integer to the `SketchBytes` in big-endian byte order.
    pub fn write_u16_be(&mut self, n: u16) {
        self.write(&n.to_be_bytes());
    }

    /// Writes a 16-bit signed integer to the `SketchBytes` in little-endian byte order.
    pub fn write_i16_le(&mut self, n: i16) {
        self.write(&n.to_le_bytes());
    }

    /// Writes a 16-bit signed integer to the `SketchBytes` in big-endian byte order.
    pub fn write_i16_be(&mut self, n: i16) {
        self.write(&n.to_be_bytes());
    }

    /// Writes a 32-bit unsigned integer to the `SketchBytes` in little-endian byte order.
    pub fn write_u32_le(&mut self, n: u32) {
        self.write(&n.to_le_bytes());
    }

    /// Writes a 32-bit unsigned integer to the `SketchBytes` in big-endian byte order.
    pub fn write_u32_be(&mut self, n: u32) {
        self.write(&n.to_be_bytes());
    }

    /// Writes a 32-bit signed integer to the `SketchBytes` in little-endian byte order.
    pub fn write_i32_le(&mut self, n: i32) {
        self.write(&n.to_le_bytes());
    }

    /// Writes a 32-bit signed integer to the `SketchBytes` in big-endian byte order.
    pub fn write_i32_be(&mut self, n: i32) {
        self.write(&n.to_be_bytes());
    }

    /// Writes a 64-bit unsigned integer to the `SketchBytes` in little-endian byte order.
    pub fn write_u64_le(&mut self, n: u64) {
        self.write(&n.to_le_bytes());
    }

    /// Writes a 64-bit unsigned integer to the `SketchBytes` in big-endian byte order.
    pub fn write_u64_be(&mut self, n: u64) {
        self.write(&n.to_be_bytes());
    }

    /// Writes a 64-bit signed integer to the `SketchBytes` in little-endian byte order.
    pub fn write_i64_le(&mut self, n: i64) {
        self.write(&n.to_le_bytes());
    }

    /// Writes a 64-bit signed integer to the `SketchBytes` in big-endian byte order.
    pub fn write_i64_be(&mut self, n: i64) {
        self.write(&n.to_be_bytes());
    }

    /// Writes a 32-bit floating-point number to the `SketchBytes` in little-endian byte order.
    pub fn write_f32_le(&mut self, n: f32) {
        self.write(&n.to_le_bytes());
    }

    /// Writes a 32-bit floating-point number to the `SketchBytes` in big-endian byte order.
    pub fn write_f32_be(&mut self, n: f32) {
        self.write(&n.to_be_bytes());
    }

    /// Writes a 64-bit floating-point number to the `SketchBytes` in little-endian byte order.
    pub fn write_f64_le(&mut self, n: f64) {
        self.write(&n.to_le_bytes());
    }

    /// Writes a 64-bit floating-point number to the `SketchBytes` in big-endian byte order.
    pub fn write_f64_be(&mut self, n: f64) {
        self.write(&n.to_be_bytes());
    }
}
