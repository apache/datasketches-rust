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

use std::io;
use std::io::Cursor;
use std::io::Read;

/// A wrapper around a byte slice that provides methods for reading various types of data from it.
pub struct SketchSlice<'a> {
    slice: Cursor<&'a [u8]>,
}

impl SketchSlice<'_> {
    /// Creates a new `SketchSlice` from the given byte slice.
    pub fn new(slice: &[u8]) -> SketchSlice<'_> {
        SketchSlice {
            slice: Cursor::new(slice),
        }
    }

    /// Advances the position of the slice by `n` bytes.
    pub fn advance(&mut self, n: u64) {
        let pos = self.slice.position();
        self.slice.set_position(pos + n);
    }

    /// Reads exactly `buf.len()` bytes from the slice into `buf`.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.slice.read_exact(buf)
    }

    /// Reads a single byte from the slice and returns it as a `u8`.
    pub fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Reads a single byte from the slice and returns it as an `i8`.
    pub fn read_i8(&mut self) -> io::Result<i8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0] as i8)
    }

    /// Reads a 16-bit unsigned integer from the slice in little-endian byte order.
    pub fn read_u16_le(&mut self) -> io::Result<u16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }

    /// Reads a 16-bit unsigned integer from the slice in big-endian byte order.
    pub fn read_u16_be(&mut self) -> io::Result<u16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }

    /// Reads a 16-bit signed integer from the slice in little-endian byte order.
    pub fn read_i16_le(&mut self) -> io::Result<i16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(i16::from_le_bytes(buf))
    }

    /// Reads a 16-bit signed integer from the slice in big-endian byte order.
    pub fn read_i16_be(&mut self) -> io::Result<i16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(i16::from_be_bytes(buf))
    }

    /// Reads a 32-bit unsigned integer from the slice in little-endian byte order.
    pub fn read_u32_le(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    /// Reads a 32-bit unsigned integer from the slice in big-endian byte order.
    pub fn read_u32_be(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    /// Reads a 32-bit signed integer from the slice in little-endian byte order.
    pub fn read_i32_le(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    /// Reads a 32-bit signed integer from the slice in big-endian byte order.
    pub fn read_i32_be(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    /// Reads a 16-bit unsigned integer from the slice in little-endian byte order.
    pub fn read_u64_le(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Reads a 16-bit unsigned integer from the slice in big-endian byte order.
    pub fn read_u64_be(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    /// Reads a 16-bit signed integer from the slice in little-endian byte order.
    pub fn read_i64_le(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    /// Reads a 16-bit signed integer from the slice in big-endian byte order.
    pub fn read_i64_be(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    /// Reads a 32-bit floating-point number from the slice in little-endian byte order.
    pub fn read_f32_le(&mut self) -> io::Result<f32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }

    /// Reads a 32-bit floating-point number from the slice in big-endian byte order.
    pub fn read_f32_be(&mut self) -> io::Result<f32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_be_bytes(buf))
    }

    /// Reads a 64-bit floating-point number from the slice in little-endian byte order.
    pub fn read_f64_le(&mut self) -> io::Result<f64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    /// Reads a 64-bit floating-point number from the slice in big-endian byte order.
    pub fn read_f64_be(&mut self) -> io::Result<f64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }
}
