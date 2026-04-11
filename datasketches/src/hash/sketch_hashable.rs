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

use std::hash::Hash;

use crate::common::canonical_double;

mod private {
    pub trait Sealed {}
}

/// A trait for customizing sketch update hash behavior.
pub trait SketchHashable: private::Sealed {
    /// Returns a canonical hashable view for use by sketch update operations.
    fn to_hashable(&self) -> impl Hash;
}

/// A wrapper for byte-oriented inputs that hashes only the payload bytes.
///
/// Rust's `Hash` implementations for byte-like types such as `&str`, `String`, `&[u8]`, and
/// `Vec<u8>` are not raw-byte writes. They delegate through `Hasher::write_*` helpers that also
/// mix structural information, notably the slice length, into the hash stream. That behavior is
/// correct for Rust collections in general, but it does not match DataSketches update hashing.
///
/// The Java and C++ DataSketches implementations hash string and byte inputs by feeding only the
/// UTF-8 / byte payload into the sketch hash function. They do not append an extra Rust-specific
/// length marker. For cross-language compatibility we need to reproduce that "raw bytes only"
/// contract here.
struct RawBytes<'a>(&'a [u8]);

impl Hash for RawBytes<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.0);
    }
}

macro_rules! impl_sketch_hashable_via_i64 {
    ($($src:ty => $mid:ty),* $(,)?) => {
        $(
            impl private::Sealed for $src {}

            impl SketchHashable for $src {
                fn to_hashable(&self) -> impl Hash {
                    (*self as $mid) as i64
                }
            }
        )*
    };
}

macro_rules! impl_sketch_hashable_passthrough {
    ($($src:ty),* $(,)?) => {
        $(
            impl private::Sealed for $src {}

            impl SketchHashable for $src {
                fn to_hashable(&self) -> impl Hash {
                    *self
                }
            }
        )*
    };
}

impl_sketch_hashable_via_i64!(
    i8 => i64,
    i16 => i64,
    i32 => i64,
    i64 => i64,
    isize => i64,
    u8 => i8,
    u16 => i16,
    u32 => i32,
);

impl_sketch_hashable_passthrough!(bool, char, i128, u64, u128, usize);

impl private::Sealed for f64 {}

impl SketchHashable for f64 {
    fn to_hashable(&self) -> impl Hash {
        canonical_double(*self)
    }
}

impl private::Sealed for f32 {}

impl SketchHashable for f32 {
    fn to_hashable(&self) -> impl Hash {
        canonical_double(*self as f64)
    }
}

impl private::Sealed for &str {}

impl SketchHashable for &str {
    fn to_hashable(&self) -> impl Hash {
        RawBytes(self.as_bytes())
    }
}

impl private::Sealed for String {}

impl SketchHashable for String {
    fn to_hashable(&self) -> impl Hash {
        RawBytes(self.as_bytes())
    }
}

impl private::Sealed for &String {}

impl SketchHashable for &String {
    fn to_hashable(&self) -> impl Hash {
        RawBytes(self.as_bytes())
    }
}

impl private::Sealed for &[u8] {}

impl SketchHashable for &[u8] {
    fn to_hashable(&self) -> impl Hash {
        RawBytes(self)
    }
}

impl private::Sealed for Vec<u8> {}

impl SketchHashable for Vec<u8> {
    fn to_hashable(&self) -> impl Hash {
        RawBytes(self.as_slice())
    }
}

impl private::Sealed for &Vec<u8> {}

impl SketchHashable for &Vec<u8> {
    fn to_hashable(&self) -> impl Hash {
        RawBytes(self.as_slice())
    }
}

impl<const N: usize> private::Sealed for [u8; N] {}

impl<const N: usize> SketchHashable for [u8; N] {
    fn to_hashable(&self) -> impl Hash {
        RawBytes(self.as_slice())
    }
}

impl<const N: usize> private::Sealed for &[u8; N] {}

impl<const N: usize> SketchHashable for &[u8; N] {
    fn to_hashable(&self) -> impl Hash {
        RawBytes(self.as_slice())
    }
}
