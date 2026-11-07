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

use std::hash::Hasher;

use crate::hash::XxHash64;

/// An item with a portable Count-Min encoding.
///
/// Implemented for integers up to 64 bits (including `isize` and `usize`), `str`, `String`,
/// byte slices, byte arrays, `Vec<u8>`, and shared references to these types. This trait is sealed.
///
/// Version 2 widens signed integers to `i64` and unsigned integers to `u64`, then hashes their
/// eight little-endian bytes. Strings use UTF-8 bytes; byte collections use their contents,
/// without a length prefix or terminator. Equal byte encodings identify the same item, even
/// across types. Empty strings and byte collections are ignored.
///
/// Convert floating-point and custom values to one of these supported types before updating or
/// querying. Generic callers should use this bound instead of [`std::hash::Hash`].
///
/// Version 1 sketches retain Rust's original [`std::hash::Hash`] encoding for the supplied
/// type, including empty inputs. Use the same input type that originally populated the sketch.
pub trait CountMinItem: private::CountMinItem {}

mod private {
    use std::hash::Hash;

    pub trait CountMinItem: Hash {
        fn hash_v2(&self, seed: u64) -> u64;

        fn is_empty(&self) -> bool {
            false
        }
    }
}

macro_rules! impl_integer {
    ($($ty:ty),* $(,)?) => {$(
        impl private::CountMinItem for $ty {
            #[inline]
            fn hash_v2(&self, seed: u64) -> u64 {
                XxHash64::hash_u64(*self as u64, seed)
            }
        }

        impl CountMinItem for $ty {}
    )*};
}

impl_integer!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

macro_rules! impl_bytes {
    ($($ty:ty),* $(,)?) => {$(
        impl private::CountMinItem for $ty {
            #[inline]
            fn hash_v2(&self, seed: u64) -> u64 {
                let mut hasher = XxHash64::with_seed(seed);
                hasher.write(self.as_ref());
                hasher.finish64()
            }

            fn is_empty(&self) -> bool {
                self.is_empty()
            }
        }

        impl CountMinItem for $ty {}
    )*};
}

impl_bytes!(str, String, [u8], Vec<u8>);

impl<const N: usize> private::CountMinItem for [u8; N] {
    #[inline]
    fn hash_v2(&self, seed: u64) -> u64 {
        self.as_slice().hash_v2(seed)
    }

    fn is_empty(&self) -> bool {
        N == 0
    }
}

impl<const N: usize> CountMinItem for [u8; N] {}

impl<I: CountMinItem + ?Sized> private::CountMinItem for &I {
    #[inline]
    fn hash_v2(&self, seed: u64) -> u64 {
        (**self).hash_v2(seed)
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }
}

impl<I: CountMinItem + ?Sized> CountMinItem for &I {}
