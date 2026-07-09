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

//! Serialization of summaries.
//!
//! A Tuple sketch stores a user-defined summary next to every retained key. Because the summary
//! type is opaque to the sketch, (de)serialization of summaries is delegated to a [`SummarySerde`]
//! object, mirroring the C++ `SerDe` template parameter and the Java `SummarySerializer` /
//! `SummaryDeserializer` interfaces.

use crate::error::Error;

/// Serializes and deserializes a summary of type `S`.
///
/// The encoding is entirely up to the implementation; the sketch only requires that
/// [`deserialize`](Self::deserialize) report how many bytes it consumed so it can advance to the
/// next entry. This supports both fixed-width summaries (such as a `u64` counter) and
/// variable-width summaries (such as an array of doubles whose length is encoded in the bytes).
pub trait SummarySerde<S> {
    /// Appends the serialized form of `summary` to `out`.
    fn serialize(&self, summary: &S, out: &mut Vec<u8>);

    /// Reads one summary from the front of `bytes`, returning it together with the number of bytes
    /// consumed.
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` is too short or otherwise malformed for this encoding.
    fn deserialize(&self, bytes: &[u8]) -> Result<(S, usize), Error>;
}

/// A [`SummarySerde`] for fixed-width little-endian primitive summaries.
///
/// This covers the common case where the summary is a single integer or float (`u32`, `u64`, `i32`,
/// `i64`, `f32`, `f64`). The value is stored in little-endian byte order, matching the Java/C++
/// primitive serializers.
///
/// # Examples
///
/// ```
/// use datasketches::tuple::PrimitiveSummarySerde;
/// use datasketches::tuple::SummarySerde;
///
/// let serde = PrimitiveSummarySerde;
/// let mut bytes = Vec::new();
/// serde.serialize(&7u64, &mut bytes);
/// assert_eq!(bytes.len(), 8);
/// let (value, consumed) = serde.deserialize(&bytes).unwrap();
/// assert_eq!((value, consumed), (7u64, 8));
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct PrimitiveSummarySerde;

impl<S> SummarySerde<S> for PrimitiveSummarySerde
where
    S: private::LeBytes,
{
    fn serialize(&self, summary: &S, out: &mut Vec<u8>) {
        summary.write_le(out);
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<(S, usize), Error> {
        if bytes.len() < S::WIDTH {
            return Err(Error::insufficient_data(format!(
                "summary: expected {} bytes, got {}",
                S::WIDTH,
                bytes.len()
            )));
        }
        Ok((S::read_le(&bytes[..S::WIDTH]), S::WIDTH))
    }
}

mod private {
    /// Sealed helper describing fixed-width little-endian encoding of a primitive.
    pub trait LeBytes: Copy {
        /// Number of bytes in the little-endian encoding.
        const WIDTH: usize;
        /// Appends the little-endian bytes of `self` to `out`.
        fn write_le(self, out: &mut Vec<u8>);
        /// Reads the value from exactly `WIDTH` leading bytes of `bytes`.
        fn read_le(bytes: &[u8]) -> Self;
    }

    macro_rules! impl_le_bytes {
        ($($t:ty),* $(,)?) => {
            $(
                impl LeBytes for $t {
                    const WIDTH: usize = std::mem::size_of::<$t>();

                    fn write_le(self, out: &mut Vec<u8>) {
                        out.extend_from_slice(&self.to_le_bytes());
                    }

                    fn read_le(bytes: &[u8]) -> Self {
                        let mut buf = [0u8; std::mem::size_of::<$t>()];
                        buf.copy_from_slice(&bytes[..std::mem::size_of::<$t>()]);
                        <$t>::from_le_bytes(buf)
                    }
                }
            )*
        };
    }

    impl_le_bytes!(u32, u64, i32, i64, f32, f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_serde_round_trips_u64() {
        let serde = PrimitiveSummarySerde;
        let mut bytes = Vec::new();
        serde.serialize(&123456789u64, &mut bytes);
        assert_eq!(bytes.len(), 8);
        let (value, consumed): (u64, usize) = serde.deserialize(&bytes).unwrap();
        assert_eq!(value, 123456789);
        assert_eq!(consumed, 8);
    }

    #[test]
    fn primitive_serde_round_trips_f64() {
        let serde = PrimitiveSummarySerde;
        let mut bytes = Vec::new();
        serde.serialize(&3.5f64, &mut bytes);
        let (value, consumed): (f64, usize) = serde.deserialize(&bytes).unwrap();
        assert_eq!(value, 3.5);
        assert_eq!(consumed, 8);
    }

    #[test]
    fn primitive_serde_consumes_only_its_width() {
        let serde = PrimitiveSummarySerde;
        let mut bytes = Vec::new();
        serde.serialize(&9u32, &mut bytes);
        bytes.extend_from_slice(&[0xAA, 0xBB]); // trailing bytes belonging to the next entry
        let (value, consumed): (u32, usize) = serde.deserialize(&bytes).unwrap();
        assert_eq!(value, 9);
        assert_eq!(consumed, 4);
    }

    #[test]
    fn primitive_serde_rejects_short_input() {
        let serde = PrimitiveSummarySerde;
        let err = SummarySerde::<u64>::deserialize(&serde, &[0u8; 3]).unwrap_err();
        assert_eq!(err.kind(), crate::error::ErrorKind::InvalidData);
    }
}
