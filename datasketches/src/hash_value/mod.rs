//! HashValue

mod canonical;

use std::hash::Hash;
use std::hash::Hasher;

pub use self::canonical::Canonical;
pub use self::canonical::canonical_f32;
pub use self::canonical::canonical_f64;

#[doc(hidden)] // for doctest
pub fn calculate_hash<T: Hash>(t: T) -> u64 {
    use std::hash::DefaultHasher;

    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// HashValue
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub struct HashValue<T> {
    value: T,
}

impl<T: Hash> Hash for HashValue<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<T: Hash> HashValue<T> {
    /// Create a `HashValue` from a hashable value.
    pub fn from_hash(value: T) -> Self {
        HashValue { value }
    }
}

impl HashValue<u64> {
    /// Create a `HashValue` from a `f64` value using the canonical mapping.
    #[inline(always)]
    pub fn canonical_f64(value: f64) -> Self {
        HashValue {
            value: if value.is_nan() {
                // Java's Double.doubleToLongBits() NaN value
                0x7ff8000000000000u64
            } else {
                // -0.0 + 0.0 == +0.0 under IEEE754 roundTiesToEven rounding mode,
                // which Rust guarantees. Thus, by adding a positive zero we
                // canonicalize signed zero without any branches in one instruction.
                (value + 0.0).to_bits()
            },
        }
    }

    /// Create a `HashValue` from a `f32` value using the canonical mapping.
    #[inline(always)]
    pub fn canonical_f32(value: f32) -> Self {
        HashValue::canonical_f64(value as f64)
    }
}

impl HashValue<i64> {
    /// Create a `HashValue` from an `i8` value by casting the value to `i64`.
    ///
    /// This canonical mapping ensures that the same hash value is produced for the same numeric
    /// value, regardless of the original type. For example, `HashValue::canonical_i8(42)` will
    /// produce the same hash value as `HashValue::from_hash(42i64)`.
    ///
    /// This is compatible with datasketches-cpp's behavior.
    pub fn canonical_i8(value: i8) -> Self {
        HashValue {
            value: value as i64,
        }
    }
}
