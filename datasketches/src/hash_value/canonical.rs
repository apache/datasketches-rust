use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::ops::DerefMut;

/// A value wrapper that uses a canonical hashing strategy for its inner value.
///
/// See the [module level documentation](super) for more.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Canonical<T>(T);

impl<T> Canonical<T> {
    /// Get the value out.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for Canonical<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Canonical<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for Canonical<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl<T: fmt::Display> fmt::Display for Canonical<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

macro_rules! impl_canonical_value {
    ($t:ty, $ctor:ident, |$v:ident| $canonical:expr) => {
        impl From<$t> for Canonical<$t> {
            fn from(value: $t) -> Self {
                $ctor(value)
            }
        }

        impl Hash for Canonical<$t> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                let $v = &self.0;
                let canonical = $canonical;
                Hash::hash(&canonical, state);
            }
        }
    };
}

/// Create a canonical hashable value from a `f32` value.
///
/// This canonical mapping ensures that the same hash value is produced for the same numeric
/// value, regardless of the IEEE754 representation. For example, `canonical_f32(0.0f32)` will
/// produce the same hash value as `canonical_f32(-0.0f32)`. Furthermore, `canonical_f32(5.0f32)`
/// will produce the same hash value as `canonical_f64(5.0f64)`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::canonical_f32;
/// # use datasketches::hash_value::canonical_f64;
/// assert_eq!(
///     calculate_hash(canonical_f32(0.0)),
///     calculate_hash(canonical_f32(-0.0))
/// );
/// assert_eq!(
///     calculate_hash(canonical_f32(5.0)),
///     calculate_hash(canonical_f64(5.0))
/// );
/// ```
pub fn canonical_f32(v: f32) -> Canonical<f32> {
    Canonical(v)
}

/// Create a canonical hashable value from a `f64` value.
///
/// This canonical mapping ensures that the same hash value is produced for the same numeric
/// value, regardless of the IEEE754 representation. For example, `canonical_f64(0.0f64)` will
/// produce the same hash value as `canonical_f64(-0.0f64)`. Furthermore, `canonical_f64(5.0f64)`
/// will produce the same hash value as `canonical_f32(5.0f32)`.
///
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::canonical_f32;
/// # use datasketches::hash_value::canonical_f64;
/// assert_eq!(
///     calculate_hash(canonical_f64(0.0)),
///     calculate_hash(canonical_f64(-0.0))
/// );
/// assert_eq!(
///     calculate_hash(canonical_f32(5.0)),
///     calculate_hash(canonical_f64(5.0))
/// );
/// ```
pub fn canonical_f64(v: f64) -> Canonical<f64> {
    Canonical(v)
}

impl_canonical_value!(f32, canonical_f32, |v| canonical_f64(*v as f64));
impl_canonical_value!(f64, canonical_f64, |v| if v.is_nan() {
    // Java's Double.doubleToLongBits() NaN value
    0x7ff8000000000000u64
} else {
    // -0.0 + 0.0 == +0.0 under IEEE754 roundTiesToEven rounding mode,
    // which Rust guarantees. Thus, by adding a positive zero we
    // canonicalize signed zero without any branches in one instruction.
    (v + 0.0).to_bits()
});

/// Create a canonical hashable value from an `i8` value.
///
/// Integer canonicalization is compatible with datasketches-cpp. Values narrower than 64 bits are
/// converted to signed 64-bit values before hashing. Unsigned narrow values are first interpreted
/// as the signed integer of the same width, then sign-extended to `i64`.
///
/// This means `255u8` canonicalizes like `-1i8`, not like `255i64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_i8, canonical_i64, canonical_u8};
/// assert_eq!(
///     calculate_hash(canonical_i8(-1)),
///     calculate_hash(canonical_u8(255))
/// );
/// assert_eq!(
///     calculate_hash(canonical_i8(42)),
///     calculate_hash(canonical_i64(42))
/// );
/// assert_ne!(
///     calculate_hash(canonical_u8(128)),
///     calculate_hash(canonical_i64(128))
/// );
/// ```
pub fn canonical_i8(v: i8) -> Canonical<i8> {
    Canonical(v)
}

/// Create a canonical hashable value from a `u8` value.
///
/// Integer canonicalization is compatible with datasketches-cpp. Values narrower than 64 bits are
/// converted to signed 64-bit values before hashing. Unsigned narrow values are first interpreted
/// as the signed integer of the same width, then sign-extended to `i64`.
///
/// This means `255u8` canonicalizes like `-1i8`, not like `255i64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_i8, canonical_i64, canonical_u8};
/// assert_eq!(
///     calculate_hash(canonical_u8(255)),
///     calculate_hash(canonical_i8(-1))
/// );
/// assert_eq!(
///     calculate_hash(canonical_u8(42)),
///     calculate_hash(canonical_i64(42))
/// );
/// assert_ne!(
///     calculate_hash(canonical_u8(128)),
///     calculate_hash(canonical_i64(128))
/// );
/// ```
pub fn canonical_u8(v: u8) -> Canonical<u8> {
    Canonical(v)
}

/// Create a canonical hashable value from an `i16` value.
///
/// Integer canonicalization is compatible with datasketches-cpp. Values narrower than 64 bits are
/// converted to signed 64-bit values before hashing. Unsigned narrow values are first interpreted
/// as the signed integer of the same width, then sign-extended to `i64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_i16, canonical_i64};
/// assert_eq!(
///     calculate_hash(canonical_i16(42)),
///     calculate_hash(canonical_i64(42))
/// );
/// assert_eq!(
///     calculate_hash(canonical_i16(-1)),
///     calculate_hash(canonical_i64(-1))
/// );
/// ```
pub fn canonical_i16(v: i16) -> Canonical<i16> {
    Canonical(v)
}

/// Create a canonical hashable value from a `u16` value.
///
/// Integer canonicalization is compatible with datasketches-cpp. Values narrower than 64 bits are
/// converted to signed 64-bit values before hashing. Unsigned narrow values are first interpreted
/// as the signed integer of the same width, then sign-extended to `i64`.
///
/// This means `65535u16` canonicalizes like `-1i16`, not like `65535i64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_i16, canonical_i64, canonical_u16};
/// assert_eq!(
///     calculate_hash(canonical_u16(65535)),
///     calculate_hash(canonical_i16(-1))
/// );
/// assert_eq!(
///     calculate_hash(canonical_u16(42)),
///     calculate_hash(canonical_i64(42))
/// );
/// assert_ne!(
///     calculate_hash(canonical_u16(32768)),
///     calculate_hash(canonical_i64(32768))
/// );
/// ```
pub fn canonical_u16(v: u16) -> Canonical<u16> {
    Canonical(v)
}

/// Create a canonical hashable value from an `i32` value.
///
/// Integer canonicalization is compatible with datasketches-cpp. Values narrower than 64 bits are
/// converted to signed 64-bit values before hashing. Unsigned narrow values are first interpreted
/// as the signed integer of the same width, then sign-extended to `i64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_i32, canonical_i64};
/// assert_eq!(
///     calculate_hash(canonical_i32(42)),
///     calculate_hash(canonical_i64(42))
/// );
/// assert_eq!(
///     calculate_hash(canonical_i32(-1)),
///     calculate_hash(canonical_i64(-1))
/// );
/// ```
pub fn canonical_i32(v: i32) -> Canonical<i32> {
    Canonical(v)
}

/// Create a canonical hashable value from a `u32` value.
///
/// Integer canonicalization is compatible with datasketches-cpp. Values narrower than 64 bits are
/// converted to signed 64-bit values before hashing. Unsigned narrow values are first interpreted
/// as the signed integer of the same width, then sign-extended to `i64`.
///
/// This means `4294967295u32` canonicalizes like `-1i32`, not like `4294967295i64`.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_i32, canonical_i64, canonical_u32};
/// assert_eq!(
///     calculate_hash(canonical_u32(4294967295)),
///     calculate_hash(canonical_i32(-1))
/// );
/// assert_eq!(
///     calculate_hash(canonical_u32(42)),
///     calculate_hash(canonical_i64(42))
/// );
/// assert_ne!(
///     calculate_hash(canonical_u32(2147483648)),
///     calculate_hash(canonical_i64(2147483648))
/// );
/// ```
pub fn canonical_u32(v: u32) -> Canonical<u32> {
    Canonical(v)
}

/// Create a canonical hashable value from an `i64` value.
///
/// Integer canonicalization is compatible with datasketches-cpp. Signed 64-bit values are hashed
/// as themselves.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::canonical_i64;
/// assert_eq!(
///     calculate_hash(canonical_i64(42)),
///     calculate_hash(42i64)
/// );
/// assert_eq!(
///     calculate_hash(canonical_i64(-1)),
///     calculate_hash(-1i64)
/// );
/// ```
pub fn canonical_i64(v: i64) -> Canonical<i64> {
    Canonical(v)
}

/// Create a canonical hashable value from a `u64` value.
///
/// Integer canonicalization is compatible with datasketches-cpp. Unsigned 64-bit values are hashed
/// as themselves.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::canonical_u64;
/// assert_eq!(
///     calculate_hash(canonical_u64(42)),
///     calculate_hash(42u64)
/// );
/// assert_eq!(
///     calculate_hash(canonical_u64(u64::MAX)),
///     calculate_hash(u64::MAX)
/// );
/// ```
pub fn canonical_u64(v: u64) -> Canonical<u64> {
    Canonical(v)
}

impl_canonical_value!(i8, canonical_i8, |v| *v as i64);
impl_canonical_value!(u8, canonical_u8, |v| (*v as i8) as i64);
impl_canonical_value!(i16, canonical_i16, |v| *v as i64);
impl_canonical_value!(u16, canonical_u16, |v| (*v as i16) as i64);
impl_canonical_value!(i32, canonical_i32, |v| *v as i64);
impl_canonical_value!(u32, canonical_u32, |v| (*v as i32) as i64);
impl_canonical_value!(i64, canonical_i64, |v| *v);
impl_canonical_value!(u64, canonical_u64, |v| *v);
