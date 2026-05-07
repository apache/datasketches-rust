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

macro_rules! impl_canonical_number {
    ($t:ty, $ctor:ident, |$v:ident| $canonical:expr) => {
        impl From<$t> for Canonical<$t> {
            fn from(value: $t) -> Self {
                $ctor(value)
            }
        }

        impl Hash for Canonical<$t> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                let $v = self.0;
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

impl_canonical_number!(f32, canonical_f32, |v| canonical_f64(v as f64));
impl_canonical_number!(f64, canonical_f64, |v| if v.is_nan() {
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
/// assert_eq!(calculate_hash(canonical_i64(42)), calculate_hash(42i64));
/// assert_eq!(calculate_hash(canonical_i64(-1)), calculate_hash(-1i64));
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
/// assert_eq!(calculate_hash(canonical_u64(42)), calculate_hash(42u64));
/// assert_eq!(
///     calculate_hash(canonical_u64(u64::MAX)),
///     calculate_hash(u64::MAX)
/// );
/// ```
pub fn canonical_u64(v: u64) -> Canonical<u64> {
    Canonical(v)
}

impl_canonical_number!(i8, canonical_i8, |v| v as i64);
impl_canonical_number!(u8, canonical_u8, |v| (v as i8) as i64);
impl_canonical_number!(i16, canonical_i16, |v| v as i64);
impl_canonical_number!(u16, canonical_u16, |v| (v as i16) as i64);
impl_canonical_number!(i32, canonical_i32, |v| v as i64);
impl_canonical_number!(u32, canonical_u32, |v| (v as i32) as i64);
impl_canonical_number!(i64, canonical_i64, |v| v);
impl_canonical_number!(u64, canonical_u64, |v| v);

/// Create a canonical hashable value from a byte vector.
///
/// Slice canonicalization hashes the raw bytes of the vector without Rust's slice length prefix.
/// This matches the datasketches-cpp raw byte update path used by sketches.
///
/// Empty byte vectors have zero bytes to hash. Use [`Canonical::<Vec<u8>>::is_empty`] to skip
/// empty values before updating a sketch if you need to mirror datasketches-cpp's empty slice
/// behavior.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::canonical_vec;
/// assert_eq!(
///     calculate_hash(canonical_vec(vec![b'a', b'b', b'c'])),
///     calculate_hash(canonical_vec(b"abc".to_vec()))
/// );
/// assert_ne!(
///     calculate_hash(canonical_vec(vec![b'a', b'b'])),
///     calculate_hash(canonical_vec(b"abc".to_vec()))
/// );
/// assert!(canonical_vec(Vec::new()).is_empty());
/// ```
pub fn canonical_vec(v: Vec<u8>) -> Canonical<Vec<u8>> {
    Canonical(v)
}

impl From<Vec<u8>> for Canonical<Vec<u8>> {
    fn from(value: Vec<u8>) -> Self {
        canonical_vec(value)
    }
}

impl Hash for Canonical<Vec<u8>> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0.as_slice());
    }
}

impl Canonical<Vec<u8>> {
    /// Returns `true` if this value has a length of zero bytes.
    ///
    /// datasketches-cpp ignores empty slices before hashing. Check this method before
    /// updating a sketch when matching that behavior matters.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::hash_value::canonical_vec;
    /// assert!(canonical_vec(Vec::new()).is_empty());
    /// assert!(!canonical_vec(b"abc".to_vec()).is_empty());
    /// ```
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Create a canonical hashable value from a string.
///
/// String canonicalization hashes the UTF-8 bytes of the string without Rust's string length
/// prefix. This matches the datasketches-cpp `std::string` update path, which hashes `c_str()` with
/// the string length.
///
/// Empty strings have zero bytes to hash. Use [`Canonical::<String>::is_empty`] to skip empty
/// values before updating a sketch if you need to mirror datasketches-cpp's empty string behavior.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_string, canonical_vec};
/// assert_eq!(
///     calculate_hash(canonical_string("abc".to_owned())),
///     calculate_hash(canonical_vec(b"abc".to_vec()))
/// );
/// assert_ne!(
///     calculate_hash(canonical_string("ab".to_owned())),
///     calculate_hash(canonical_string("abc".to_owned()))
/// );
/// assert!(canonical_string(String::new()).is_empty());
/// ```
pub fn canonical_string(v: String) -> Canonical<String> {
    Canonical(v)
}

impl From<String> for Canonical<String> {
    fn from(value: String) -> Self {
        canonical_string(value)
    }
}

impl Hash for Canonical<String> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0.as_bytes());
    }
}

impl Canonical<String> {
    /// Returns `true` if this value has a length of zero bytes.
    ///
    /// datasketches-cpp ignores empty strings before hashing. Check this method before
    /// updating a sketch when matching that behavior matters.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::hash_value::canonical_string;
    /// assert!(canonical_string(String::new()).is_empty());
    /// assert!(!canonical_string("abc".to_owned()).is_empty());
    /// ```
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Create a canonical hashable value from a byte slice.
///
/// Slice canonicalization hashes the raw bytes of the slice without Rust's slice length prefix.
/// This matches the datasketches-cpp raw byte update path used by sketches.
///
/// Empty byte slices have zero bytes to hash. Use [`Canonical::<&[u8]>::is_empty`] to skip empty
/// values before updating a sketch if you need to mirror datasketches-cpp's empty slice behavior.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_slice, canonical_vec};
/// assert_eq!(
///     calculate_hash(canonical_slice(b"abc")),
///     calculate_hash(canonical_vec(b"abc".to_vec()))
/// );
/// assert_ne!(
///     calculate_hash(canonical_slice(b"ab")),
///     calculate_hash(canonical_slice(b"abc"))
/// );
/// assert!(canonical_slice(&[]).is_empty());
/// ```
pub fn canonical_slice(v: &[u8]) -> Canonical<&[u8]> {
    Canonical(v)
}

impl<'a> From<&'a [u8]> for Canonical<&'a [u8]> {
    fn from(value: &'a [u8]) -> Self {
        canonical_slice(value)
    }
}

impl Hash for Canonical<&[u8]> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0);
    }
}

impl Canonical<&[u8]> {
    /// Returns `true` if this value has a length of zero bytes.
    ///
    /// datasketches-cpp ignores empty slices before hashing. Check this method before
    /// updating a sketch when matching that behavior matters.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::hash_value::canonical_slice;
    /// assert!(canonical_slice(&[]).is_empty());
    /// assert!(!canonical_slice(b"abc").is_empty());
    /// ```
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Create a canonical hashable value from a string slice.
///
/// String canonicalization hashes the UTF-8 bytes of the string slice without Rust's string length
/// prefix. This matches the datasketches-cpp `std::string` update path, which hashes `c_str()` with
/// the string length.
///
/// Empty string slices have zero bytes to hash. Use [`Canonical::<&str>::is_empty`] to skip empty
/// values before updating a sketch if you need to mirror datasketches-cpp's empty string behavior.
///
/// # Examples
///
/// ```
/// # use datasketches::hash_value::calculate_hash;
/// # use datasketches::hash_value::{canonical_str, canonical_string};
/// assert_eq!(
///     calculate_hash(canonical_str("abc")),
///     calculate_hash(canonical_string("abc".to_owned()))
/// );
/// assert_ne!(
///     calculate_hash(canonical_str("ab")),
///     calculate_hash(canonical_str("abc"))
/// );
/// assert!(canonical_str("").is_empty());
/// ```
pub fn canonical_str(v: &str) -> Canonical<&str> {
    Canonical(v)
}

impl<'a> From<&'a str> for Canonical<&'a str> {
    fn from(value: &'a str) -> Self {
        canonical_str(value)
    }
}

impl Hash for Canonical<&str> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0.as_bytes());
    }
}

impl Canonical<&str> {
    /// Returns `true` if this value has a length of zero bytes.
    ///
    /// datasketches-cpp ignores empty strings before hashing. Check this method before
    /// updating a sketch when matching that behavior matters.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::hash_value::canonical_str;
    /// assert!(canonical_str("").is_empty());
    /// assert!(!canonical_str("abc").is_empty());
    /// ```
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
