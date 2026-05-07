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
pub fn canonical_f32(x: f32) -> Canonical<f32> {
    Canonical(x)
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
pub fn canonical_f64(x: f64) -> Canonical<f64> {
    Canonical(x)
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
