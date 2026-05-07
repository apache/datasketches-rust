//! HashValue

mod canonical;

use std::hash::Hash;
use std::hash::Hasher;

pub use self::canonical::Canonical;
pub use self::canonical::canonical_f32;
pub use self::canonical::canonical_f64;
pub use self::canonical::canonical_i8;
pub use self::canonical::canonical_i16;
pub use self::canonical::canonical_i32;
pub use self::canonical::canonical_i64;
pub use self::canonical::canonical_slice;
pub use self::canonical::canonical_str;
pub use self::canonical::canonical_string;
pub use self::canonical::canonical_u8;
pub use self::canonical::canonical_u16;
pub use self::canonical::canonical_u32;
pub use self::canonical::canonical_u64;
pub use self::canonical::canonical_vec;

#[doc(hidden)] // for doctest
pub fn calculate_hash<T: Hash>(t: T) -> u64 {
    use std::hash::DefaultHasher;

    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
