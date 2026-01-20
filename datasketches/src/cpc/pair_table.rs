/// A highly specialized hash table used for sparse data.
///
/// This table stores `(row, col)` pairs and uses linear probing for collision resolution. It is
/// optimized for scenarios where the cardinality of entries is low.
pub(crate) struct PairTable {
    pub keys: Vec<u64>,
    pub values: Vec<u8>,
}
