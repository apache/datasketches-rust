use crate::cpc::pair_table::PairTable;

pub(super) struct CompressedState {
    table_data: Vec<u32>,
    table_data_words: u32,
    // can be different from the number of entries in the sketch in hybrid mode
    table_num_entries: u32,
    window_data: Vec<u32>,
    window_data_words: u32,
}

pub(super) struct UncompressedState {
    table: PairTable,
    window: Vec<u8>,
}