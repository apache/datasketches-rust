use crate::hll::HllType;
use crate::hll::array4::Array4;
use crate::hll::array6::Array6;
use crate::hll::array8::Array8;
use crate::hll::hash_set::HashSet;
use crate::hll::list::List;

#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    List { list: List, hll_type: HllType },
    Set { set: HashSet, hll_type: HllType },
    Array4(Array4),
    Array6(Array6),
    Array8(Array8),
}
