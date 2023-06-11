use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;

#[rustfmt::skip]
pub fn default_datoms() -> Vec<Datom> {
    let tx = 0u64;
    vec![
        // first transaction
        Datom::add(tx, DB_TX_TIME_ID, 0u64, tx),
        // "db/attr/ident" attribute
        Datom::add(DB_ATTR_IDENT_ID, DB_ATTR_IDENT_ID, DB_ATTR_IDENT_IDENT, tx),
        Datom::add(DB_ATTR_IDENT_ID, DB_ATTR_DOC_ID, "Human readable name of attribute", tx),
        Datom::add(DB_ATTR_IDENT_ID, DB_ATTR_TYPE_ID, ValueType::Str as u64, tx),
        Datom::add(DB_ATTR_IDENT_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        Datom::add(DB_ATTR_IDENT_ID, DB_ATTR_UNIQUE_ID, 1u64, tx),
        // "db/attr/doc" attribute
        Datom::add(DB_ATTR_DOC_ID, DB_ATTR_IDENT_ID, DB_ATTR_DOC_IDENT, tx),
        Datom::add(DB_ATTR_DOC_ID, DB_ATTR_DOC_ID, "Documentation of attribute", tx),
        Datom::add(DB_ATTR_DOC_ID, DB_ATTR_TYPE_ID, ValueType::Str as u64, tx),
        Datom::add(DB_ATTR_DOC_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        // "db/attr/type" attribute
        Datom::add(DB_ATTR_TYPE_ID, DB_ATTR_IDENT_ID, DB_ATTR_TYPE_IDENT, tx),
        Datom::add(DB_ATTR_TYPE_ID, DB_ATTR_DOC_ID, "Data type of attribute", tx),
        Datom::add(DB_ATTR_TYPE_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u64, tx),
        Datom::add(DB_ATTR_TYPE_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        // "db/attr/cardinality" attribute
        Datom::add(DB_ATTR_CARDINALITY_ID, DB_ATTR_IDENT_ID, DB_ATTR_CARDINALITY_IDENT, tx),
        Datom::add(DB_ATTR_CARDINALITY_ID, DB_ATTR_DOC_ID, "Cardinality of attribyte", tx),
        Datom::add(DB_ATTR_CARDINALITY_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u64, tx),
        Datom::add(DB_ATTR_CARDINALITY_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        // "db/attr/unique" attribute
        Datom::add(DB_ATTR_UNIQUE_ID, DB_ATTR_IDENT_ID, DB_ATTR_UNIQUE_IDENT, tx),
        Datom::add(DB_ATTR_UNIQUE_ID, DB_ATTR_DOC_ID, "Indicates this attribute is unique", tx),
        Datom::add(DB_ATTR_UNIQUE_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u64, tx),
        Datom::add(DB_ATTR_UNIQUE_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        // "db/tx/time" attribute
        Datom::add(DB_TX_TIME_ID, DB_ATTR_IDENT_ID, DB_TX_TIME_IDENT, tx),
        Datom::add(DB_TX_TIME_ID, DB_ATTR_DOC_ID, "Transaction's wall clock time", tx),
        Datom::add(DB_TX_TIME_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u64, tx),
        Datom::add(DB_TX_TIME_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
    ]
}
