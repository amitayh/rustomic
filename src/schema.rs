use std::collections::HashMap;

use crate::datom;
use crate::tx;

pub const DB_ATTR_IDENT_IDENT: &str = "db/attr/ident";
pub const DB_ATTR_IDENT_ID: u64 = 1;

pub const DB_ATTR_CARDINALITY_IDENT: &str = "db/attr/cardinality";
pub const DB_ATTR_CARDINALITY_ID: u64 = 2;

pub const DB_ATTR_TYPE_IDENT: &str = "db/attr/type";
pub const DB_ATTR_TYPE_ID: u64 = 3;

pub const DB_ATTR_DOC_IDENT: &str = "db/attr/doc";
pub const DB_ATTR_DOC_ID: u64 = 4;

pub const DB_ATTR_UNIQUE_IDENT: &str = "db/attr/unique";
pub const DB_ATTR_UNIQUE_ID: u64 = 5;

pub const DB_TX_TIME_IDENT: &str = "db/tx/time";
pub const DB_TX_TIME_ID: u64 = 6;

#[derive(PartialEq, Eq)]
pub enum ValueType {
    U8 = 0,
    I32 = 1,
    U32 = 2,
    I64 = 3,
    U64 = 4,
    Str = 5,
    Ref = 6,
}

impl ValueType {
    pub fn from(value: u8) -> Option<ValueType> {
        match value {
            0 => Some(ValueType::U8),
            1 => Some(ValueType::I32),
            2 => Some(ValueType::U32),
            3 => Some(ValueType::I64),
            4 => Some(ValueType::U64),
            5 => Some(ValueType::Str),
            6 => Some(ValueType::Ref),
            _ => None,
        }
    }
}

impl datom::Value {
    pub fn matches_type(&self, value_type: ValueType) -> bool {
        match self {
            datom::Value::U8(_) => value_type == ValueType::U8,
            datom::Value::I32(_) => value_type == ValueType::I32,
            datom::Value::U32(_) => value_type == ValueType::U32,
            datom::Value::I64(_) => value_type == ValueType::I64,
            datom::Value::U64(_) => value_type == ValueType::U64 || value_type == ValueType::Ref,
            datom::Value::Str(_) => value_type == ValueType::Str,
            _ => false,
        }
    }
}

pub enum Cardinality {
    One = 0,
    Many = 1,
}

pub struct Attribute {
    ident: String,
    value_type: ValueType,
    cardinality: Cardinality,
    doc: Option<String>,
    unique: bool,
}

impl Attribute {
    pub fn new(ident: &str, value_type: ValueType, cardinality: Cardinality) -> Self {
        Attribute {
            ident: String::from(ident),
            value_type,
            cardinality,
            doc: None,
            unique: false,
        }
    }

    pub fn with_doc(mut self, doc: &str) -> Self {
        self.doc = Some(String::from(doc));
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn build(self) -> tx::Operation {
        let mut operation = tx::Operation::on_new()
            .set(DB_ATTR_IDENT_IDENT, self.ident)
            .set(DB_ATTR_CARDINALITY_IDENT, self.cardinality as u8)
            .set(DB_ATTR_TYPE_IDENT, self.value_type as u8);
        if let Some(doc) = self.doc {
            operation.set_mut(DB_ATTR_DOC_IDENT, doc);
        }
        if self.unique {
            operation.set_mut(DB_ATTR_UNIQUE_IDENT, 1u8);
        }
        operation
    }
}

#[rustfmt::skip]
pub fn default_datoms(tx: u64) -> Vec<datom::Datom> {
    vec![
        // "db/attr/ident" attribute
        datom::Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_IDENT_ID, DB_ATTR_IDENT_IDENT, tx),
        datom::Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_DOC_ID, "Human readable name of attribute", tx),
        datom::Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_TYPE_ID, ValueType::Str as u8, tx),
        datom::Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        datom::Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_UNIQUE_ID, 1u8, tx),
        // "db/attr/doc" attribute
        datom::Datom::new(DB_ATTR_DOC_ID, DB_ATTR_IDENT_ID, DB_ATTR_DOC_IDENT, tx),
        datom::Datom::new(DB_ATTR_DOC_ID, DB_ATTR_DOC_ID, "Documentation of attribute", tx),
        datom::Datom::new(DB_ATTR_DOC_ID, DB_ATTR_TYPE_ID, ValueType::Str as u8, tx),
        datom::Datom::new(DB_ATTR_DOC_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // "db/attr/type" attribute
        datom::Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_IDENT_ID, DB_ATTR_TYPE_IDENT, tx),
        datom::Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_DOC_ID, "Data type of attribute", tx),
        datom::Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_TYPE_ID, ValueType::U8 as u8, tx),
        datom::Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // "db/attr/cardinality" attribute
        datom::Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_IDENT_ID, DB_ATTR_CARDINALITY_IDENT, tx),
        datom::Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_DOC_ID, "Cardinality of attribyte", tx),
        datom::Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_TYPE_ID, ValueType::U8 as u8, tx),
        datom::Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // "db/attr/unique" attribute
        datom::Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_IDENT_ID, DB_ATTR_UNIQUE_IDENT, tx),
        datom::Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_DOC_ID, "Indicates this attribute is unique", tx),
        datom::Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_TYPE_ID, ValueType::U8 as u8, tx),
        datom::Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // "db/tx/time" attribute
        datom::Datom::new(DB_TX_TIME_ID, DB_ATTR_IDENT_ID, DB_TX_TIME_IDENT, tx),
        datom::Datom::new(DB_TX_TIME_ID, DB_ATTR_DOC_ID, "Transaction's wall clock time", tx),
        datom::Datom::new(DB_TX_TIME_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u8, tx),
        datom::Datom::new(DB_TX_TIME_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // first transaction
        datom::Datom::new(tx, DB_TX_TIME_ID, 0u64, tx),
    ]
}

pub fn default_ident_to_entity() -> HashMap<String, u64> {
    let mut ident_to_entity = HashMap::new();
    ident_to_entity.insert(String::from(DB_ATTR_IDENT_IDENT), DB_ATTR_IDENT_ID);
    ident_to_entity.insert(String::from(DB_ATTR_TYPE_IDENT), DB_ATTR_TYPE_ID);
    ident_to_entity.insert(
        String::from(DB_ATTR_CARDINALITY_IDENT),
        DB_ATTR_CARDINALITY_ID,
    );
    ident_to_entity.insert(String::from(DB_ATTR_DOC_IDENT), DB_ATTR_DOC_ID);
    ident_to_entity.insert(String::from(DB_TX_TIME_IDENT), DB_TX_TIME_ID);
    ident_to_entity
}
