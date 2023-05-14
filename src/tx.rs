use crate::datom;

pub enum EntityIdentifier {
    Existing(u64),
    Temp(String),
}

pub struct AttributeValue {
    attribute: String,
    value: datom::Value,
}

pub enum Operation {
    Add {
        entity: EntityIdentifier,
        attribute: u64,
        value: datom::Value,
    },
    Retract {
        entity: u64,
        attribute: u64,
    },
}

pub struct Transaction {
    pub operations: Vec<Operation>,
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum Cardinality {
    One,
    Many,
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum ValueType {
    Ref,
    Str,
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Attribute {
    pub ident: String,
    pub cardinality: Cardinality,
    pub value_type: ValueType,
    pub doc: Option<String>,
}
