use crate::datom;
use std::collections::HashMap;

pub enum Entity {
    New,            // Create a new entity and assign ID automatically.
    Id(u64),        // Update existing entity by ID.
    TempId(String), // Use a temp ID within transaction.
}

pub struct AttributeValue {
    pub attribute: String,
    pub value: datom::Value,
}

impl AttributeValue {
    pub fn new<V: Into<datom::Value>>(attribute: &str, value: V) -> AttributeValue {
        AttributeValue {
            attribute: String::from(attribute),
            value: value.into(),
        }
    }
}

pub struct Operation {
    pub entity: Entity,
    pub attributes: Vec<AttributeValue>,
}

pub struct Transaction {
    pub operations: Vec<Operation>,
}

pub struct TransctionResult {
    pub tx_data: Vec<datom::Datom>,
    pub temp_ids: HashMap<String, u64>,
}

#[derive(Debug)]
pub enum TransactionError {
    Error,
}
