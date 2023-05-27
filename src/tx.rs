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

pub struct Operation {
    pub entity: Entity,
    pub attributes: Vec<AttributeValue>,
}

impl Operation {
    pub fn on_new() -> Self {
        Operation {
            entity: Entity::New,
            attributes: Vec::new(),
        }
    }

    pub fn on_id(entity_id: u64) -> Self {
        Operation {
            entity: Entity::Id(entity_id),
            attributes: Vec::new(),
        }
    }

    pub fn on_temp_id(temp_id: &str) -> Self {
        Operation {
            entity: Entity::TempId(String::from(temp_id)),
            attributes: Vec::new(),
        }
    }

    pub fn set<V: Into<datom::Value>>(mut self, attribute: &str, value: V) -> Self {
        self.set_mut(attribute, value);
        self
    }

    pub fn set_mut<V: Into<datom::Value>>(&mut self, attribute: &str, value: V) {
        self.attributes.push(AttributeValue {
            attribute: String::from(attribute),
            value: value.into(),
        });
    }
}

pub struct Transaction {
    pub operations: Vec<Operation>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            operations: Vec::new(),
        }
    }

    pub fn with(mut self, operation: Operation) -> Self {
        self.operations.push(operation);
        self
    }
}

pub struct TransctionResult {
    pub tx_data: Vec<datom::Datom>,
    pub temp_ids: HashMap<String, u64>,
}

#[derive(Debug)]
pub enum TransactionError {
    Error,
}
