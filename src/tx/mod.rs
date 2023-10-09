pub mod attribute_resolver;
pub mod transactor;

use std::collections::HashMap;
use std::rc::Rc;

use crate::datom::Datom;
use crate::datom::Value;
use thiserror::Error;

pub enum Entity {
    New,             // Create a new entity and assign ID automatically.
    Id(u64),         // Update existing entity by ID.
    TempId(Rc<str>), // Use a temp ID within transaction.
}

pub struct AttributeValue {
    pub attribute: Rc<str>,
    pub value: Value,
}

pub struct Operation {
    pub entity: Entity,
    pub attributes: Vec<AttributeValue>,
}

impl Operation {
    pub fn new(entity: Entity) -> Self {
        Operation {
            entity,
            attributes: Vec::new(),
        }
    }

    pub fn on_new() -> Self {
        Self::new(Entity::New)
    }

    pub fn on_id(entity_id: u64) -> Self {
        Self::new(Entity::Id(entity_id))
    }

    pub fn on_temp_id(temp_id: &str) -> Self {
        Self::new(Entity::TempId(Rc::from(temp_id)))
    }

    pub fn set<V: Into<Value>>(mut self, attribute: &str, value: V) -> Self {
        self.set_mut(attribute, value);
        self
    }

    pub fn set_mut<V: Into<Value>>(&mut self, attribute: &str, value: V) {
        self.attributes.push(AttributeValue {
            attribute: Rc::from(attribute),
            value: value.into(),
        });
    }
}

#[derive(Default)]
pub struct Transaction {
    pub operations: Vec<Operation>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            operations: Vec::new(),
        }
    }

    pub fn with<O: Into<Operation>>(mut self, o: O) -> Self {
        self.operations.push(o.into());
        self
    }
}

#[derive(Debug)]
pub struct TransctionResult {
    pub tx_id: u64,
    pub tx_data: Vec<Datom>,
    pub temp_ids: HashMap<Rc<str>, u64>,
}

#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("error")]
    Error, // TODO: remove generic error
    #[error("invalid attribute type")]
    InvalidAttributeType,
    #[error("duplicate temp ID `{0}`")]
    DuplicateTempId(Rc<str>),
    #[error("temp ID `{0}` not found")]
    TempIdNotFound(Rc<str>),
    #[error("storage error")]
    StorageError(#[from] crate::storage::StorageError),
}
