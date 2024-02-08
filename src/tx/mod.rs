pub mod transactor;

use std::collections::HashMap;
use std::rc::Rc;

use crate::datom::Datom;
use crate::datom::Value;
use crate::storage::attribute_resolver::ResolveError;
use thiserror::Error;

pub type Result<T, E> = std::result::Result<T, TransactionError<E>>;

pub enum OperatedEntity {
    New,             // Create a new entity and assign ID automatically.
    Id(u64),         // Update existing entity by ID.
    TempId(Rc<str>), // Use a temp ID within transaction.
}

pub enum AttributeValue {
    Value(Value),    // Set a concrete value to attribute.
    TempId(Rc<str>), // Reference a temp ID within transaction.
}

pub struct AttributeOperation {
    pub attribute: Rc<str>,
    pub value: AttributeValue,
}

pub struct EntityOperation {
    pub entity: OperatedEntity,
    pub attributes: Vec<AttributeOperation>,
}

impl EntityOperation {
    pub fn new(entity: OperatedEntity) -> Self {
        Self {
            entity,
            attributes: Vec::new(),
        }
    }

    pub fn on_new() -> Self {
        Self::new(OperatedEntity::New)
    }

    pub fn on_id(entity_id: u64) -> Self {
        Self::new(OperatedEntity::Id(entity_id))
    }

    pub fn on_temp_id(temp_id: &str) -> Self {
        Self::new(OperatedEntity::TempId(Rc::from(temp_id)))
    }

    pub fn set_value<V: Into<Value>>(self, attribute: &str, value: V) -> Self {
        self.set(Rc::from(attribute), AttributeValue::Value(value.into()))
    }

    pub fn set_reference(self, attribute: &str, temp_id: &str) -> Self {
        self.set(
            Rc::from(attribute),
            AttributeValue::TempId(Rc::from(temp_id)),
        )
    }

    fn set(mut self, attribute: Rc<str>, value: AttributeValue) -> Self {
        self.attributes
            .push(AttributeOperation { attribute, value });
        self
    }
}

#[derive(Default)]
pub struct Transaction {
    pub operations: Vec<EntityOperation>,
}

impl Transaction {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with<O: Into<EntityOperation>>(mut self, o: O) -> Self {
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
pub enum TransactionError<S> {
    #[error("storage error")]
    StorageError(#[from] S),
    #[error("invalid attribute type")]
    InvalidAttributeType,
    #[error("duplicate temp ID `{0}`")]
    DuplicateTempId(Rc<str>),
    #[error("temp ID `{0}` not found")]
    TempIdNotFound(Rc<str>),
    #[error("resolve error")]
    ResolveError(#[from] ResolveError<S>),
}
