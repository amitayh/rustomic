pub mod transactor;

use std::collections::HashMap;

use crate::datom::Datom;
use crate::datom::Op;
use crate::datom::Value;
use crate::schema::attribute::ValueType;
use crate::storage::attribute_resolver::ResolveError;
use thiserror::Error;

pub type Result<T, E> = std::result::Result<T, TransactionError<E>>;

pub enum OperatedEntity {
    New,            // Create a new entity and assign ID automatically.
    Id(u64),        // Update existing entity by ID.
    TempId(String), // Use a temp ID within transaction.
}

pub enum AttributeValue {
    Value(Value),   // Set a concrete value to attribute.
    TempId(String), // Reference a temp ID within transaction.
}

pub struct AttributeOperation {
    // TODO: allow to reference an attribute by ID in addition to ident
    pub attribute: String,
    pub value: AttributeValue,
    pub op: Op,
}

pub struct EntityOperation {
    pub entity: OperatedEntity,
    pub attributes: Vec<AttributeOperation>,
}

impl EntityOperation {
    #[must_use]
    pub fn new(entity: OperatedEntity) -> Self {
        Self {
            entity,
            attributes: Vec::new(),
        }
    }

    #[must_use]
    pub fn on_new() -> Self {
        Self::new(OperatedEntity::New)
    }

    #[must_use]
    pub fn on_id(entity_id: u64) -> Self {
        Self::new(OperatedEntity::Id(entity_id))
    }

    #[must_use]
    pub fn on_temp_id(temp_id: &str) -> Self {
        Self::new(OperatedEntity::TempId(temp_id.to_string()))
    }

    #[must_use]
    pub fn assert(self, attribute: &str, value: impl Into<Value>) -> Self {
        self.set(
            attribute.to_string(),
            AttributeValue::Value(value.into()),
            Op::Assert,
        )
    }

    #[must_use]
    pub fn retract(self, attribute: &str, value: impl Into<Value>) -> Self {
        self.set(
            attribute.to_string(),
            AttributeValue::Value(value.into()),
            Op::Retract,
        )
    }

    #[must_use]
    pub fn set_reference(self, attribute: &str, temp_id: &str) -> Self {
        self.set(
            attribute.to_string(),
            AttributeValue::TempId(temp_id.to_string()),
            Op::Assert,
        )
    }

    fn set(mut self, attribute: String, value: AttributeValue, op: Op) -> Self {
        self.attributes.push(AttributeOperation {
            attribute,
            value,
            op,
        });
        self
    }
}

#[derive(Default)]
pub struct Transaction {
    pub operations: Vec<EntityOperation>,
}

impl Transaction {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with(mut self, o: impl Into<EntityOperation>) -> Self {
        self.operations.push(o.into());
        self
    }
}

#[derive(Debug)]
pub struct TransctionResult {
    pub tx_id: u64,
    pub tx_data: Vec<Datom>,
    pub temp_ids: HashMap<String, u64>,
}

#[derive(Debug, Error)]
pub enum TransactionError<S> {
    #[error("storage error")]
    StorageError(#[from] S),
    #[error("invalid attribute type")]
    InvalidAttributeType {
        attribute_id: u64,
        attribute_type: ValueType,
        value: Value,
    },
    #[error("duplicate temp ID `{0}`")]
    DuplicateTempId(String),
    #[error("temp ID `{0}` not found")]
    TempIdNotFound(String),
    #[error("resolve error")]
    ResolveError(#[from] ResolveError<S>),
    #[error("duplicate value for attribute {attribute}")]
    DuplicateUniqueValue { attribute: u64, value: Value },
}
