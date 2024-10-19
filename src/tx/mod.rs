pub mod transactor;

use std::collections::HashMap;
use std::sync::Arc;

use crate::datom::Datom;
use crate::datom::Op;
use crate::datom::Value;
use crate::schema::attribute::ValueType;
use crate::storage::attribute_resolver::ResolveError;
use thiserror::Error;

pub type Result<T, E> = std::result::Result<T, TransactionError<E>>;

pub enum OperatedEntity {
    New,              // Create a new entity and assign ID automatically.
    Id(u64),          // Update existing entity by ID.
    TempId(Arc<str>), // Use a temp ID within transaction.
}

pub enum AttributeValue {
    Value(Value),     // Set a concrete value to attribute.
    TempId(Arc<str>), // Reference a temp ID within transaction.
}

pub struct AttributeOperation {
    // TODO: allow to reference an attribute by ID in addition to ident
    pub attribute: Arc<str>,
    pub value: AttributeValue,
    pub op: Op,
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
        Self::new(OperatedEntity::TempId(Arc::from(temp_id)))
    }

    pub fn assert(self, attribute: &str, value: impl Into<Value>) -> Self {
        self.set(
            Arc::from(attribute),
            AttributeValue::Value(value.into()),
            Op::Assert,
        )
    }

    pub fn retract(self, attribute: &str, value: impl Into<Value>) -> Self {
        self.set(
            Arc::from(attribute),
            AttributeValue::Value(value.into()),
            Op::Retract,
        )
    }

    pub fn set_reference(self, attribute: &str, temp_id: &str) -> Self {
        self.set(
            Arc::from(attribute),
            AttributeValue::TempId(Arc::from(temp_id)),
            Op::Assert,
        )
    }

    fn set(mut self, attribute: Arc<str>, value: AttributeValue, op: Op) -> Self {
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, o: impl Into<EntityOperation>) -> Self {
        self.operations.push(o.into());
        self
    }
}

#[derive(Debug)]
pub struct TransctionResult {
    pub tx_id: u64,
    pub tx_data: Vec<Datom>,
    pub temp_ids: HashMap<Arc<str>, u64>,
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
    DuplicateTempId(Arc<str>),
    #[error("temp ID `{0}` not found")]
    TempIdNotFound(Arc<str>),
    #[error("resolve error")]
    ResolveError(#[from] ResolveError<S>),
    #[error("duplicate value for attribute {attribute}")]
    DuplicateUniqueValue { attribute: u64, value: Value },
}
