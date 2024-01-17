pub mod attribute_resolver;
pub mod disk;
pub mod memory;
pub mod serde;

use std::collections::HashMap;
use std::rc::Rc;

use crate::datom::*;
use crate::query::clause::*;
use crate::query::pattern::*;

#[derive(Default, Debug, Clone)]
pub struct Restricts {
    pub entity: Option<u64>,
    pub attribute: Option<u64>,
    pub value: Option<Value>,
    pub tx: Option<u64>,
    pub basis_tx: u64,
}

impl Restricts {
    pub fn new(basis_tx: u64) -> Self {
        Self {
            entity: None,
            attribute: None,
            value: None,
            tx: None,
            basis_tx,
        }
    }

    pub fn from(
        pattern: &DataPattern,
        assignment: &HashMap<Rc<str>, Value>,
        basis_tx: u64,
    ) -> Self {
        let entity = match pattern.entity {
            Pattern::Constant(entity) => Some(entity),
            Pattern::Variable(ref variable) => match assignment.get(variable) {
                Some(&Value::Ref(entity)) => Some(entity),
                _ => None,
            },
            _ => None,
        };
        let attribute = match pattern.attribute {
            Pattern::Constant(AttributeIdentifier::Id(attribute)) => Some(attribute),
            Pattern::Variable(ref variable) => match assignment.get(variable) {
                Some(&Value::Ref(entity)) => Some(entity),
                _ => None,
            },
            _ => None,
        };
        let value = match pattern.value {
            Pattern::Constant(ref value) => Some(value.clone()),
            Pattern::Variable(ref variable) => assignment.get(variable).cloned(),
            _ => None,
        };
        let tx = match pattern.tx {
            Pattern::Constant(tx) => Some(tx),
            Pattern::Variable(ref variable) => match assignment.get(variable) {
                Some(&Value::Ref(entity)) => Some(entity),
                _ => None,
            },
            _ => None,
        };
        Self {
            entity,
            attribute,
            value,
            tx,
            basis_tx,
        }
    }

    pub fn with_entity(mut self, entity: u64) -> Self {
        self.entity = Some(entity);
        self
    }

    pub fn with_attribute(mut self, attribute: u64) -> Self {
        self.attribute = Some(attribute);
        self
    }

    pub fn with_value(mut self, value: Value) -> Self {
        self.value = Some(value);
        self
    }

    pub fn with_tx(mut self, tx: u64) -> Self {
        self.tx = Some(tx);
        self
    }
}

impl Datom {
    pub fn satisfies(&self, restricts: &Restricts) -> bool {
        self.op == Op::Added
            && self.tx <= restricts.basis_tx
            && restricts
                .entity
                .map_or(true, |entity| self.entity == entity)
            && restricts
                .attribute
                .map_or(true, |attribute| self.attribute == attribute)
            && restricts
                .value
                .as_ref()
                .map_or(true, |value| &self.value == value)
            && restricts.tx.map_or(true, |tx| self.tx == tx)
    }
}

pub trait ReadStorage {
    type Error: std::error::Error;

    /// Returns an iterator that yields all *non-retracted* datoms that match the restircts.
    /// Iterator might fail with `Self::Error` during iteration.
    /// Ordering of datoms is not guaranteed.
    fn find(&self, restricts: Restricts) -> impl Iterator<Item = Result<Datom, Self::Error>>;
}

pub trait WriteStorage {
    type Error: std::error::Error;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
}
