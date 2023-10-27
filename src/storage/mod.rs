pub mod attribute_resolver;
pub mod disk;
pub mod memory;
pub mod serde;

use crate::datom::*;
use crate::query::clause::*;
use crate::query::pattern::*;

#[derive(Default)]
pub struct Restricts {
    pub entity: Option<u64>,
    pub attribute: Option<u64>,
    pub value: Option<Value>,
    pub tx: Option<u64>,
}

impl Restricts {
    pub fn new() -> Self {
        Self {
            entity: None,
            attribute: None,
            value: None,
            tx: None,
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

pub trait ReadStorage<'a> {
    type Error: std::error::Error;
    type Iter: Iterator<Item = Result<Datom, Self::Error>>;

    /// Returns an iterator that yields all *non-retracted* datoms that match the search clause.
    /// Iterator might fail with `Self::Error` during iteration.
    /// Ordering of datoms is not guaranteed.
    fn find(&'a self, restricts: &Restricts) -> Self::Iter;

    #[deprecated]
    fn find_old(&'a self, clause: &Clause) -> Self::Iter {
        let mut restricts = Restricts::new();
        if let Pattern::Constant(entity) = clause.entity {
            restricts = restricts.with_entity(entity);
        }
        if let Pattern::Constant(AttributeIdentifier::Id(attribute)) = clause.attribute {
            restricts = restricts.with_attribute(attribute);
        }
        if let Pattern::Constant(value) = &clause.value {
            restricts = restricts.with_value(value.clone());
        }
        self.find(&restricts)
    }
}

pub trait WriteStorage {
    type Error: std::error::Error;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
}
