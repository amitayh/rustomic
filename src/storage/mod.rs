pub mod attribute_resolver;
pub mod disk;
pub mod memory;
pub mod serde;

use crate::datom::*;

#[derive(Default, Debug)]
pub struct Restricts {
    pub entity: Option<u64>,
    pub attribute: Option<u64>,
    pub value: Option<Value>,
    pub tx: Option<u64>,
    pub tx2: u64,
}

impl Restricts {
    pub fn new(tx: u64) -> Self {
        Self {
            entity: None,
            attribute: None,
            value: None,
            tx: None,
            tx2: tx,
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
    fn find(&'a self, restricts: Restricts) -> Self::Iter;
}

pub trait WriteStorage {
    type Error: std::error::Error;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
}
