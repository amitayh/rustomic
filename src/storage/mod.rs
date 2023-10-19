pub mod attribute_resolver;
pub mod disk;
pub mod memory;
pub mod memory2;
pub mod serde;

use std::rc::Rc;

use crate::datom::*;
use crate::query::clause::Clause;
use crate::query::pattern::*;
use crate::schema::attribute::*;
use crate::schema::*;
use thiserror::Error;

pub struct FindRequest {
    entity: Option<u64>,
    attribute: Option<u64>,
    value: Option<Value>,
}

pub trait ReadStorage<'a> {
    type Error: std::error::Error;
    type Iter: Iterator<Item = Result<Datom, Self::Error>>;

    fn find(&'a self, clause: &Clause) -> Self::Iter;

    fn resolve_ident(&'a self, ident: &str) -> Result<Option<Attribute>, Self::Error> {
        // [?attribute :db/attr/ident ?ident]
        let clause = Clause::new()
            .with_attribute(AttributePattern::Id(DB_ATTR_IDENT_ID))
            .with_value(ValuePattern::constant(ident.into()));
        if let Some(datom) = self.find(&clause).next() {
            return self.resolve_id(datom?.entity);
        }
        Ok(None)
    }

    fn resolve_id(&'a self, attribute_id: u64) -> Result<Option<Attribute>, Self::Error> {
        let mut builder = Builder::new(attribute_id);
        // [?attribute _ _]
        let clause = Clause::new().with_entity(EntityPattern::Id(attribute_id));
        for datom in self.find(&clause) {
            builder.consume(&datom?);
        }
        Ok(builder.build())
    }
}

pub trait WriteStorage {
    type Error: std::error::Error;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
}

// ------------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Attribute {
    pub id: u64,
    pub ident: Rc<str>,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
}

struct Builder {
    id: u64,
    ident: Option<Rc<str>>,
    value_type: Option<ValueType>,
    cardinality: Option<Cardinality>,
}

impl Builder {
    fn new(id: u64) -> Self {
        Self {
            id,
            ident: None,
            value_type: None,
            cardinality: None,
        }
    }

    fn consume(&mut self, datom: &Datom) {
        match datom.attribute {
            DB_ATTR_IDENT_ID => {
                self.ident = datom.value.as_string();
            }
            DB_ATTR_TYPE_ID => {
                self.value_type = datom.value.as_u64().and_then(ValueType::from);
            }
            DB_ATTR_CARDINALITY_ID => {
                self.cardinality = datom.value.as_u64().and_then(Cardinality::from);
            }
            _ => (),
        }
    }

    fn build(self) -> Option<Attribute> {
        let ident = self.ident?;
        let value_type = self.value_type?;
        let cardinality = self.cardinality?;
        Some(Attribute {
            id: self.id,
            ident,
            value_type,
            cardinality,
        })
    }
}

// ------------------------------------------------------------------------------------------------

#[derive(Debug, Error)]
#[error("storage error")]
pub struct StorageError;
