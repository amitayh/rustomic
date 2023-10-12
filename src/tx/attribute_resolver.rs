use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;

use crate::datom::Datom;
use crate::query::clause::Clause;
use crate::query::pattern::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::*;

#[derive(Clone, Debug)]
pub struct Attribute {
    pub id: u64,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
}

pub trait AttributeResolver {
    type Error;

    fn resolve(&mut self, ident: &str) -> Result<Option<Attribute>, Self::Error>;
}

// ------------------------------------------------------------------------------------------------

pub struct StorageAttributeResolver<S: ReadStorage> {
    storage: Arc<RwLock<S>>,
}

impl<S: ReadStorage> StorageAttributeResolver<S> {
    pub fn new(storage: Arc<RwLock<S>>) -> Self {
        Self { storage }
    }
}

impl<S: ReadStorage> AttributeResolver for StorageAttributeResolver<S> {
    type Error = S::ReadError;

    fn resolve(&mut self, ident: &str) -> Result<Option<Attribute>, Self::Error> {
        let storage = self.storage.read().unwrap(); // TODO

        // [?attribute :db/attr/ident ?ident]
        let clause = Clause::new()
            .with_attribute(AttributePattern::Id(DB_ATTR_IDENT_ID))
            .with_value(ValuePattern::constant(ident.into()));

        if let Some(datom) = storage.find(&clause)?.next() {
            let attribute_id = datom.entity;
            let mut builder = Builder::new(attribute_id);
            // [?attribute _ _]
            let clause = Clause::new().with_entity(EntityPattern::Id(attribute_id));
            for datom in storage.find(&clause)? {
                builder.consume(&datom);
            }
            return Ok(builder.build());
        }

        Ok(None)
    }
}

struct Builder {
    id: u64,
    value_type: Option<ValueType>,
    cardinality: Option<Cardinality>,
}

impl Builder {
    fn new(id: u64) -> Self {
        Self {
            id,
            value_type: None,
            cardinality: None,
        }
    }

    fn consume(&mut self, datom: &Datom) {
        match datom.attribute {
            DB_ATTR_TYPE_ID => {
                self.value_type = datom.value.as_u64().and_then(ValueType::from);
            }
            DB_ATTR_CARDINALITY_ID => {
                self.cardinality = datom.value.as_u64().and_then(Cardinality::from);
            }
            _ => (),
        }
    }

    fn build(&self) -> Option<Attribute> {
        let id = self.id;
        let value_type = self.value_type?;
        let cardinality = self.cardinality?;
        Some(Attribute {
            id,
            value_type,
            cardinality,
        })
    }
}

// ------------------------------------------------------------------------------------------------

pub struct CachingAttributeResolver<Inner: AttributeResolver> {
    cache: HashMap<Rc<str>, Attribute>,
    inner: Inner,
}

impl<Inner: AttributeResolver> CachingAttributeResolver<Inner> {
    pub fn new(inner: Inner) -> Self {
        let cache = HashMap::new();
        Self { cache, inner }
    }
}

impl<Inner: AttributeResolver> AttributeResolver for CachingAttributeResolver<Inner> {
    type Error = Inner::Error;

    fn resolve(&mut self, ident: &str) -> Result<Option<Attribute>, Self::Error> {
        if let Some(attribute) = self.cache.get(ident) {
            return Ok(Some(attribute.clone()));
        }
        if let Some(attribute) = self.inner.resolve(ident)? {
            self.cache.insert(ident.into(), attribute.clone());
            return Ok(Some(attribute));
        }
        Ok(None)
    }
}
