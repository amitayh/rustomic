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
    pub ident: Rc<str>,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
}

pub trait AttributeResolver {
    type Error;

    fn resolve_ident(&mut self, ident: &str) -> Result<Option<Attribute>, Self::Error>;

    fn resolve_id(&mut self, attribute_id: u64) -> Result<Option<Attribute>, Self::Error>;
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
    type Error = S::Error;

    fn resolve_ident(&mut self, ident: &str) -> Result<Option<Attribute>, Self::Error> {
        // [?attribute :db/attr/ident ?ident]
        let clause = Clause::new()
            .with_attribute(AttributePattern::Id(DB_ATTR_IDENT_ID))
            .with_value(ValuePattern::constant(ident.into()));

        let mut datoms = self.storage.read().unwrap().find(&clause)?;
        if let Some(datom) = datoms.next() {
            return self.resolve_id(datom.entity);
        }

        Ok(None)
    }

    fn resolve_id(&mut self, attribute_id: u64) -> Result<Option<Attribute>, Self::Error> {
        let mut builder = Builder::new(attribute_id);
        // [?attribute _ _]
        let clause = Clause::new().with_entity(EntityPattern::Id(attribute_id));
        for datom in self.storage.read().unwrap().find(&clause)? {
            builder.consume(&datom);
        }
        return Ok(builder.build());
    }
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
        let id = self.id;
        let ident = self.ident?;
        let value_type = self.value_type?;
        let cardinality = self.cardinality?;
        Some(Attribute {
            id,
            ident,
            value_type,
            cardinality,
        })
    }
}

// ------------------------------------------------------------------------------------------------

pub struct CachingAttributeResolver<Inner: AttributeResolver> {
    // TODO should the hash maps the same reference?
    // or have `by_id` be `HashMap<u64, Rc<str>>`
    by_ident: HashMap<Rc<str>, Attribute>,
    by_id: HashMap<u64, Attribute>,
    inner: Inner,
}

impl<Inner: AttributeResolver> CachingAttributeResolver<Inner> {
    pub fn new(inner: Inner) -> Self {
        Self { by_ident: HashMap::new(), by_id: HashMap::new(), inner }
    }

    fn update_cache(&mut self, attribute: &Attribute) {
        self.by_ident.insert(attribute.ident.clone(), attribute.clone());
        self.by_id.insert(attribute.id, attribute.clone());
    }
}

impl<Inner: AttributeResolver> AttributeResolver for CachingAttributeResolver<Inner> {
    type Error = Inner::Error;

    fn resolve_ident(&mut self, ident: &str) -> Result<Option<Attribute>, Self::Error> {
        if let Some(attribute) = self.by_ident.get(ident) {
            return Ok(Some(attribute.clone()));
        }
        if let Some(attribute) = self.inner.resolve_ident(ident)? {
            self.update_cache(&attribute);
            return Ok(Some(attribute));
        }
        Ok(None)
    }

    fn resolve_id(&mut self, attribute_id: u64) -> Result<Option<Attribute>, Self::Error> {
        if let Some(attribute) = self.by_id.get(&attribute_id) {
            return Ok(Some(attribute.clone()));
        }
        if let Some(attribute) = self.inner.resolve_id(attribute_id)? {
            self.update_cache(&attribute);
            return Ok(Some(attribute));
        }
        Ok(None)
    }
}
