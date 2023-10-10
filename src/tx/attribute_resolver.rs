use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;

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

    fn resolve(&mut self, ident: &str) -> Option<Attribute>;
}

pub struct StorageAttributeResolver<S: Storage> {
    storage: Arc<RwLock<S>>,
}

impl<S: Storage> StorageAttributeResolver<S> {
    pub fn new(storage: Arc<RwLock<S>>) -> Self {
        Self { storage }
    }
}

impl<S: Storage> AttributeResolver for StorageAttributeResolver<S> {
    type Error = S::Error;

    fn resolve(&mut self, ident: &str) -> Option<Attribute> {
        let storage = self.storage.read().unwrap(); // TODO
        let attribute_id = storage
            .find(
                &Clause::new()
                    .with_attribute(AttributePattern::Id(DB_ATTR_IDENT_ID))
                    .with_value(ValuePattern::constant(ident.into())),
            )
            .unwrap() // TODO
            .first()?
            .entity;

        let mut cardinality = None;
        let mut value_type = None;
        let clause = Clause::new().with_entity(EntityPattern::Id(attribute_id));
        for datom in storage.find(&clause).unwrap() {
            // TODO
            match datom.attribute {
                DB_ATTR_TYPE_ID => {
                    value_type = datom.value.as_u64().and_then(ValueType::from);
                }
                DB_ATTR_CARDINALITY_ID => {
                    cardinality = datom.value.as_u64().and_then(Cardinality::from);
                }
                _ => (),
            }
        }
        Some(Attribute {
            id: attribute_id,
            value_type: value_type?,
            cardinality: cardinality?,
        })
    }
}

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

    fn resolve(&mut self, ident: &str) -> Option<Attribute> {
        self.cache.get(ident).cloned().or_else(|| {
            let result = self.inner.resolve(ident);
            if let Some(attribute) = &result {
                self.cache.insert(ident.into(), attribute.clone());
            }
            result
        })
    }
}
