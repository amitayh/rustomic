use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc, RwLock},
};

use crate::{
    query::{
        clause::Clause,
        pattern::{AttributePattern, ValuePattern},
    },
    schema::{
        attribute::{Cardinality, ValueType},
        DB_ATTR_IDENT_ID,
    },
    storage::Storage,
};

pub struct Attribute {
    pub id: u64,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
}

pub trait AttributeResolver {
    fn resolve(&self, ident: &str) -> Option<&Attribute>;
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
    fn resolve(&self, ident: &str) -> Option<&Attribute> {
        let storage = self.storage.read().unwrap();
        let clause = Clause::new()
            .with_attribute(AttributePattern::Id(DB_ATTR_IDENT_ID))
            .with_value(ValuePattern::constant(ident.into()));
        storage.find_datoms(&clause, u64::MAX);
        None
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
    fn resolve(&self, ident: &str) -> Option<&Attribute> {
        self.cache.get(ident).or_else(|| self.inner.resolve(ident))
    }
}
