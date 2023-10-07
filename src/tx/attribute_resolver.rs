use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::query::clause::Clause;
use crate::query::pattern::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::*;

#[derive(Clone)]
pub struct Attribute {
    pub id: u64,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
}

pub trait AttributeResolver {
    fn resolve(&mut self, ident: &str) -> Option<&Attribute>;
}

pub struct StorageAttributeResolver<'a, S: ReadStorage<'a>> {
    storage: Arc<S>,
    _marker: PhantomData<&'a S>,
}

impl<'a, S: ReadStorage<'a>> StorageAttributeResolver<'a, S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage, _marker: PhantomData }
    }
}

impl<'a, S: ReadStorage<'a>> AttributeResolver for StorageAttributeResolver<'a, S> {
    fn resolve(&mut self, ident: &str) -> Option<&Attribute> {
        let clause = Clause::new()
            .with_attribute(AttributePattern::Id(DB_ATTR_IDENT_ID))
            .with_value(ValuePattern::constant(ident.into()));
        //self.storage.find(&clause);
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
    fn resolve(&mut self, ident: &str) -> Option<&Attribute> {
        Some(self.cache.entry(ident.into()).or_insert_with(|| self.inner.resolve(ident).unwrap().clone()))
        //let mut result = self.cache.get(ident);
        //if result.is_none() {
        //    if let Some(attribute) = self.inner.resolve(ident) {
        //        self.cache.insert(ident.into(), attribute.clone());
        //        result = Some(attribute);
        //    }
        //}
        //result
    }
}
