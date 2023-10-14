use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;

use crate::storage::Attribute;
use crate::storage::ReadStorage;

pub struct CachingAttributeResolver<S: ReadStorage> {
    // TODO should the hash maps the same reference?
    // or have `by_id` be `HashMap<u64, Rc<str>>`
    by_ident: HashMap<Rc<str>, Attribute>,
    by_id: HashMap<u64, Attribute>,
    storage: Arc<RwLock<S>>,
}

impl<S: ReadStorage> CachingAttributeResolver<S> {
    pub fn new(storage: Arc<RwLock<S>>) -> Self {
        Self {
            by_ident: HashMap::new(),
            by_id: HashMap::new(),
            storage,
        }
    }

    pub fn resolve_ident(&mut self, ident: &str) -> Result<Option<Attribute>, S::Error> {
        if let Some(attribute) = self.by_ident.get(ident) {
            return Ok(Some(attribute.clone()));
        }
        let result = self.storage.read().unwrap().resolve_ident(ident)?;
        if let Some(attribute) = result {
            self.update_cache(&attribute);
            return Ok(Some(attribute));
        }
        Ok(None)
    }

    pub fn resolve_id(&mut self, attribute_id: u64) -> Result<Option<Attribute>, S::Error> {
        if let Some(attribute) = self.by_id.get(&attribute_id) {
            return Ok(Some(attribute.clone()));
        }
        let result = self.storage.read().unwrap().resolve_id(attribute_id)?;
        if let Some(attribute) = result {
            self.update_cache(&attribute);
            return Ok(Some(attribute));
        }
        Ok(None)
    }

    fn update_cache(&mut self, attribute: &Attribute) {
        self.by_ident
            .insert(attribute.ident.clone(), attribute.clone());
        self.by_id.insert(attribute.id, attribute.clone());
    }
}
