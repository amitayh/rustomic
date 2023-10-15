use std::collections::HashMap;
use std::rc::Rc;

use crate::storage::Attribute;
use crate::storage::ReadStorage;

pub struct CachingAttributeResolver {
    // TODO should the hash maps the same reference?
    // or have `by_id` be `HashMap<u64, Rc<str>>`
    by_ident: HashMap<Rc<str>, Attribute>,
    by_id: HashMap<u64, Attribute>,
}

impl CachingAttributeResolver {
    pub fn new() -> Self {
        Self {
            by_ident: HashMap::new(),
            by_id: HashMap::new(),
        }
    }

    pub fn resolve_ident<S: ReadStorage>(
        &mut self,
        storage: &S,
        ident: &str,
    ) -> Result<Option<Attribute>, S::Error> {
        if let Some(attribute) = self.by_ident.get(ident) {
            return Ok(Some(attribute.clone()));
        }
        if let Some(attribute) = storage.resolve_ident(ident)? {
            self.update_cache(&attribute);
            return Ok(Some(attribute));
        }
        Ok(None)
    }

    pub fn resolve_id<S: ReadStorage>(
        &mut self,
        storage: &S,
        attribute_id: u64,
    ) -> Result<Option<Attribute>, S::Error> {
        if let Some(attribute) = self.by_id.get(&attribute_id) {
            return Ok(Some(attribute.clone()));
        }
        if let Some(attribute) = storage.resolve_id(attribute_id)? {
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
