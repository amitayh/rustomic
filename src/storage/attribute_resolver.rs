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

    pub fn resolve_ident<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
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

    pub fn resolve_id<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
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
            .insert(Rc::clone(&attribute.ident), attribute.clone());
        self.by_id.insert(attribute.id, attribute.clone());
    }
}

#[cfg(test)]
mod tests {
    mod resolve_ident {
        use crate::clock::Instant;
        use crate::schema::attribute::{Attribute, ValueType};
        use crate::schema::default::default_datoms;
        use crate::storage::attribute_resolver::*;
        use crate::storage::memory2::InMemoryStorage;
        use crate::storage::*;
        use crate::tx::transactor::Transactor;
        use crate::tx::Transaction;

        fn create_storage() -> InMemoryStorage {
            let mut storage = InMemoryStorage::new();
            storage.save(&default_datoms()).unwrap();
            storage
        }

        #[test]
        fn returns_none_when_attribute_does_not_exist() {
            let storage = create_storage();
            let mut resolver = CachingAttributeResolver::new();
            let result = resolver.resolve_ident(&storage, "foo/bar");
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        #[test]
        fn resolves_existing_attribute() {
            let mut storage = create_storage();
            let mut transactor = Transactor::new();

            let attribute = Attribute::new("foo/bar", ValueType::U64);
            let transaction = Transaction::new().with(attribute);
            let tx_result = transactor.transact(&storage, Instant(0), transaction);
            assert!(tx_result.is_ok());
            assert!(storage.save(&tx_result.unwrap().tx_data).is_ok());

            let mut resolver = CachingAttributeResolver::new();
            let result = resolver.resolve_ident(&storage, "foo/bar");
            assert!(result.is_ok());
            //assert_eq!(Some(attribute), result.unwrap());
        }
    }

    mod reolve_id {}
}
