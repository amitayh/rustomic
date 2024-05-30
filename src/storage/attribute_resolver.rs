use std::collections::HashMap;
use std::rc::Rc;
use thiserror::Error;

use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::attribute_builder::*;
use crate::storage::ReadStorage;

use super::Restricts;

#[derive(Default)]
pub struct AttributeResolver {
    cache: HashMap<Rc<str>, Option<Attribute>>,
}

impl AttributeResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn resolve<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        ident: Rc<str>,
        tx: u64,
    ) -> Result<&Attribute, ResolveError<S::Error>> {
        let result = self.cache.entry(Rc::clone(&ident)).or_default();
        if result.is_none() {
            *result = resolve_ident(storage, Rc::clone(&ident), tx)?;
        }
        result.as_ref().ok_or(ResolveError::IdentNotFound(ident))
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ResolveError<S> {
    #[error("storage error")]
    StorageError(#[from] S),
    #[error("ident `{0}` not found")]
    IdentNotFound(Rc<str>),
}

fn resolve_ident<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    ident: Rc<str>,
    tx: u64,
) -> Result<Option<Attribute>, S::Error> {
    // [?attribute :db/attr/ident ?ident]
    let restricts = Restricts::new(tx)
        .with_attribute(DB_ATTR_IDENT_ID)
        .with_value(Value::Str(ident));
    if let Some(datom) = storage.find(restricts).next() {
        return resolve_id(storage, datom?.entity, tx);
    }
    Ok(None)
}

fn resolve_id<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    attribute_id: u64,
    tx: u64,
) -> Result<Option<Attribute>, S::Error> {
    let mut builder = AttributeBuilder::new(attribute_id);
    // [?attribute _ _]
    let restricts = Restricts::new(tx).with_entity(attribute_id);
    for datom in storage.find(restricts) {
        builder.consume(datom?);
    }
    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::clock::Instant;
    use crate::schema::default::default_datoms;
    use crate::storage::attribute_resolver::*;
    use crate::storage::memory::*;
    use crate::storage::*;
    use crate::tx::transactor::Transactor;
    use crate::tx::Transaction;

    struct CountingStorage<'a> {
        inner: InMemoryStorage<'a>,
        count: Cell<usize>,
    }

    impl<'a> CountingStorage<'a> {
        fn new() -> Self {
            Self {
                inner: InMemoryStorage::new(),
                count: Cell::new(0),
            }
        }

        fn current_count(&self) -> usize {
            self.count.get()
        }
    }

    impl<'a> ReadStorage<'a> for CountingStorage<'a> {
        type Error = <InMemoryStorage<'a> as ReadStorage<'a>>::Error;
        type Iter = <InMemoryStorage<'a> as ReadStorage<'a>>::Iter;

        fn find(&'a self, restricts: Restricts) -> Self::Iter {
            self.count.set(self.count.get() + 1);
            self.inner.find(restricts)
        }

        fn latest_entity_id(&self) -> Result<u64, Self::Error> {
            self.inner.latest_entity_id()
        }
    }

    impl<'a> WriteStorage for CountingStorage<'a> {
        type Error = <InMemoryStorage<'a> as WriteStorage>::Error;
        fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
            self.inner.save(datoms)
        }
    }

    fn create_storage<'a>() -> CountingStorage<'a> {
        let mut storage = CountingStorage::new();
        storage.save(&default_datoms()).unwrap();
        storage
    }

    #[test]
    fn returns_none_when_attribute_does_not_exist() {
        let storage = create_storage();
        let mut resolver = AttributeResolver::new();
        let ident = Rc::from("foo/bar");
        let result = resolver.resolve(&storage, Rc::clone(&ident), u64::MAX);
        assert!(result.is_err_and(|err| err == ResolveError::IdentNotFound(ident)));
    }

    #[test]
    fn resolves_existing_attribute() {
        let mut storage = create_storage();
        let mut transactor = Transactor::new();

        let attribute = AttributeDefinition::new("foo/bar", ValueType::U64);
        let transaction = Transaction::new().with(attribute);
        let tx_result = transactor.transact(&storage, Instant(0), transaction);
        assert!(tx_result.is_ok());
        assert!(storage.save(&tx_result.unwrap().tx_data).is_ok());

        let mut resolver = AttributeResolver::new();
        let result = resolver.resolve(&storage, Rc::from("foo/bar"), u64::MAX);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(Rc::from("foo/bar"), result.definition.ident);
        assert_eq!(ValueType::U64, result.definition.value_type);
    }

    #[test]
    fn cache_hit() {
        let mut storage = create_storage();
        let mut transactor = Transactor::new();

        // No calls to `CountingStorage::find` yet.
        assert_eq!(0, storage.current_count());

        let attribute = AttributeDefinition::new("foo/bar", ValueType::U64);
        let transaction = Transaction::new().with(attribute);
        let tx_result = transactor.transact(&storage, Instant(0), transaction);
        assert!(tx_result.is_ok());
        assert!(storage.save(&tx_result.unwrap().tx_data).is_ok());

        let mut resolver = AttributeResolver::new();
        let result1 = resolver
            .resolve(&storage, Rc::from("foo/bar"), u64::MAX)
            .cloned();
        assert!(result1.is_ok());

        // Storage was used to resolve `foo/bar`.
        let queries = storage.current_count();
        assert!(queries > 0);

        let result2 = resolver
            .resolve(&storage, Rc::from("foo/bar"), u64::MAX)
            .cloned();
        assert!(result2.is_ok());
        assert_eq!(result1, result2);

        // No additional calls to storage were needed to resolve cached attribute.
        assert_eq!(queries, storage.current_count());
    }
}
