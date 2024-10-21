use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::attribute_builder::*;
use crate::storage::ReadStorage;

use super::Restricts;

#[derive(Default)]
pub struct AttributeResolver {
    cache: Arc<RwLock<HashMap<Arc<str>, Option<Arc<Attribute>>>>>,
}

impl AttributeResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn resolve<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        ident: &Arc<str>,
        tx: u64,
    ) -> Result<Arc<Attribute>, ResolveError<S::Error>> {
        {
            let cache_read = self.cache.read().await;
            if let Some(attribute) = cache_read.get(ident) {
                return attribute
                    .clone()
                    .ok_or_else(|| ResolveError::IdentNotFound(Arc::clone(ident)));
            }
        }

        let result = resolve_by_ident(storage, Arc::clone(ident), tx)?;
        let mut cache_write = self.cache.write().await;
        cache_write.insert(Arc::clone(ident), result.clone());
        result.ok_or_else(|| ResolveError::IdentNotFound(Arc::clone(ident)))
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ResolveError<S> {
    #[error("storage error")]
    StorageError(#[from] S),
    #[error("ident `{0}` not found")]
    IdentNotFound(Arc<str>),
}

fn resolve_by_ident<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    ident: Arc<str>,
    tx: u64,
) -> Result<Option<Arc<Attribute>>, S::Error> {
    // [?attribute :db/attr/ident ?ident]
    let restricts = Restricts::new(tx)
        .with_attribute(DB_ATTR_IDENT_ID)
        .with_value(Value::Str(ident));
    if let Some(datom) = storage.find(restricts).next() {
        return resolve_by_id(storage, datom?.entity, tx);
    }
    Ok(None)
}

fn resolve_by_id<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    attribute_id: u64,
    tx: u64,
) -> Result<Option<Arc<Attribute>>, S::Error> {
    let mut builder = AttributeBuilder::new(attribute_id);
    // [?attribute _ _]
    let restricts = Restricts::new(tx).with_entity(attribute_id);
    for datom in storage.find(restricts) {
        builder.consume(datom?);
    }
    match builder.build() {
        Some(attribute) => Ok(Some(Arc::new(attribute))),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::clock::Instant;
    use crate::schema::default::default_datoms;
    use crate::storage::attribute_resolver::*;
    use crate::storage::memory::*;
    use crate::storage::*;
    use crate::tx::transactor;
    use crate::tx::Transaction;

    struct CountingStorage {
        inner: InMemoryStorage,
        count: Cell<usize>,
    }

    impl CountingStorage {
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

    impl<'a> ReadStorage<'a> for CountingStorage {
        type Error = <InMemoryStorage as ReadStorage<'a>>::Error;
        type Iter = <InMemoryStorage as ReadStorage<'a>>::Iter;

        fn find(&'a self, restricts: Restricts) -> Self::Iter {
            self.count.set(self.count.get() + 1);
            self.inner.find(restricts)
        }

        fn latest_entity_id(&self) -> Result<u64, Self::Error> {
            self.inner.latest_entity_id()
        }
    }

    impl<'a> WriteStorage for CountingStorage {
        type Error = <InMemoryStorage as WriteStorage>::Error;
        fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
            self.inner.save(datoms)
        }
    }

    fn create_storage<'a>() -> CountingStorage {
        let mut storage = CountingStorage::new();
        storage.save(&default_datoms()).unwrap();
        storage
    }

    #[tokio::test]
    async fn returns_none_when_attribute_does_not_exist() {
        let storage = create_storage();
        let mut resolver = AttributeResolver::new();
        let ident = Arc::from("foo/bar");
        let result = resolver.resolve(&storage, &ident, u64::MAX).await;
        assert!(result.is_err_and(|err| matches!(err, ResolveError::IdentNotFound(_))));
    }

    #[tokio::test]
    async fn resolves_existing_attribute() {
        let mut storage = create_storage();

        let mut resolver = AttributeResolver::new();
        let attribute = AttributeDefinition::new("foo/bar", ValueType::U64);
        let transaction = Transaction::new().with(attribute);
        let tx_result =
            transactor::transact(&storage, &mut resolver, Instant(0), transaction).await;
        assert!(tx_result.is_ok());
        assert!(storage.save(&tx_result.unwrap().tx_data).is_ok());

        let result = resolver
            .resolve(&storage, &Arc::from("foo/bar"), u64::MAX)
            .await;
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(Arc::from("foo/bar"), result.definition.ident);
        assert_eq!(ValueType::U64, result.definition.value_type);
    }

    #[tokio::test]
    async fn cache_hit() {
        let mut storage = create_storage();

        // No calls to `CountingStorage::find` yet.
        assert_eq!(0, storage.current_count());

        let mut resolver = AttributeResolver::new();
        let attribute = AttributeDefinition::new("foo/bar", ValueType::U64);
        let transaction = Transaction::new().with(attribute);
        let tx_result =
            transactor::transact(&storage, &mut resolver, Instant(0), transaction).await;
        assert!(tx_result.is_ok());
        assert!(storage.save(&tx_result.unwrap().tx_data).is_ok());

        let result1 = resolver
            .resolve(&storage, &Arc::from("foo/bar"), u64::MAX)
            .await;
        assert!(result1.is_ok());

        // Storage was used to resolve `foo/bar`.
        let queries = storage.current_count();
        assert!(queries > 0);

        let result2 = resolver
            .resolve(&storage, &Arc::from("foo/bar"), u64::MAX)
            .await;
        assert!(result2.is_ok());
        //assert_eq!(result1, result2);

        // No additional calls to storage were needed to resolve cached attribute.
        assert_eq!(queries, storage.current_count());
    }
}
