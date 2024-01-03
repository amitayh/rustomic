use std::collections::HashMap;
use std::rc::Rc;

use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::ReadStorage;

use super::Restricts;

#[derive(Default)]
pub struct AttributeResolver {
    cache: HashMap<Rc<str>, Attribute>,
}

impl AttributeResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn resolve<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        ident: &str,
        tx: u64,
    ) -> Result<Option<Attribute>, S::Error> {
        if let Some(attribute) = self.cache.get(ident) {
            // TODO: this implementation doesn't take schema changes into account
            return Ok(Some(attribute.clone()));
        }
        if let Some(attribute) = resolve_ident(storage, ident, tx)? {
            self.update_cache(attribute.clone());
            return Ok(Some(attribute));
        }
        Ok(None)
    }

    fn update_cache(&mut self, attribute: Attribute) {
        self.cache
            .insert(Rc::clone(&attribute.definition.ident), attribute);
    }
}

fn resolve_ident<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    ident: &str,
    tx: u64,
) -> Result<Option<Attribute>, S::Error> {
    // [?attribute :db/attr/ident ?ident]
    let restricts = Restricts::new(tx)
        .with_attribute(DB_ATTR_IDENT_ID)
        .with_value(ident.into());
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
    let mut builder = Builder::new(attribute_id);
    // [?attribute _ _]
    let restricts = Restricts::new(tx).with_entity(attribute_id);
    for datom in storage.find(restricts) {
        builder.consume(datom?);
    }
    Ok(builder.build())
}

// ------------------------------------------------------------------------------------------------

struct Builder {
    id: u64,
    ident: Option<Rc<str>>,
    value_type: Option<ValueType>,
    cardinality: Option<Cardinality>,
    doc: Option<Rc<str>>,
    unique: bool,
}

impl Builder {
    fn new(id: u64) -> Self {
        Self {
            id,
            ident: None,
            value_type: None,
            cardinality: None,
            doc: None,
            unique: false,
        }
    }

    fn consume(&mut self, datom: Datom) {
        match datom {
            Datom {
                attribute: DB_ATTR_IDENT_ID,
                value: Value::Str(ident),
                ..
            } => self.ident = Some(ident),
            Datom {
                attribute: DB_ATTR_TYPE_ID,
                value: Value::U64(value_type),
                ..
            } => self.value_type = ValueType::from(value_type),
            Datom {
                attribute: DB_ATTR_CARDINALITY_ID,
                value: Value::U64(cardinality),
                ..
            } => self.cardinality = Cardinality::from(cardinality),
            Datom {
                attribute: DB_ATTR_DOC_ID,
                value: Value::Str(doc),
                ..
            } => self.doc = Some(doc),
            Datom {
                attribute: DB_ATTR_UNIQUE_ID,
                value: Value::U64(1),
                ..
            } => self.unique = true,
            _ => (),
        }
    }

    fn build(self) -> Option<Attribute> {
        let ident = self.ident?;
        let value_type = self.value_type?;
        let cardinality = self.cardinality?;
        Some(Attribute {
            id: self.id,
            definition: AttributeDefinition {
                ident,
                value_type,
                cardinality,
                doc: self.doc,
                unique: self.unique,
            },
        })
    }
}

// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::clock::Instant;
    use crate::schema::attribute::{AttributeDefinition, ValueType};
    use crate::schema::default::default_datoms;
    use crate::storage::attribute_resolver::*;
    use crate::storage::memory::InMemoryStorage;
    use crate::storage::*;
    use crate::tx::transactor::Transactor;
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
    }

    impl WriteStorage for CountingStorage {
        type Error = <InMemoryStorage as WriteStorage>::Error;
        fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
            self.inner.save(datoms)
        }
    }

    fn create_storage() -> CountingStorage {
        let mut storage = CountingStorage::new();
        storage.save(&default_datoms()).unwrap();
        storage
    }

    #[test]
    fn returns_none_when_attribute_does_not_exist() {
        let storage = create_storage();
        let mut resolver = AttributeResolver::new();
        let result = resolver.resolve(&storage, "foo/bar", u64::MAX);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
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
        let result = resolver.resolve(&storage, "foo/bar", u64::MAX);
        assert!(result.is_ok());

        let result = result.unwrap().unwrap();
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
        let result1 = resolver.resolve(&storage, "foo/bar", u64::MAX);
        assert!(result1.is_ok());
        let result1 = result1.unwrap();
        assert!(result1.is_some());

        // Storage was used to resolve `foo/bar`.
        let queries = storage.current_count();
        assert!(queries > 0);

        let result2 = resolver.resolve(&storage, "foo/bar", u64::MAX);
        assert!(result2.is_ok());
        let result2 = result2.unwrap();
        assert_eq!(result1, result2);

        // No additional calls to storage were needed to resolve cached attribute.
        assert_eq!(queries, storage.current_count());
    }
}
