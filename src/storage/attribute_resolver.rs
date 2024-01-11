use std::collections::HashMap;
use std::rc::Rc;
use thiserror::Error;

use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;
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

    pub fn resolve<S: ReadStorage>(
        &mut self,
        storage: &S,
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

fn resolve_ident<S: ReadStorage>(
    storage: &S,
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

fn resolve_id<S: ReadStorage>(
    storage: &S,
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
    version: u64,
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
            version: 0,
            ident: None,
            value_type: None,
            cardinality: None,
            doc: None,
            unique: false,
        }
    }

    fn consume(&mut self, datom: Datom) {
        self.version = self.version.max(datom.tx);
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
            version: self.version,
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
    use crate::schema::default::default_datoms;
    use crate::storage::attribute_resolver::*;
    use crate::storage::memory::*;
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

    impl ReadStorage for CountingStorage {
        type Error = <InMemoryStorage as ReadStorage>::Error;

        fn find(&self, restricts: Restricts) -> impl Iterator<Item = Result<Datom, Self::Error>> {
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
