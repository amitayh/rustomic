use std::collections::HashMap;
use std::rc::Rc;

use crate::datom::*;
use crate::query::clause::Clause;
use crate::query::pattern::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::ReadStorage;

pub struct CachingAttributeResolver {
    cache: HashMap<Rc<str>, Attribute>,
}

impl CachingAttributeResolver {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn resolve_ident<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        ident: &str,
    ) -> Result<Option<Attribute>, S::Error> {
        if let Some(attribute) = self.cache.get(ident) {
            return Ok(Some(attribute.clone()));
        }
        if let Some(attribute) = resolve_ident(storage, ident)? {
            self.update_cache(attribute.clone());
            return Ok(Some(attribute));
        }
        Ok(None)
    }

    fn update_cache(&mut self, attribute: Attribute) {
        self.cache.insert(Rc::clone(&attribute.ident), attribute);
    }
}

fn resolve_ident<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    ident: &str,
) -> Result<Option<Attribute>, S::Error> {
    // [?attribute :db/attr/ident ?ident]
    let clause = Clause::new()
        .with_attribute(AttributePattern::Id(DB_ATTR_IDENT_ID))
        .with_value(ValuePattern::constant(ident.into()));
    if let Some(datom) = storage.find(&clause).next() {
        return resolve_id(storage, datom?.entity);
    }
    Ok(None)
}

fn resolve_id<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    attribute_id: u64,
) -> Result<Option<Attribute>, S::Error> {
    let mut builder = Builder::new(attribute_id);
    // [?attribute _ _]
    let clause = Clause::new().with_entity(EntityPattern::Id(attribute_id));
    for datom in storage.find(&clause) {
        builder.consume(&datom?);
    }
    Ok(builder.build())
}

// ------------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    pub id: u64,
    pub ident: Rc<str>,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
}

struct Builder {
    id: u64,
    ident: Option<Rc<str>>,
    value_type: Option<ValueType>,
    cardinality: Option<Cardinality>,
}

impl Builder {
    fn new(id: u64) -> Self {
        Self {
            id,
            ident: None,
            value_type: None,
            cardinality: None,
        }
    }

    fn consume(&mut self, datom: &Datom) {
        match datom {
            Datom {
                entity: _,
                attribute: DB_ATTR_IDENT_ID,
                value: Value::Str(ident),
                tx: _,
                op: _,
            } => self.ident = Some(Rc::clone(ident)),
            Datom {
                entity: _,
                attribute: DB_ATTR_TYPE_ID,
                value: Value::U64(value_type),
                tx: _,
                op: _,
            } => self.value_type = ValueType::from(*value_type),
            Datom {
                entity: _,
                attribute: DB_ATTR_CARDINALITY_ID,
                value: Value::U64(cardinality),
                tx: _,
                op: _,
            } => self.cardinality = Cardinality::from(*cardinality),
            _ => (),
        }
    }

    fn build(self) -> Option<Attribute> {
        let ident = self.ident?;
        let value_type = self.value_type?;
        let cardinality = self.cardinality?;
        Some(Attribute {
            id: self.id,
            ident,
            value_type,
            cardinality,
        })
    }
}

// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::clock::Instant;
    use crate::schema::attribute::{Attribute, ValueType};
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

        fn find(&'a self, clause: &Clause) -> Self::Iter {
            self.count.set(self.count.get() + 1);
            self.inner.find(clause)
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

        let result = result.unwrap().unwrap();
        assert_eq!(Rc::from("foo/bar"), result.ident);
        assert_eq!(ValueType::U64, result.value_type);
    }

    #[test]
    fn cache_hit() {
        let mut storage = create_storage();
        let mut transactor = Transactor::new();

        // No calls to `CountingStorage::find` yet.
        assert_eq!(0, storage.current_count());

        let attribute = Attribute::new("foo/bar", ValueType::U64);
        let transaction = Transaction::new().with(attribute);
        let tx_result = transactor.transact(&storage, Instant(0), transaction);
        assert!(tx_result.is_ok());
        assert!(storage.save(&tx_result.unwrap().tx_data).is_ok());

        let mut resolver = CachingAttributeResolver::new();
        let result1 = resolver.resolve_ident(&storage, "foo/bar");
        assert!(result1.is_ok());
        let result1 = result1.unwrap();
        assert!(result1.is_some());

        // Storage was used to resolve `foo/bar`.
        let queries = storage.current_count();
        assert!(queries > 0);

        let result2 = resolver.resolve_ident(&storage, "foo/bar");
        assert!(result2.is_ok());
        let result2 = result2.unwrap();
        assert_eq!(result1, result2);

        // No additional calls to storage were needed to resolve cached attribute.
        assert_eq!(queries, storage.current_count());
    }
}
