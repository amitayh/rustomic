use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::datom;
use crate::schema::DB_ATTR_IDENT_ID;

pub trait Storage {
    fn save(&mut self, datoms: &Vec<datom::Datom>) -> Result<(), StorageError>;

    fn resolve_ident(&self, ident: &str) -> Result<u64, StorageError>;
}

// https://docs.datomic.com/pro/query/indexes.html
pub struct InMemoryStorage {
    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    eavt: BTreeMap<u64, BTreeMap<u64, BTreeMap<datom::Value, u64>>>,

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all :release/name
    // attributes are grouped together. This allows Datomic to efficiently query for all values of
    // the :release/name attribute, because they reside next to one another in this index.
    aevt: BTreeMap<u64, BTreeMap<u64, BTreeMap<datom::Value, u64>>>,

    // Lookup entity ID by ident
    ident_to_entity_id: HashMap<String, u64>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            eavt: BTreeMap::new(),
            aevt: BTreeMap::new(),
            ident_to_entity_id: HashMap::new(),
        }
    }
}

impl Storage for InMemoryStorage {
    fn save(&mut self, datoms: &Vec<datom::Datom>) -> Result<(), StorageError> {
        for datom in datoms {
            self.update_eavt(datom);
            self.update_aevt(datom);
            self.update_ident_to_entity_id(datom);
        }
        Ok(())
    }

    fn resolve_ident(&self, ident: &str) -> Result<u64, StorageError> {
        let entity_id = self.ident_to_entity_id.get(ident).copied();
        entity_id.ok_or_else(|| StorageError::IdentNotFound(String::from(ident)))
    }
}

impl InMemoryStorage {
    fn update_eavt(&mut self, datom: &datom::Datom) {
        let avt = self.eavt.entry(datom.entity).or_default();
        let vt = avt.entry(datom.attribute).or_default();
        vt.insert(datom.value.clone(), datom.tx);
    }

    fn update_aevt(&mut self, datom: &datom::Datom) {
        let evt = self.aevt.entry(datom.attribute).or_default();
        let vt = evt.entry(datom.entity).or_default();
        vt.insert(datom.value.clone(), datom.tx);
    }

    fn update_ident_to_entity_id(&mut self, datom: &datom::Datom) {
        if let datom::Datom {
            entity,
            attribute: DB_ATTR_IDENT_ID,
            value: datom::Value::Str(ident),
            tx: _,
            op: _,
        } = datom
        {
            self.ident_to_entity_id.insert(ident.clone(), *entity);
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    IdentNotFound(String),
}
