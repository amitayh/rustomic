use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::datom;
use crate::datom::Datom;
use crate::query::AttributePattern;
use crate::query::Clause;
use crate::query::EntityPattern;
use crate::query::ValuePattern;
use crate::schema::default_datoms;
use crate::schema::DB_ATTR_IDENT_ID;

pub trait Storage {
    fn save(&mut self, datoms: &Vec<datom::Datom>) -> Result<(), StorageError>;

    fn resolve_ident(&self, ident: &str) -> Result<u64, StorageError>;

    fn find_datoms(&self, clause: &Clause) -> Result<Vec<datom::Datom>, StorageError>;
}

type Vt = BTreeMap<datom::Value, Vec<u64>>;
type Avt = BTreeMap<u64, Vt>;
type Eavt = BTreeMap<u64, Avt>;
type Evt = BTreeMap<u64, Vt>;
type Aevt = BTreeMap<u64, Evt>;

// https://docs.datomic.com/pro/query/indexes.html
pub struct InMemoryStorage {
    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    eavt: Eavt,

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all :release/name
    // attributes are grouped together. This allows Datomic to efficiently query for all values of
    // the :release/name attribute, because they reside next to one another in this index.
    aevt: Aevt,

    // Lookup entity ID by ident
    ident_to_entity_id: HashMap<String, u64>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        let mut storage = InMemoryStorage {
            eavt: BTreeMap::new(),
            aevt: BTreeMap::new(),
            ident_to_entity_id: HashMap::new(),
        };
        let init_datoms = default_datoms();
        storage.save(&init_datoms).unwrap();
        storage
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

    fn find_datoms(&self, clause: &Clause) -> Result<Vec<datom::Datom>, StorageError> {
        let mut datoms = Vec::new();
        for (entity, avt) in self.entity_iter(&self.eavt, clause) {
            for (attribute, vt) in self.attribute_iter(avt, clause) {
                for (value, tx) in self.value_iter(vt, clause) {
                    if let Some(t) = tx.last() {
                        datoms.push(Datom::new(*entity, *attribute, value.clone(), *t));
                    }
                }
            }
        }
        Ok(datoms)
    }
}

impl InMemoryStorage {
    fn update_eavt(&mut self, datom: &datom::Datom) {
        let avt = self.eavt.entry(datom.entity).or_default();
        let vt = avt.entry(datom.attribute).or_default();
        let t = vt.entry(datom.value.clone()).or_default();
        t.push(datom.tx);
    }

    fn update_aevt(&mut self, datom: &datom::Datom) {
        let evt = self.aevt.entry(datom.attribute).or_default();
        let vt = evt.entry(datom.entity).or_default();
        let t = vt.entry(datom.value.clone()).or_default();
        t.push(datom.tx);
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

    fn entity_iter<'a>(&self, eavt: &'a Eavt, clause: &'a Clause) -> Iter<'a, u64, Avt> {
        match &clause.entity {
            EntityPattern::Id(entity) => eavt
                .get(entity)
                .map(|avt| Iter::One(Some((entity, avt))))
                .unwrap_or(Iter::None),
            _ => Iter::All(eavt.iter()),
        }
    }

    fn attribute_iter<'a>(&self, avt: &'a Avt, clause: &'a Clause) -> Iter<'a, u64, Vt> {
        match &clause.attribute {
            AttributePattern::Id(attribute) => avt
                .get(attribute)
                .map(|vt| Iter::One(Some((attribute, vt))))
                .unwrap_or(Iter::None),
            _ => Iter::All(avt.iter()),
        }
    }

    fn value_iter<'a>(&self, vt: &'a Vt, clause: &'a Clause) -> Iter<'a, datom::Value, Vec<u64>> {
        match &clause.value {
            ValuePattern::Constant(value) => vt
                .get(value)
                .map(|vt| Iter::One(Some((value, vt))))
                .unwrap_or(Iter::None),
            _ => Iter::All(vt.iter()),
        }
    }
}

enum Iter<'a, K, V> {
    None,
    One(Option<(&'a K, &'a V)>),
    All(btree_map::Iter<'a, K, V>),
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::None => None,
            Iter::One(item) => {
                let result = *item;
                *item = None;
                result
            }
            Iter::All(iter) => iter.next(),
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    IdentNotFound(String),
}
