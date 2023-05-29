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

    fn resolve_ident<'a>(&'a self, ident: &str) -> Result<&'a u64, StorageError>;

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

    fn resolve_ident<'a>(&'a self, ident: &str) -> Result<&'a u64, StorageError> {
        let entity_id = self.ident_to_entity_id.get(ident);
        entity_id.ok_or_else(|| StorageError::IdentNotFound(String::from(ident)))
    }

    fn find_datoms(&self, clause: &Clause) -> Result<Vec<datom::Datom>, StorageError> {
        match clause {
            Clause {
                entity: EntityPattern::Id(_),
                attribute: _,
                value: _,
            } => self.find_datoms_eavt(clause),
            _ => self.find_datoms_aevt(clause),
        }
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

    fn find_datoms_eavt(&self, clause: &Clause) -> Result<Vec<datom::Datom>, StorageError> {
        let mut datoms = Vec::new();
        for (entity, avt) in self.e_iter(&self.eavt, &clause.entity) {
            for (attribute, vt) in self.a_iter(avt, &clause.attribute)? {
                for (value, tx) in self.v_iter(vt, &clause.value) {
                    if let Some(t) = tx.last() {
                        datoms.push(Datom::new(*entity, *attribute, value.clone(), *t));
                    }
                }
            }
        }
        Ok(datoms)
    }

    fn find_datoms_aevt(&self, clause: &Clause) -> Result<Vec<datom::Datom>, StorageError> {
        let mut datoms = Vec::new();
        for (attribute, evt) in self.a_iter(&self.aevt, &clause.attribute)? {
            for (entity, vt) in self.e_iter(evt, &clause.entity) {
                for (value, tx) in self.v_iter(vt, &clause.value) {
                    if let Some(t) = tx.last() {
                        datoms.push(Datom::new(*entity, *attribute, value.clone(), *t));
                    }
                }
            }
        }
        Ok(datoms)
    }

    fn e_iter<'a, V>(
        &self,
        map: &'a BTreeMap<u64, V>,
        entity: &'a EntityPattern,
    ) -> Iter<'a, u64, V> {
        match entity {
            EntityPattern::Id(id) => self.iter_one(map, id),
            _ => Iter::Many(map.iter()),
        }
    }

    fn a_iter<'a, V>(
        &'a self,
        map: &'a BTreeMap<u64, V>,
        entity: &'a AttributePattern,
    ) -> Result<Iter<'a, u64, V>, StorageError> {
        match entity {
            AttributePattern::Ident(ident) => {
                let id = self.resolve_ident(ident)?;
                Ok(self.iter_one(map, id))
            }
            AttributePattern::Id(id) => Ok(self.iter_one(map, &id)),
            _ => Ok(Iter::Many(map.iter())),
        }
    }

    fn v_iter<'a>(&self, vt: &'a Vt, value: &'a ValuePattern) -> Iter<'a, datom::Value, Vec<u64>> {
        match value {
            ValuePattern::Constant(value) => self.iter_one(vt, value),
            _ => Iter::Many(vt.iter()),
        }
    }

    fn iter_one<'a, K: Ord, V>(&self, map: &'a BTreeMap<K, V>, key: &'a K) -> Iter<'a, K, V> {
        map.get(key)
            .map(|value| Iter::One(Some((key, value))))
            .unwrap_or(Iter::Zero)
    }
}

enum Iter<'a, K, V> {
    Zero,
    One(Option<(&'a K, &'a V)>),
    Many(btree_map::Iter<'a, K, V>),
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Zero => None,
            Iter::One(item) => {
                let result = *item;
                *item = None;
                result
            }
            Iter::Many(iter) => iter.next(),
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    IdentNotFound(String),
}
