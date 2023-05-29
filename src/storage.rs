use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::datom::Datom;
use crate::datom::Value;
use crate::query::AttributePattern;
use crate::query::Clause;
use crate::query::EntityPattern;
use crate::query::ValuePattern;
use crate::schema::default_datoms;
use crate::schema::ValueType;
use crate::schema::DB_ATTR_IDENT_ID;
use crate::schema::DB_ATTR_TYPE_ID;

// TODO: create structs?
type Entity = u64;
type Attribute = u64;
type Transaction = u64;

pub trait Storage {
    fn save(&mut self, datoms: &Vec<Datom>) -> Result<(), StorageError>;

    fn resolve_ident(&self, ident: &str) -> Result<Entity, StorageError>;

    fn find_datoms(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError>;
}

// https://docs.datomic.com/pro/query/indexes.html
pub struct InMemoryStorage {
    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    eavt: BTreeMap<Entity, BTreeMap<Attribute, BTreeMap<Value, Vec<Transaction>>>>,

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all :release/name
    // attributes are grouped together. This allows Datomic to efficiently query for all values of
    // the :release/name attribute, because they reside next to one another in this index.
    aevt: BTreeMap<Attribute, BTreeMap<Entity, BTreeMap<Value, Vec<Transaction>>>>,

    // Lookup entity ID by ident
    ident_to_entity: HashMap<String, Entity>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        let mut storage = InMemoryStorage {
            eavt: BTreeMap::new(),
            aevt: BTreeMap::new(),
            ident_to_entity: HashMap::new(),
        };
        let init_datoms = default_datoms();
        storage.save_internal(&init_datoms);
        storage
    }
}

impl Storage for InMemoryStorage {
    fn save(&mut self, datoms: &Vec<Datom>) -> Result<(), StorageError> {
        for datom in datoms {
            self.validate(datom)?
        }
        self.save_internal(datoms);
        Ok(())
    }

    fn resolve_ident(&self, ident: &str) -> Result<Entity, StorageError> {
        self.resolve_ident_internal(ident).copied()
    }

    fn find_datoms(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError> {
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
    fn validate(&mut self, datom: &Datom) -> Result<(), StorageError> {
        let clause = Clause::new()
            .with_entity(EntityPattern::Id(datom.attribute))
            .with_attribute(AttributePattern::Id(DB_ATTR_TYPE_ID));
        let value_type = self
            .find_datoms(&clause)?
            .first()
            .and_then(|datom| datom.value.as_u8())
            .and_then(|value| ValueType::from(*value))
            .ok_or(StorageError::InvalidAttributeType)?;

        if !datom.value.matches_type(value_type) {
            Err(StorageError::InvalidAttributeType)
        } else {
            Ok(())
        }
    }

    fn save_internal(&mut self, datoms: &Vec<Datom>) {
        for datom in datoms {
            self.update_eavt(datom);
            self.update_aevt(datom);
            self.update_ident_to_entity_id(datom);
        }
    }
    fn update_eavt(&mut self, datom: &Datom) {
        let avt = self.eavt.entry(datom.entity).or_default();
        let vt = avt.entry(datom.attribute).or_default();
        let t = vt.entry(datom.value.clone()).or_default();
        t.push(datom.tx);
    }

    fn update_aevt(&mut self, datom: &Datom) {
        let evt = self.aevt.entry(datom.attribute).or_default();
        let vt = evt.entry(datom.entity).or_default();
        let t = vt.entry(datom.value.clone()).or_default();
        t.push(datom.tx);
    }

    fn update_ident_to_entity_id(&mut self, datom: &Datom) {
        if let Datom {
            entity,
            attribute: DB_ATTR_IDENT_ID,
            value: Value::Str(ident),
            tx: _,
            op: _,
        } = datom
        {
            self.ident_to_entity.insert(ident.clone(), *entity);
        }
    }

    fn find_datoms_eavt(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError> {
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

    fn find_datoms_aevt(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError> {
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
        map: &'a BTreeMap<Entity, V>,
        entity: &'a EntityPattern,
    ) -> Iter<'a, Entity, V> {
        match entity {
            EntityPattern::Id(id) => self.kv_iter(map, id),
            _ => Iter::Many(map.iter()),
        }
    }

    fn a_iter<'a, V>(
        &'a self,
        map: &'a BTreeMap<Attribute, V>,
        entity: &'a AttributePattern,
    ) -> Result<Iter<'a, Attribute, V>, StorageError> {
        match entity {
            AttributePattern::Id(id) => Ok(self.kv_iter(map, id)),
            AttributePattern::Ident(ident) => {
                let id = self.resolve_ident_internal(ident)?;
                Ok(self.kv_iter(map, id))
            }
            _ => Ok(Iter::Many(map.iter())),
        }
    }

    fn v_iter<'a>(
        &self,
        vt: &'a BTreeMap<Value, Vec<Transaction>>,
        value: &'a ValuePattern,
    ) -> Iter<'a, Value, Vec<Transaction>> {
        match value {
            ValuePattern::Constant(value) => self.kv_iter(vt, value),
            _ => Iter::Many(vt.iter()),
        }
    }

    fn kv_iter<'a, K: Ord, V>(&self, map: &'a BTreeMap<K, V>, key: &'a K) -> Iter<'a, K, V> {
        map.get(key)
            .map(|value| Iter::One(Some((key, value))))
            .unwrap_or(Iter::Zero)
    }

    fn resolve_ident_internal<'a>(&'a self, ident: &str) -> Result<&'a Entity, StorageError> {
        let entity = self.ident_to_entity.get(ident);
        entity.ok_or_else(|| StorageError::IdentNotFound(String::from(ident)))
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
    InvalidAttributeType,
}
