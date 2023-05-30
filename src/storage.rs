use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::datom::Datom;
use crate::datom::Op;
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
type Index<A, B, C> = BTreeMap<A, BTreeMap<B, BTreeMap<C, BTreeMap<Transaction, Op>>>>;

pub trait Storage {
    fn save(&mut self, datoms: &Vec<Datom>) -> Result<(), StorageError>;

    fn resolve_ident(&self, ident: &str) -> Result<Entity, StorageError>;

    fn attribute_type(&self, attribute: Attribute) -> Result<ValueType, StorageError>;

    fn find_datoms(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError>;
}

// https://docs.datomic.com/pro/query/indexes.html
pub struct InMemoryStorage {
    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    eavt: Index<Entity, Attribute, Value>,

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all :release/name
    // attributes are grouped together.
    aevt: Index<Attribute, Entity, Value>,

    // The AVET index provides efficient access to particular combinations of attribute and value.
    // The example below shows a portion of the AVET index allowing lookup by :release/names. The
    // AVET index is more expensive to maintain than other indexes, and as such it is the only
    // index that is not enabled by default. To maintain AVET for an attribute, specify :db/index
    // true (or some value for :db/unique) when installing or altering the attribute. The AVET
    // index also supports the indexRange API, which returns all attribute values in a particular
    // range.
    avet: Index<Attribute, Value, Entity>,

    // The VAET index contains all and only datoms whose attribute has a :db/valueType of
    // :db.type/ref. This is also known as the reverse index since it allows efficient navigation
    // of relationships in reverse.
    _vaet: Index<Value, Attribute, Entity>,

    // Lookup entity ID by ident
    ident_to_entity: HashMap<String, Entity>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        let mut storage = InMemoryStorage {
            eavt: BTreeMap::new(),
            aevt: BTreeMap::new(),
            avet: BTreeMap::new(),
            _vaet: BTreeMap::new(),
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

    fn attribute_type(&self, attribute: Attribute) -> Result<ValueType, StorageError> {
        let clause = Clause::new()
            .with_entity(EntityPattern::Id(attribute))
            .with_attribute(AttributePattern::Id(DB_ATTR_TYPE_ID));
        Ok(self
            .find_datoms(&clause)?
            .first()
            .and_then(|datom| datom.value.as_u8())
            .and_then(|value| ValueType::from(*value))
            .ok_or(StorageError::InvalidAttributeType)?)
    }

    fn find_datoms(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError> {
        match clause {
            Clause {
                entity: EntityPattern::Id(_),
                attribute: AttributePattern::Id(_) | AttributePattern::Ident(_),
                value: _,
            } => self.find_datoms_eavt(clause),
            Clause {
                entity: _,
                attribute: AttributePattern::Id(_) | AttributePattern::Ident(_),
                value: ValuePattern::Constant(_)
            } => self.find_datoms_avet(clause),
            _ => self.find_datoms_aevt(clause),
        }
    }
}

impl InMemoryStorage {
    fn validate(&self, datom: &Datom) -> Result<(), StorageError> {
        let value_type = self.attribute_type(datom.attribute)?;
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
            self.update_avet(datom);
            self.update_ident_to_entity_id(datom);
        }
    }

    fn update_eavt(&mut self, datom: &Datom) {
        let avt = self.eavt.entry(datom.entity).or_default();
        let vt = avt.entry(datom.attribute).or_default();
        let t = vt.entry(datom.value.clone()).or_default();
        t.insert(datom.tx, datom.op.clone());
    }

    fn update_aevt(&mut self, datom: &Datom) {
        let evt = self.aevt.entry(datom.attribute).or_default();
        let vt = evt.entry(datom.entity).or_default();
        let t = vt.entry(datom.value.clone()).or_default();
        t.insert(datom.tx, datom.op.clone());
    }

    fn update_avet(&mut self, datom: &Datom) {
        let vet = self.avet.entry(datom.attribute).or_default();
        let et = vet.entry(datom.value.clone()).or_default();
        let t = et.entry(datom.entity).or_default();
        t.insert(datom.tx, datom.op.clone());
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
                for (value, t) in self.v_iter(vt, &clause.value) {
                    if let Some((tx, op)) = t.last_key_value() {
                        datoms.push(Datom {
                            entity: *entity,
                            attribute: *attribute,
                            value: value.clone(),
                            tx: *tx,
                            op: op.clone(),
                        })
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
                for (value, t) in self.v_iter(vt, &clause.value) {
                    if let Some((tx, op)) = t.last_key_value() {
                        datoms.push(Datom {
                            entity: *entity,
                            attribute: *attribute,
                            value: value.clone(),
                            tx: *tx,
                            op: op.clone(),
                        })
                    }
                }
            }
        }
        Ok(datoms)
    }

    fn find_datoms_avet(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError> {
        let mut datoms = Vec::new();
        for (attribute, vet) in self.a_iter(&self.avet, &clause.attribute)? {
            for (value, et) in self.v_iter(vet, &clause.value) {
                for (entity, t) in self.e_iter(et, &clause.entity) {
                    if let Some((tx, op)) = t.last_key_value() {
                        datoms.push(Datom {
                            entity: *entity,
                            attribute: *attribute,
                            value: value.clone(),
                            tx: *tx,
                            op: op.clone(),
                        })
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
        attribute: &'a AttributePattern,
    ) -> Result<Iter<'a, Attribute, V>, StorageError> {
        match attribute {
            AttributePattern::Id(id) => Ok(self.kv_iter(map, id)),
            AttributePattern::Ident(ident) => {
                let id = self.resolve_ident_internal(ident)?;
                Ok(self.kv_iter(map, id))
            }
            _ => Ok(Iter::Many(map.iter())),
        }
    }

    fn v_iter<'a, V>(
        &self,
        map: &'a BTreeMap<Value, V>,
        value: &'a ValuePattern,
    ) -> Iter<'a, Value, V> {
        match value {
            ValuePattern::Constant(value) => self.kv_iter(map, value),
            _ => Iter::Many(map.iter()),
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
