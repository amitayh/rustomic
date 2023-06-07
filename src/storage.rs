use std::collections::btree_map;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::datom::*;
use crate::query::*;
use crate::schema::*;

// TODO: create structs?
type EntityId = u64;
type AttributeId = u64;
type TransactionId = u64;
type Index<A, B, C> = BTreeMap<A, BTreeMap<B, BTreeMap<C, BTreeMap<TransactionId, Op>>>>;

// TODO: separate read / write?
pub trait Storage {
    //type Iter: Iterator<Item = Datom>;

    fn save(&mut self, datoms: &Vec<Datom>) -> Result<(), StorageError>;

    fn resolve_ident(&self, ident: &str) -> Result<EntityId, StorageError>;

    fn find_attribute(&self, attribute: AttributeId) -> Result<Attribute, StorageError>;

    fn find_datoms(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError>;
    //fn find_datoms(&self, clause: &Clause) -> Result<Self::Iter, StorageError>;
}

// https://docs.datomic.com/pro/query/indexes.html
pub struct InMemoryStorage {
    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    eavt: Index<EntityId, AttributeId, Value>,

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all :release/name
    // attributes are grouped together.
    aevt: Index<AttributeId, EntityId, Value>,

    // The AVET index provides efficient access to particular combinations of attribute and value.
    // The example below shows a portion of the AVET index allowing lookup by :release/names. The
    // AVET index is more expensive to maintain than other indexes, and as such it is the only
    // index that is not enabled by default. To maintain AVET for an attribute, specify :db/index
    // true (or some value for :db/unique) when installing or altering the attribute. The AVET
    // index also supports the indexRange API, which returns all attribute values in a particular
    // range.
    // TODO: this should only contain datoms with index
    avet: Index<AttributeId, Value, EntityId>,

    // Lookup entity ID by ident
    ident_to_entity: HashMap<String, EntityId>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        let mut storage = InMemoryStorage {
            eavt: BTreeMap::new(),
            aevt: BTreeMap::new(),
            avet: BTreeMap::new(),
            ident_to_entity: HashMap::new(),
        };
        let init_datoms = default_datoms();
        storage.save_internal(&init_datoms);
        storage
    }
}

impl Storage for InMemoryStorage {
    //type Iter = std::slice::Iter<'a, Datom>;
    fn save(&mut self, datoms: &Vec<Datom>) -> Result<(), StorageError> {
        // TODO: add reverse index for attribute of type `Ref`
        for datom in datoms {
            self.validate(datom)?
        }
        self.save_internal(datoms);
        Ok(())
    }

    fn resolve_ident(&self, ident: &str) -> Result<EntityId, StorageError> {
        self.resolve_ident_internal(ident).copied()
    }

    fn find_attribute(&self, attribute: AttributeId) -> Result<Attribute, StorageError> {
        let avt = self
            .eavt
            .get(&attribute)
            .ok_or_else(|| StorageError::AttributeNotFound(attribute))?;

        let mut builder = AttributeBuilder::new();
        for (attribute0, vt) in avt {
            for (value, _) in self.latest_values(Iter::Many(vt.iter())) {
                match attribute0 {
                    &DB_ATTR_IDENT_ID => {
                        if let Some(ident) = value.as_str() {
                            builder.with_ident(ident);
                        }
                    }
                    &DB_ATTR_TYPE_ID => {
                        let value_type = value.as_u64().and_then(|value| ValueType::from(*value));
                        if let Some(value_type) = value_type {
                            builder.with_type(value_type);
                        }
                    }
                    &DB_ATTR_CARDINALITY_ID => {
                        let cardinality =
                            value.as_u64().and_then(|value| Cardinality::from(*value));
                        if let Some(cardinality) = cardinality {
                            builder.with_cardinality(cardinality);
                        }
                    }
                    &DB_ATTR_DOC_ID => {
                        if let Some(doc) = value.as_str() {
                            builder.with_doc(doc);
                        }
                    }
                    &DB_ATTR_UNIQUE_ID => {
                        if let Some(&1) = value.as_u64() {
                            builder.with_unique();
                        }
                    }
                    _ => {}
                }
            }
        }
        builder.build().ok_or(StorageError::Error)
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
                value: ValuePattern::Constant(_) | ValuePattern::Range(_, _),
            } => self.find_datoms_avet(clause),
            _ => self.find_datoms_aevt(clause),
        }
    }
}

impl InMemoryStorage {
    fn validate(&self, datom: &Datom) -> Result<(), StorageError> {
        let attribute = self.find_attribute(datom.attribute)?;
        if !datom.value.matches_type(attribute.value_type) {
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
                let v_iter = self.v_iter(vt, &clause.value);
                for (value, tx) in self.latest_values(v_iter) {
                    datoms.push(Datom {
                        entity: *entity,
                        attribute: *attribute,
                        value: value.clone(),
                        tx,
                        op: Op::Added,
                    })
                }
                //for (value, t) in self.v_iter(vt, &clause.value) {
                //    for (tx, op) in t {
                //        datoms.push(Datom {
                //            entity: *entity,
                //            attribute: *attribute,
                //            value: value.clone(),
                //            tx: *tx,
                //            op: op.clone(),
                //        })
                //    }
                //}
                // let lala = self.find_attribute(*attribute)?;
                // match lala.cardinality {
                //     Cardinality::One => {
                //         if let Some(value) = self.latest_value(vt) {
                //             datoms.push(Datom {
                //                 entity: *entity,
                //                 attribute: *attribute,
                //                 value: value.clone(),
                //                 tx: 0,
                //                 op: Op::Added,
                //             })
                //         }
                //     }
                //     Cardinality::Many => {
                //         for value in self.latest_values(vt) {
                //             datoms.push(Datom {
                //                 entity: *entity,
                //                 attribute: *attribute,
                //                 value: value.clone(),
                //                 tx: 0,
                //                 op: Op::Added,
                //             })
                //         }
                //     }
                // }
            }
        }
        Ok(datoms)
    }

    fn find_datoms_aevt(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError> {
        let mut datoms = Vec::new();

        /*
        let foo = self.a_iter(&self.aevt, &clause.attribute)?.flat_map(|(attribute, evt)| {
            self.e_iter(evt, &clause.entity).flat_map(|(entity, vt)| {
                self.v_iter(vt, &clause.value).flat_map(|(value, t)| {
                    t.last_key_value().map(|(tx, op)| {
                        Datom {
                            entity: *entity,
                            attribute: *attribute,
                            value: value.clone(),
                            tx: *tx,
                            op: op.clone(),
                        }
                    }).into_iter()
                })
            })
        });
        */

        for (attribute, evt) in self.a_iter(&self.aevt, &clause.attribute)? {
            for (entity, vt) in self.e_iter(evt, &clause.entity) {
                let v_iter = self.v_iter(vt, &clause.value);
                for (value, tx) in self.latest_values(v_iter) {
                    datoms.push(Datom {
                        entity: *entity,
                        attribute: *attribute,
                        value: value.clone(),
                        tx,
                        op: Op::Added,
                    })
                }
                //for (value, t) in self.v_iter(vt, &clause.value) {
                //    for (tx, op) in t {
                //        datoms.push(Datom {
                //            entity: *entity,
                //            attribute: *attribute,
                //            value: value.clone(),
                //            tx: *tx,
                //            op: op.clone(),
                //        })
                //    }
                //    // if let Some((tx, op)) = t.last_key_value() {
                //    //     datoms.push(Datom {
                //    //         entity: *entity,
                //    //         attribute: *attribute,
                //    //         value: value.clone(),
                //    //         tx: *tx,
                //    //         op: op.clone(),
                //    //     })
                //    // }
                //}
            }
        }
        Ok(datoms)
    }

    fn find_datoms_avet<'a>(&'a self, clause: &'a Clause) -> Result<Vec<Datom>, StorageError> {
        let mut datoms = Vec::new();
        for (attribute, vet) in self.a_iter(&self.avet, &clause.attribute)? {
            for (value, et) in self.v_iter(vet, &clause.value) {
                for (entity, t) in self.e_iter(et, &clause.entity) {
                    for (tx, op) in t {
                        datoms.push(Datom {
                            entity: *entity,
                            attribute: *attribute,
                            value: value.clone(),
                            tx: *tx,
                            op: op.clone(),
                        })
                    }
                    // if let Some((tx, op)) = t.last_key_value() {
                    //     datoms.push(Datom {
                    //         entity: *entity,
                    //         attribute: *attribute,
                    //         value: value.clone(),
                    //         tx: *tx,
                    //         op: op.clone(),
                    //     })
                    // }
                }
            }
        }
        Ok(datoms)
    }

    fn e_iter<'a, V>(
        &self,
        map: &'a BTreeMap<EntityId, V>,
        entity: &'a EntityPattern,
    ) -> Iter<'a, EntityId, V> {
        match entity {
            EntityPattern::Id(id) => self.kv_iter(map, id),
            _ => Iter::Many(map.iter()),
        }
    }

    fn a_iter<'a, V>(
        &'a self,
        map: &'a BTreeMap<AttributeId, V>,
        attribute: &'a AttributePattern,
    ) -> Result<Iter<'a, AttributeId, V>, StorageError> {
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
            ValuePattern::Range(start, end) => Iter::Range(map.range((*start, *end))),
            _ => Iter::Many(map.iter()),
        }
    }

    fn kv_iter<'a, K: Ord, V>(&self, map: &'a BTreeMap<K, V>, key: &'a K) -> Iter<'a, K, V> {
        map.get(key)
            .map(|value| Iter::One(Some((key, value))))
            .unwrap_or(Iter::Zero)
    }

    fn resolve_ident_internal<'a>(&'a self, ident: &str) -> Result<&'a EntityId, StorageError> {
        let entity = self.ident_to_entity.get(ident);
        entity.ok_or_else(|| StorageError::IdentNotFound(String::from(ident)))
    }

    fn latest_values<'a>(
        &self,
        v_iter: Iter<'a, Value, BTreeMap<TransactionId, Op>>,
    ) -> impl Iterator<Item = (&'a Value, u64)> {
        let mut latest = HashMap::new();
        for (value, t) in v_iter {
            for (tx, op) in t {
                match (latest.get_mut(value), op) {
                    (Some(prev_tx), Op::Added) if tx > prev_tx => *prev_tx = *tx,
                    (Some(prev_tx), Op::Retracted) if tx > prev_tx => {
                        latest.remove(value);
                    }
                    (None, Op::Added) => {
                        latest.insert(value, *tx);
                    }
                    _ => {}
                }
            }
        }
        latest.into_iter()
    }

    //fn latest_values<'a>(
    //    &self,
    //    vt: &'a BTreeMap<Value, BTreeMap<TransactionId, Op>>,
    //) -> impl Iterator<Item = (&'a Value, u64)> {
    //    let mut latest = HashMap::new();
    //    for (value, t) in vt {
    //        for (tx, op) in t {
    //            match (latest.get_mut(value), op) {
    //                (Some(prev_tx), Op::Added) if tx > prev_tx => *prev_tx = *tx,
    //                (Some(prev_tx), Op::Retracted) if tx > prev_tx => {
    //                    latest.remove(value);
    //                }
    //                (None, Op::Added) => {
    //                    latest.insert(value, *tx);
    //                }
    //                _ => {}
    //            }
    //        }
    //    }
    //    latest.into_iter()
    //}
}

enum Iter<'a, K, V> {
    Zero,
    One(Option<(&'a K, &'a V)>),
    Range(Range<'a, K, V>),
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
            Iter::Range(range) => range.next(),
            Iter::Many(iter) => iter.next(),
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    Error,
    AttributeNotFound(u64),
    IdentNotFound(String),
    InvalidAttributeType,
}
