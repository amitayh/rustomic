use std::collections::btree_map;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

use crate::datom::*;
use crate::query::clause::Clause;
use crate::query::pattern::*;
use crate::schema::default::*;
use crate::schema::*;
use crate::storage::*;

type TxOp = BTreeMap<TransactionId, Op>;
type Index<A, B, C> = BTreeMap<A, BTreeMap<B, BTreeMap<C, TxOp>>>;

// +-------+---------------------------------+--------------------------------+
// | Index | Sort order                      | Contains                       |
// +-------+---------------------------------+--------------------------------+
// | EAVT  | entity / attribute / value / tx | All datoms                     |
// | AEVT  | attribute / entity / value / tx | All datoms                     |
// | AVET  | attribute / value / entity / tx | Datoms with indexed attributes |
// +-------+---------------------------------+--------------------------------+
//
// https://docs.datomic.com/pro/query/indexes.html
#[derive(Default, Debug)]
pub struct InMemoryStorage {
    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    //
    // The example below shows all of the facts about entity 42 grouped together:
    //
    //   +----+----------------+------------------------+------+--------+
    //   | E  | A              | V                      | Tx   | Op    |
    //   +----+----------------+------------------------+------+--------+
    //   | 41 | release/name   | "Abbey Road"           | 1100 | Added |
    // * | 42 | release/name   | "Magical Mystery Tour" | 1007 | Added |
    // * | 42 | release/year   | 1967                   | 1007 | Added |
    // * | 42 | release/artist | "The Beatles"          | 1007 | Added |
    //   | 43 | release/name   | "Let It Be"            | 1234 | Added |
    //   +----+----------------+------------------------+------+--------+
    //
    // EAVT is also useful in master or detail lookups, since the references to detail entities are
    // just ordinary versus alongside the scalar attributes of the master entity. Better still,
    // Datomic assigns entity ids so that when master and detail records are created in the same
    // transaction, they are colocated in EAVT.
    eavt: Index<EntityId, AttributeId, Value>,

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all release/name
    // attributes are grouped together. This allows Datomic to efficiently query for all values of
    // the release/name attribute, because they reside next to one another in this index.
    //
    //   +----------------+----+------------------------+------+--------+
    //   | A              | E  | V                      | Tx   | Op    |
    //   +----------------+----+------------------------+------+--------+
    //   | release/artist | 42 | "The Beatles"          | 1007 | Added |
    // * | release/name   | 41 | "Abbey Road"           | 1100 | Added |
    // * | release/name   | 42 | "Magical Mystery Tour" | 1007 | Added |
    // * | release/name   | 43 | "Let It Be"            | 1234 | Added |
    //   | release/year   | 42 | 1967                   | 1007 | Added |
    //   +----------------+----+------------------------+------+--------+
    aevt: Index<AttributeId, EntityId, Value>,

    // The AVET index provides efficient access to particular combinations of attribute and value.
    // The example below shows a portion of the AVET index allowing lookup by release/names.
    //
    // The AVET index is more expensive to maintain than other indexes, and as such it is the only
    // index that is not enabled by default. To maintain AVET for an attribute, specify db/index
    // true (or some value for db/unique) when installing or altering the attribute.
    //
    //   +----------------+------------------------+----+------+--------+
    //   | A              | V                      | E  | Tx   | Op    |
    //   +----------------+------------------------+----+------+--------+
    //   | release/name   | "Abbey Road"           | 41 | 1100 | Added |
    // * | release/name   | "Let It Be"            | 43 | 1234 | Added |
    // * | release/name   | "Let It Be"            | 55 | 2367 | Added |
    //   | release/name   | "Magical Mystery Tour" | 42 | 1007 | Added |
    //   | release/year   | 1967                   | 42 | 1007 | Added |
    //   | release/year   | 1984                   | 55 | 2367 | Added |
    //   +----------------+------------------------+----+------+--------+
    avet: Index<AttributeId, Value, EntityId>,

    // Lookup entity ID by ident
    ident_to_entity: HashMap<Rc<str>, EntityId>,
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
        storage.save(&init_datoms).unwrap();
        storage
    }
}

impl Storage for InMemoryStorage {
    //type Iter = std::slice::Iter<'a, Datom>;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), StorageError> {
        for datom in datoms {
            self.update_eavt(datom);
            self.update_aevt(datom);
            self.update_avet(datom);
            self.update_ident_to_entity_id(datom);
        }
        Ok(())
    }

    fn find_datoms(&self, clause: &Clause, tx_range: u64) -> Result<Vec<Datom>, StorageError> {
        match clause {
            Clause {
                entity: EntityPattern::Id(_),
                attribute: AttributePattern::Id(_) | AttributePattern::Ident(_),
                value: _,
                tx: _,
            } => self.find_datoms_eavt(clause),
            Clause {
                entity: _,
                attribute: AttributePattern::Id(_) | AttributePattern::Ident(_),
                value: ValuePattern::Constant(_), // | ValuePattern::Range(_, _),
                tx: _,
            } => self.find_datoms_avet(clause, tx_range),
            _ => self.find_datoms_aevt(clause),
        }
    }
}

impl<'a> InMemoryStorage {
    fn update_eavt(&mut self, datom: &Datom) {
        let avt = self.eavt.entry(datom.entity).or_default();
        let vt = avt.entry(datom.attribute).or_default();
        let t = vt.entry(datom.value.clone()).or_default();
        t.insert(datom.tx, datom.op);
    }

    fn update_aevt(&mut self, datom: &Datom) {
        let evt = self.aevt.entry(datom.attribute).or_default();
        let vt = evt.entry(datom.entity).or_default();
        let t = vt.entry(datom.value.clone()).or_default();
        t.insert(datom.tx, datom.op);
    }

    fn update_avet(&mut self, datom: &Datom) {
        let vet = self.avet.entry(datom.attribute).or_default();
        let et = vet.entry(datom.value.clone()).or_default();
        let t = et.entry(datom.entity).or_default();
        t.insert(datom.tx, datom.op);
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
            for (attribute, vt) in self.a_iter(avt, &clause.attribute) {
                let v_iter = self.v_iter(vt, &clause.value);
                for (value, tx) in self.latest_values(v_iter, &clause.tx) {
                    datoms.push(Datom::add(*entity, *attribute, value.clone(), tx));
                }
            }
        }
        Ok(datoms)
    }

    fn find_datoms_aevt(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError> {
        let mut datoms = Vec::new();
        for (attribute, evt) in self.a_iter(&self.aevt, &clause.attribute) {
            for (entity, vt) in self.e_iter(evt, &clause.entity) {
                let v_iter = self.v_iter(vt, &clause.value);
                for (value, tx) in self.latest_values(v_iter, &clause.tx) {
                    datoms.push(Datom::add(*entity, *attribute, value.clone(), tx))
                }
            }
        }
        Ok(datoms)
    }

    fn find_datoms_avet(
        &'a self,
        clause: &'a Clause,
        tx_range: u64,
    ) -> Result<Vec<Datom>, StorageError> {
        let mut datoms = Vec::new();
        for (attribute, vet) in self.a_iter(&self.avet, &clause.attribute) {
            for (value, et) in self.v_iter(vet, &clause.value) {
                for (entity, t) in self.e_iter(et, &clause.entity) {
                    let mut latest_value_tx = None;
                    for (tx, op) in t.range(..=tx_range) {
                        latest_value_tx = match op {
                            Op::Added => Some(tx),
                            Op::Retracted => None,
                        };
                    }
                    if let Some(tx) = latest_value_tx {
                        datoms.push(Datom::add(*entity, *attribute, value.clone(), *tx));
                    }
                }
            }
        }
        Ok(datoms)
    }

    fn e_iter<V>(
        &self,
        map: &'a BTreeMap<EntityId, V>,
        entity: &'a EntityPattern,
    ) -> Iter<'a, EntityId, V> {
        match entity {
            EntityPattern::Id(id) => self.kv_iter(map, id),
            _ => Iter::Many(map.iter()),
        }
    }

    fn a_iter<V>(
        &self,
        map: &'a BTreeMap<AttributeId, V>,
        attribute: &'a AttributePattern,
    ) -> Iter<'a, AttributeId, V> {
        match attribute {
            AttributePattern::Id(id) => self.kv_iter(map, id),
            AttributePattern::Ident(ident) => Iter::One(
                self.ident_to_entity
                    .get(ident)
                    .and_then(|attribute| map.get_key_value(attribute)),
            ),
            _ => Iter::Many(map.iter()),
        }
    }

    fn v_iter<V>(
        &self,
        map: &'a BTreeMap<Value, V>,
        value: &'a ValuePattern,
    ) -> Iter<'a, Value, V> {
        match value {
            ValuePattern::Constant(value) => self.kv_iter(map, value),
            //ValuePattern::Range(start, end) => Iter::Range(map.range((start, end))),
            _ => Iter::Many(map.iter()),
        }
    }

    fn kv_iter<K: Ord, V>(&self, map: &'a BTreeMap<K, V>, key: &'a K) -> Iter<'a, K, V> {
        map.get(key)
            .map(|value| Iter::One(Some((key, value))))
            .unwrap_or(Iter::Zero)
    }

    fn tx_iter(&self, map: &'a TxOp, tx: &'a TxPattern) -> Iter<'a, TransactionId, Op> {
        match *tx {
            TxPattern::Range(start, end) => Iter::Range(map.range((start, end))),
            _ => Iter::Many(map.iter()),
        }
    }

    fn latest_values<In: Iterator<Item = (&'a Value, &'a TxOp)>>(
        &self,
        v_iter: In,
        tx: &'a TxPattern,
    ) -> HashMap<&'a Value, u64> {
        let mut latest = HashMap::new();
        for (value, t) in v_iter {
            for (tx, op) in self.tx_iter(t, tx) {
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
        latest
    }
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
