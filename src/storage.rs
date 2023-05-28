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
type Evt = BTreeMap<u64, Vt>;

// https://docs.datomic.com/pro/query/indexes.html
pub struct InMemoryStorage {
    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    eavt: BTreeMap<u64, Avt>,

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all :release/name
    // attributes are grouped together. This allows Datomic to efficiently query for all values of
    // the :release/name attribute, because they reside next to one another in this index.
    aevt: BTreeMap<u64, Evt>,

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
        // let iter = Iter::new(self, clause);
        // let iter = Iter {
        //     storage: self,
        //     clause,
        //     state: IterState::Qux,
        // };
        // for datom in iter {
        //     datoms.push(datom);
        // }

        let lala = match &clause.entity {
            EntityPattern::Id(entity) => self
                .eavt
                .get(entity)
                .map(|avt| EntityIter::One {
                    entity,
                    avt,
                    done: false,
                })
                .unwrap_or(EntityIter::None),
            _ => EntityIter::All(self.eavt.iter()),
        };
        for (entity, avt) in lala {
            for (attribute, vt) in avt {
                for (value, tx) in vt {
                    for t in tx {
                        let datom = Datom::new(*entity, *attribute, value.clone(), *t);
                        if datom.satisfies(clause) {
                            datoms.push(datom);
                        }
                    }
                }
            }
        }
        Ok(datoms)
    }
}

enum EntityIter<'a> {
    None,
    One {
        entity: &'a u64,
        avt: &'a BTreeMap<u64, Vt>,
        done: bool,
    },
    All(btree_map::Iter<'a, u64, BTreeMap<u64, BTreeMap<datom::Value, Vec<u64>>>>),
}

impl<'a> Iterator for EntityIter<'a> {
    type Item = (&'a u64, &'a Avt);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EntityIter::One {
                entity,
                avt,
                done: done_var @ false,
            } => {
                *done_var = true;
                Some((entity, avt))
            }
            EntityIter::All(iter) => iter.next(),
            _ => None,
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
}

#[derive(Debug)]
pub enum StorageError {
    IdentNotFound(String),
}
