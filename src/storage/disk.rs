use std::collections::{HashMap, HashSet};

use rocksdb::{PrefixRange, ReadOptions};

use crate::schema::attribute::*;
use crate::schema::default::*;
use crate::storage::*;

pub struct DiskStorage {
    db: rocksdb::DB,
    attribute_cardinality: HashMap<AttributeId, Cardinality>,
}

#[allow(dead_code)]
fn bytes_string(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02X}", byte))
        .collect::<Vec<String>>()
        .join(" ")
}

impl Storage for DiskStorage {
    fn save(&mut self, datoms: &[Datom]) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        for datom in datoms {
            batch.put(serde::datom::serialize::eavt(datom), "");
            batch.put(serde::datom::serialize::aevt(datom), "");
            batch.put(serde::datom::serialize::avet(datom), "");
        }
        self.db.write(batch).unwrap();
        Ok(())
    }

    fn find_datoms(&self, clause: &Clause, _tx_range: u64) -> Result<Vec<Datom>, StorageError> {
        let mut result = Vec::new();
        let read_options = DiskStorage::read_options(clause);
        // TODO: `retracted_values` should contain entity and attribute
        let mut retracted_values = HashSet::new();
        for item in self
            .db
            .iterator_opt(rocksdb::IteratorMode::Start, read_options)
        {
            let (key, _value) = item.unwrap();
            //println!("@@@ KEY {}", bytes_string(&key));
            //dbg!(&datom);
            let datom = serde::datom::deserialize(&key).unwrap();
            if datom.op == Op::Retracted {
                retracted_values.insert(datom.value.clone());
            } else if !retracted_values.contains(&datom.value) {
                result.push(datom);
            } else {
                retracted_values.remove(&datom.value);
            }
        }
        Ok(result)
    }

    fn resolve_ident(&self, _ident: &str) -> Result<EntityId, StorageError> {
        todo!()
    }
}

impl DiskStorage {
    // TODO: initialize existing db without reloading default datoms
    pub fn new(db: rocksdb::DB) -> Self {
        let mut storage = DiskStorage {
            db,
            attribute_cardinality: HashMap::new(),
        };
        let init_datoms = default_datoms();
        storage.save(&init_datoms).unwrap();
        storage
    }

    fn read_options(clause: &Clause) -> ReadOptions {
        let mut read_options = rocksdb::ReadOptions::default();
        let range = PrefixRange(serde::index::key(clause));
        read_options.set_iterate_range(range);
        read_options
    }
}
