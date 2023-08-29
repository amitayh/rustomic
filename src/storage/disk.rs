use std::collections::{HashMap, HashSet};

use rocksdb::{PrefixRange, ReadOptions};

use crate::schema::attribute::*;
use crate::schema::default::*;
use crate::storage::*;
use thiserror::Error;

pub struct DiskStorage {
    db: rocksdb::DB,
    attribute_cardinality: HashMap<AttributeId, Cardinality>,
}

/*
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
*/

pub trait Foo {
    type Error;
    type Iter: Iterator<Item = Datom>;
    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
    fn find_datoms(&self, clause: &Clause) -> Result<Self::Iter, Self::Error>;
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

    pub fn find_datoms2(&self, clause: &Clause) -> Result<Vec<Datom>, StorageError> {
        let mut result = Vec::new();
        // TODO: `retracted_values` should contain entity and attribute
        let mut retracted_values = HashSet::new();
        for item in self.db.prefix_iterator(serde::index::key(clause)) {
            let (key, _) = item.unwrap();
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
}

impl Foo for DiskStorage {
    type Error = DiskStorageError;
    type Iter = std::vec::IntoIter<Datom>;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
        let mut batch = rocksdb::WriteBatch::default();
        for datom in datoms {
            batch.put(serde::datom::serialize::eavt(datom), "");
            batch.put(serde::datom::serialize::aevt(datom), "");
            batch.put(serde::datom::serialize::avet(datom), "");
        }
        self.db.write(batch)?;
        Ok(())
    }

    fn find_datoms(&self, clause: &Clause) -> Result<Self::Iter, Self::Error> {
        let mut result = Vec::new();
        let key = serde::index::key(clause);

        /*
        let mut iterator = self.db.prefix_iterator(&key);
        while let Some(result) = iterator.next() {
            let (datom_bytes, _) = result?;
            let datom = serde::datom::deserialize(&datom_bytes)?;
        }
        */

        let mut read_options = ReadOptions::default();
        read_options.set_iterate_range(PrefixRange(key));
        let iterator = self
            .db
            .iterator_opt(rocksdb::IteratorMode::Start, read_options);
        for item in iterator {
            let (datom_bytes, _) = item?;
            let datom = serde::datom::deserialize(&datom_bytes)?;
            dbg!(&datom);
            if datom.op == Op::Added {
                result.push(datom);
            }
        }
        Ok(result.into_iter())
    }
}

#[derive(Debug, Error)]
pub enum DiskStorageError {
    #[error("storage error")]
    DbError(#[from] rocksdb::Error),
    #[error("read error")]
    ReadError(#[from] serde::ReadError),
}

trait ByteString {
    fn bytes_string(&self) -> String;
}

impl ByteString for [u8] {
    fn bytes_string(&self) -> String {
        self.iter()
            .map(|byte| format!("{:02X}", byte))
            .collect::<Vec<String>>()
            .join(" ")
    }
}
