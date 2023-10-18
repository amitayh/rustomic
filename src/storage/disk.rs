use std::collections::{HashMap, HashSet};

use rocksdb::*;

use crate::schema::attribute::*;
use crate::schema::default::*;
use crate::storage::*;
use thiserror::Error;

type AttributeId = u64;

pub struct DiskStorage {
    db: rocksdb::DB,
    _attribute_resolver: HashMap<AttributeId, Cardinality>,
}

impl DiskStorage {
    // TODO: initialize existing db without reloading default datoms
    pub fn new(db: rocksdb::DB) -> Self {
        let mut storage = Self {
            db,
            _attribute_resolver: HashMap::new(),
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

impl WriteStorage for DiskStorage {
    type Error = DiskStorageError;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
        let mut batch = rocksdb::WriteBatch::default();
        // TODO: should we use 3 different DBs, or 1 DB with tag?
        for datom in datoms {
            batch.put(serde::datom::serialize::eavt(datom), "");
            batch.put(serde::datom::serialize::aevt(datom), "");
            batch.put(serde::datom::serialize::avet(datom), "");
        }
        self.db.write(batch)?;
        Ok(())
    }
}

impl<'a> ReadStorage<'a> for DiskStorage {
    type Error = DiskStorageError;
    //type Iter = std::vec::IntoIter<Datom>;
    type Iter = FooIter<'a>;

    fn find(&'a self, clause: &Clause) -> Result<Self::Iter, Self::Error> {
        Ok(FooIter::new(clause, &self.db))
    }
}

pub struct FooIter<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    end: Vec<u8>,
}

impl<'a> FooIter<'a> {
    fn new(clause: &Clause, db: &'a rocksdb::DB) -> Self {
        let start = serde::index::key(clause);
        let read_options = ReadOptions::default();
        let mut iterator = db.raw_iterator_opt(read_options);
        iterator.seek(&start);
        let end = serde::index::next_prefix(&start).unwrap(); // TODO
        Self { iterator, end }
    }
}

impl Iterator for FooIter<'_> {
    type Item = Datom;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.iterator.valid() {
            return None;
        }

        let datom_bytes = self.iterator.key()?;
        if datom_bytes >= &self.end {
            return None;
        }

        let datom = serde::datom::deserialize(datom_bytes).unwrap();
        if datom.op == Op::Retracted {
            let seek_key_size = serde::index::seek_key_size(&datom);
            let seek_key = serde::index::next_prefix(&datom_bytes[..seek_key_size]).unwrap();
            self.iterator.seek(seek_key);
            return self.next();
        }

        self.iterator.next();
        Some(datom)
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
