use std::collections::{HashMap, HashSet};

use rocksdb::*;

use crate::schema::attribute::*;
use crate::schema::default::*;
use crate::storage::*;
use thiserror::Error;

type AttributeId = u64;

pub struct DiskStorage {
    db: rocksdb::DB,
    _attribute_cardinality: HashMap<AttributeId, Cardinality>,
}

// TODO?
// Separate storage to 2 layers:
// 1. Base storage - read/write datoms in sorted order
// 2. Layer that understands attributes and entities

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

pub trait Foo<'a> {
    type Error;
    type Iter: Iterator<Item = Datom>;
    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
    fn find_datoms(&'a self, clause: &Clause) -> Result<Self::Iter, Self::Error>;
}

impl DiskStorage {
    // TODO: initialize existing db without reloading default datoms
    pub fn new(db: rocksdb::DB) -> Self {
        let mut storage = DiskStorage {
            db,
            _attribute_cardinality: HashMap::new(),
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

impl<'a> Foo<'a> for DiskStorage {
    type Error = DiskStorageError;
    //type Iter = std::vec::IntoIter<Datom>;
    type Iter = FooIter<'a>;

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

    fn find_datoms(&'a self, clause: &Clause) -> Result<Self::Iter, Self::Error> {
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
        let end = next_prefix(&start).unwrap(); // TODO
        Self { iterator, end }
    }
}

impl<'a> Iterator for FooIter<'a> {
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
            let seek_key = next_prefix(&datom_bytes[..seek_key_size]).unwrap();
            self.iterator.seek(seek_key);
            return self.next();
        }

        self.iterator.next();
        Some(datom)
    }
}

/// Returns lowest value following largest value with given prefix.
///
/// In other words, computes upper bound for a prefix scan over list of keys
/// sorted in lexicographical order.  This means that a prefix scan can be
/// expressed as range scan over a right-open `[prefix, next_prefix(prefix))`
/// range.
///
/// For example, for prefix `foo` the function returns `fop`.
///
/// Returns `None` if there is no value which can follow value with given
/// prefix.  This happens when prefix consists entirely of `'\xff'` bytes (or is
/// empty).
fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let ffs = prefix
        .iter()
        .rev()
        .take_while(|&&byte| byte == u8::MAX)
        .count();
    let next = &prefix[..(prefix.len() - ffs)];
    if next.is_empty() {
        // Prefix consisted of \xff bytes.  There is no prefix that
        // follows it.
        None
    } else {
        let mut next = next.to_vec();
        *next.last_mut().unwrap() += 1;
        Some(next)
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
