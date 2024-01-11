use std::ops::Range;

use rocksdb::*;

use crate::storage::serde::*;
use crate::storage::*;
use thiserror::Error;

pub struct DiskStorage {
    db: rocksdb::DB,
}

impl DiskStorage {
    pub fn new(db: rocksdb::DB) -> Self {
        Self { db }
    }
}

impl WriteStorage for DiskStorage {
    type Error = rocksdb::Error;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
        let mut batch = rocksdb::WriteBatch::default();
        // TODO: should we use 3 different DBs, or 1 DB with tag?
        for datom in datoms {
            batch.put(datom::serialize::eavt(datom), "");
            batch.put(datom::serialize::aevt(datom), "");
            batch.put(datom::serialize::avet(datom), "");
        }
        self.db.write(batch)?;
        Ok(())
    }
}

impl ReadStorage for DiskStorage {
    type Error = DiskStorageError;

    fn find(&self, restricts: Restricts) -> impl Iterator<Item = Result<Datom, Self::Error>> {
        DiskStorageIter::new(restricts, &self.db)
    }
}

pub struct DiskStorageIter<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    end: Bytes,
    tx: u64,
}

impl<'a> DiskStorageIter<'a> {
    fn new(restricts: Restricts, db: &'a rocksdb::DB) -> Self {
        let Range { start, end } = index::key_range(&restricts);
        let read_options = ReadOptions::default();
        let mut iterator = db.raw_iterator_opt(read_options);
        iterator.seek(&start);
        Self {
            iterator,
            end,
            tx: restricts.tx2,
        }
    }
}

impl Iterator for DiskStorageIter<'_> {
    type Item = Result<Datom, DiskStorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.iterator.valid() {
            return match self.iterator.status() {
                Ok(_) => None,
                Err(err) => Some(Err(DiskStorageError::DbError(err))),
            };
        }

        let datom_bytes = self.iterator.key()?;
        if datom_bytes >= &self.end {
            return None;
        }

        match datom::deserialize(datom_bytes) {
            Ok(datom) if datom.op == Op::Retracted || datom.tx > self.tx => {
                self.iterator
                    .seek(index::seek_key(&datom.value, datom_bytes, self.tx));
                self.next()
            }
            Ok(datom) => {
                self.iterator.next();
                Some(Ok(datom))
            }
            Err(err) => Some(Err(DiskStorageError::ReadError(err))),
        }
    }
}

#[derive(Debug, Error)]
pub enum DiskStorageError {
    #[error("storage error")]
    DbError(#[from] rocksdb::Error),
    #[error("read error")]
    ReadError(#[from] ReadError),
}
