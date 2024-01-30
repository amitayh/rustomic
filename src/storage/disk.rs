use std::marker::PhantomData;
use std::ops::Range;

use rocksdb::*;

use crate::storage::serde::*;
use crate::storage::*;
use thiserror::Error;

pub struct DiskStorage<'a> {
    db: rocksdb::DB,
    marker: PhantomData<&'a Self>,
}

impl<'a> DiskStorage<'a> {
    pub fn new(db: rocksdb::DB) -> Self {
        Self {
            db,
            marker: PhantomData,
        }
    }
}

impl<'a> WriteStorage for DiskStorage<'a> {
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

impl<'a> ReadStorage<'a> for DiskStorage<'a> {
    type Error = DiskStorageError;
    type Iter = DiskStorageIter<'a>;

    fn find(&'a self, restricts: Restricts) -> Self::Iter {
        DiskStorageIter::new(restricts, &self.db)
    }
}

pub struct DiskStorageIter<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    end: Bytes,
    restricts: Restricts,
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
            restricts,
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

        let bytes = self.iterator.key()?;
        if bytes >= &self.end {
            return None;
        }

        match datom::deserialize(bytes) {
            Ok(datom) if !self.restricts.test(&datom) => {
                let key = index::seek_key(&datom.value, bytes, self.restricts.tx.value());
                self.iterator.seek(key);
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
