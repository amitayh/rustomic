use std::marker::PhantomData;
use std::path::Path;

use rocksdb::*;

use crate::storage::restricts::*;
use crate::storage::serde::*;
use crate::storage::*;
use thiserror::Error;

use super::serde::index::BytesRange;

pub struct ReadOnly;
pub struct ReadWrite;

pub struct DiskStorage<'a, Mode> {
    db: rocksdb::DB,
    marker: PhantomData<&'a Mode>,
}

impl<'a, Mode> DiskStorage<'a, Mode> {
    fn new(db: rocksdb::DB) -> Self {
        Self {
            db,
            marker: PhantomData,
        }
    }
}

fn cf_handle(db: &rocksdb::DB, partition: Partition) -> Result<&ColumnFamily, DiskStorageError> {
    db.cf_handle(partition.into())
        .ok_or(DiskStorageError::ColumnFamilyNotFound(partition))
}

impl<'a> DiskStorage<'a, ReadOnly> {
    pub fn read_only(path: impl AsRef<Path>) -> Result<Self, DiskStorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_for_read_only(&options, path, Partition::all(), false)?;
        Ok(Self::new(db))
    }
}

impl<'a> DiskStorage<'a, ReadWrite> {
    pub fn read_write(path: impl AsRef<Path>) -> Result<Self, DiskStorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, path, Partition::all())?;
        Ok(Self::new(db))
    }
}

impl<'a> WriteStorage for DiskStorage<'a, ReadWrite> {
    type Error = DiskStorageError;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
        let eavt = cf_handle(&self.db, Partition::Eavt)?;
        let aevt = cf_handle(&self.db, Partition::Aevt)?;
        let avet = cf_handle(&self.db, Partition::Avet)?;
        let mut batch = rocksdb::WriteBatch::default();
        for datom in datoms {
            batch.put_cf(eavt, datom::serialize::eavt(datom), "");
            batch.put_cf(aevt, datom::serialize::aevt(datom), "");
            batch.put_cf(avet, datom::serialize::avet(datom), "");
        }
        self.db.write(batch)?;
        Ok(())
    }
}

impl<'a, Mode> ReadStorage<'a> for DiskStorage<'a, Mode> {
    type Error = DiskStorageError;
    type Iter = DiskStorageIter<'a>;

    fn find(&'a self, restricts: Restricts) -> Self::Iter {
        DiskStorageIter::new(restricts, &self.db)
    }
}

pub struct DiskStorageIter<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    end: Option<Bytes>,
    partition: Partition,
    restricts: Restricts,
}

impl<'a> DiskStorageIter<'a> {
    fn new(restricts: Restricts, db: &'a rocksdb::DB) -> Self {
        let (partition, range) = index::key_range(&restricts);
        let cf = cf_handle(db, partition).unwrap(); // TODO
        let mut iterator = db.raw_iterator_cf(cf);
        let mut end = None;
        match range {
            BytesRange::Full => {
                iterator.seek_to_first();
            }
            BytesRange::From(start) => {
                iterator.seek(start);
            }
            BytesRange::Between(start, e) => {
                iterator.seek(start);
                end = Some(e);
            }
        }
        Self {
            iterator,
            end,
            partition,
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
        if let Some(end) = &self.end {
            if bytes >= end {
                return None;
            }
        }

        match datom::deserialize(self.partition, bytes) {
            Ok(datom) if !self.restricts.test(&datom) => {
                if let Some(key) = index::seek_key(&datom.value, bytes, self.restricts.tx.value()) {
                    self.iterator.seek(key);
                }
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
    #[error("column family {:?} not found", 0)]
    ColumnFamilyNotFound(Partition),
}
