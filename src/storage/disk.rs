use std::marker::PhantomData;
use std::path::Path;

use either::Either;
use rocksdb::*;

use crate::storage::restricts::*;
use crate::storage::serde::*;
use crate::storage::*;
use thiserror::Error;

use super::serde::index::IndexedRange;
use super::serde::index::Range;

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

trait Partition {
    fn name(&self) -> &'static str;
}

impl Partition for Index {
    fn name(&self) -> &'static str {
        match self {
            Self::Eavt => "eavt",
            Self::Aevt => "aevt",
            Self::Avet => "avet",
        }
    }
}

struct System;

impl Partition for System {
    fn name(&self) -> &'static str {
        "system"
    }
}

fn partitions() -> [&'static str; 4] {
    [
        Index::Eavt.name(),
        Index::Aevt.name(),
        Index::Avet.name(),
        System.name(),
    ]
}

fn cf_handle(
    db: &rocksdb::DB,
    partition: impl Partition,
) -> Result<&ColumnFamily, DiskStorageError> {
    db.cf_handle(partition.name())
        .ok_or_else(|| DiskStorageError::ColumnFamilyNotFound(partition.name()))
}

impl<'a> DiskStorage<'a, ReadOnly> {
    pub fn read_only(path: impl AsRef<Path>) -> Result<Self, DiskStorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_for_read_only(&options, path, partitions(), false)?;
        Ok(Self::new(db))
    }
}

impl<'a> DiskStorage<'a, ReadWrite> {
    pub fn read_write(path: impl AsRef<Path>) -> Result<Self, DiskStorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, path, partitions())?;
        Ok(Self::new(db))
    }
}

impl<'a> WriteStorage for DiskStorage<'a, ReadWrite> {
    type Error = DiskStorageError;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
        let eavt = cf_handle(&self.db, Index::Eavt)?;
        let aevt = cf_handle(&self.db, Index::Aevt)?;
        let avet = cf_handle(&self.db, Index::Avet)?;
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
    type Error = Either<DiskStorageError, ReadError>;
    type Iter = DatomsIterator<DiskStorageIter<'a>>;

    fn find(&'a self, restricts: Restricts) -> Self::Iter {
        let iter = DiskStorageIter::new(&restricts, &self.db);
        DatomsIterator::new(iter, restricts)
    }
}

pub struct DiskStorageIter<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    end: Option<Bytes>,
    should_continue: bool,
}

impl<'a> DiskStorageIter<'a> {
    fn new(restricts: &Restricts, db: &'a rocksdb::DB) -> Self {
        let IndexedRange(index, range) = IndexedRange::from(restricts);
        let cf = cf_handle(db, index).unwrap(); // TODO
        let mut iterator = db.raw_iterator_cf(cf);
        let mut end = None;
        match range {
            Range::Full => {
                iterator.seek_to_first();
            }
            Range::From(start) => {
                iterator.seek(start);
            }
            Range::Between(start, e) => {
                iterator.seek(start);
                end = Some(e);
            }
        }
        Self {
            iterator,
            end,
            should_continue: false,
        }
    }
}

/*
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

        match datom::deserialize(self.index, bytes) {
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
*/

#[derive(Debug, Error)]
pub enum DiskStorageError {
    #[error("storage error")]
    DbError(#[from] rocksdb::Error),
    #[error("column family {:?} not found", 0)]
    ColumnFamilyNotFound(&'static str),
}

// -------------------------------------------------------------------------------------------------

impl SeekableIterator for DiskStorageIter<'_> {
    type Error = DiskStorageError;

    fn next(&mut self) -> Option<Result<&[u8], Self::Error>> {
        if self.should_continue {
            self.should_continue = false;
            self.iterator.next();
        }

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
        self.should_continue = true;
        Some(Ok(bytes))
    }

    fn seek(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        self.iterator.seek(key);
        self.should_continue = false;
        Ok(())
    }
}
