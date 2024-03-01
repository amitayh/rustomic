use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;

use rocksdb::*;

use crate::storage::restricts::*;
use crate::storage::serde::*;
use crate::storage::*;
use thiserror::Error;

pub struct ReadOnly;
pub struct ReadWrite;

pub struct DiskStorage<'a, Mode> {
    db: rocksdb::DB,
    marker: PhantomData<&'a Mode>,
}

#[derive(Debug, Clone, Copy)]
pub enum Partition {
    Eavt,
    Aevt,
    Avet,
    System,
}

impl Into<&'static str> for Partition {
    fn into(self) -> &'static str {
        match self {
            Self::Eavt => "eavt",
            Self::Aevt => "eavt",
            Self::Avet => "eavt",
            Self::System => "eavt",
        }
    }
}

impl Partition {
    fn all() -> [&'static str; 4] {
        [
            Self::Eavt.into(),
            Self::Aevt.into(),
            Self::Avet.into(),
            Self::System.into(),
        ]
    }
}

impl<'a, Mode> DiskStorage<'a, Mode> {
    fn new(db: rocksdb::DB) -> Self {
        Self {
            db,
            marker: PhantomData,
        }
    }

    fn cf_handle(&self, partition: Partition) -> Result<&rocksdb::ColumnFamily, DiskStorageError> {
        self.db
            .cf_handle(partition.into())
            .ok_or(DiskStorageError::ColumnFamilyNotFound(partition))
    }
}

impl<'a> DiskStorage<'a, ReadOnly> {
    pub fn read_only(path: impl AsRef<Path>) -> Result<Self, DiskStorageError> {
        let options = Options::default();
        let db = DB::open_for_read_only(&options, path, true)?;
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
        let mut batch = rocksdb::WriteBatch::default();
        //let cf = self.column_families().unwrap();
        let eavt = self.cf_handle(Partition::Eavt)?;
        let aevt = self.cf_handle(Partition::Aevt)?;
        let avet = self.cf_handle(Partition::Avet)?;
        // TODO: should we use 3 different DBs, or 1 DB with tag?
        // 3 different column families?
        for datom in datoms {
            batch.put_cf(eavt, datom::serialize::eavt(datom), "");
            batch.put_cf(aevt, datom::serialize::aevt(datom), "");
            batch.put_cf(avet, datom::serialize::avet(datom), "");

            batch.put(datom::serialize::eavt(datom), "");
            batch.put(datom::serialize::aevt(datom), "");
            batch.put(datom::serialize::avet(datom), "");
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
    end: Bytes,
    restricts: Restricts,
}

impl<'a> DiskStorageIter<'a> {
    fn new(restricts: Restricts, db: &'a rocksdb::DB) -> Self {
        let Range { start, end } = index::key_range(&restricts);
        let mut iterator = db.raw_iterator();
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
    #[error("column family {:?} not found", 0)]
    ColumnFamilyNotFound(Partition),
}
