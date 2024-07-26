use std::marker::PhantomData;
use std::path::Path;
use std::u64;

use either::Either;
use rocksdb::*;
use thiserror::Error;

use crate::storage::iter::*;
use crate::storage::restricts::*;
use crate::storage::serde::index::*;
use crate::storage::serde::*;
use crate::storage::*;

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

const KEY_LATEST_ENTITY_ID: &str = "latest_entity_id";
//const KEY_VERSION: &str = "version";

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
        let system = cf_handle(&self.db, System)?;
        let mut batch = rocksdb::WriteBatch::default();
        let mut latest_entity_id = 0;
        for datom in datoms {
            batch.put_cf(eavt, datom::serialize::eavt(datom), "");
            batch.put_cf(aevt, datom::serialize::aevt(datom), "");
            batch.put_cf(avet, datom::serialize::avet(datom), "");
            latest_entity_id = latest_entity_id.max(datom.entity);
        }
        batch.put_cf(system, KEY_LATEST_ENTITY_ID, latest_entity_id.to_be_bytes());
        self.db.write(batch)?;
        Ok(())
    }
}

impl<'a, Mode> ReadStorage<'a> for DiskStorage<'a, Mode> {
    type Error = Either<DiskStorageError, ReadError>;
    type Iter = DatomsIterator<DiskStorageIter<'a>>;

    fn find(&'a self, restricts: Restricts) -> Self::Iter {
        let range = RestrictedIndexRange::from(restricts);
        let iter = DiskStorageIter::new(&range, &self.db);
        DatomsIterator::new(iter, range)
    }

    fn latest_entity_id(&self) -> Result<u64, Self::Error> {
        self.try_latest_entity_id().map_err(Either::Left)
    }
}

impl<'a, Mode> DiskStorage<'a, Mode> {
    fn try_latest_entity_id(&self) -> Result<u64, DiskStorageError> {
        let system = cf_handle(&self.db, System)?;
        let bytes = self.db.get_cf(system, KEY_LATEST_ENTITY_ID)?;
        let id = bytes
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes)
            .unwrap_or(0);
        Ok(id)
    }
}

pub struct DiskStorageIter<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    should_continue: bool,
}

impl<'a> DiskStorageIter<'a> {
    fn new(range: &RestrictedIndexRange, db: &'a rocksdb::DB) -> Self {
        let cf = cf_handle(db, range.index).unwrap(); // TODO
        let mut iterator = db.raw_iterator_cf(cf);
        match &range.start {
            None => iterator.seek_to_first(),
            Some(start) => iterator.seek(start),
        }
        Self {
            iterator,
            should_continue: false,
        }
    }
}

impl BytesIterator for DiskStorageIter<'_> {
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
        self.should_continue = true;
        Some(Ok(bytes))
    }

    fn seek(&mut self, key: Bytes) -> Result<(), Self::Error> {
        self.iterator.seek(key);
        self.should_continue = false;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum DiskStorageError {
    #[error("storage error")]
    DbError(#[from] rocksdb::Error),
    #[error("column family {:?} not found", 0)]
    ColumnFamilyNotFound(&'static str),
}
