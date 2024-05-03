use std::collections::btree_set::Range;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::marker::PhantomData;

use either::Either;

use crate::storage::iter::*;
use crate::storage::serde::index::*;
use crate::storage::serde::*;
use crate::storage::*;

#[derive(Default)]
pub struct InMemoryStorage<'a> {
    eavt: BTreeSet<Bytes>,
    aevt: BTreeSet<Bytes>,
    avet: BTreeSet<Bytes>,
    marker: PhantomData<&'a Self>,
}

impl<'a> InMemoryStorage<'a> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<'a> WriteStorage for InMemoryStorage<'a> {
    type Error = Infallible;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
        for datom in datoms {
            self.eavt.insert(datom::serialize::eavt(datom));
            self.aevt.insert(datom::serialize::aevt(datom));
            self.avet.insert(datom::serialize::avet(datom));
        }
        Ok(())
    }
}

impl<'a> ReadStorage<'a> for InMemoryStorage<'a> {
    type Error = Either<Infallible, ReadError>;
    type Iter = DatomsIterator<InMemoryStorageIter<'a>>;

    fn find(&'a self, restricts: Restricts) -> Self::Iter {
        let iter = InMemoryStorageIter::new(self, &restricts);
        DatomsIterator::new(iter, restricts)
    }
}

pub struct InMemoryStorageIter<'a> {
    index: &'a BTreeSet<Bytes>,
    range: Range<'a, Bytes>,
}

impl<'a> InMemoryStorageIter<'a> {
    fn new(storage: &'a InMemoryStorage, restricts: &Restricts) -> Self {
        let IndexedRange(partition, range) = IndexedRange::from(restricts);
        let index = match partition {
            Index::Eavt => &storage.eavt,
            Index::Aevt => &storage.aevt,
            Index::Avet => &storage.avet,
        };
        Self {
            index,
            range: index.range(range),
        }
    }
}

impl SeekableIterator for InMemoryStorageIter<'_> {
    type Error = Infallible;

    fn next(&mut self) -> Option<Result<&[u8], Self::Error>> {
        let bytes = self.range.next()?;
        Some(Ok(bytes))
    }

    fn seek(&mut self, start: Bytes) -> Result<(), Self::Error> {
        self.range = self.index.range(start..);
        Ok(())
    }
}
