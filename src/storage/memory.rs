use std::collections::btree_set::Range;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::marker::PhantomData;

use either::Either;

use crate::storage::serde::*;
use crate::storage::*;

use super::serde::index::IndexedRange;

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
        let iter = InMemoryStorageIter::new(&self, &restricts);
        DatomsIterator::new(iter, restricts)
    }
}

pub struct InMemoryStorageIter<'a> {
    index: &'a BTreeSet<Bytes>,
    range: Range<'a, Bytes>,
    end: Option<Bytes>,
}

impl<'a> InMemoryStorageIter<'a> {
    fn new(storage: &'a InMemoryStorage, restricts: &Restricts) -> Self {
        let IndexedRange(partition, range) = IndexedRange::from(restricts);
        let end = match &range {
            index::Range::Between(_, end) => Some(end.clone()),
            index::Range::Full | index::Range::From(_) => None,
        };
        let index = match partition {
            Index::Eavt => &storage.eavt,
            Index::Aevt => &storage.aevt,
            Index::Avet => &storage.avet,
        };
        Self {
            index,
            range: index.range(range),
            end,
        }
    }
}

// -------------------------------------------------------------------------------------------------

impl SeekableIterator for InMemoryStorageIter<'_> {
    type Error = Infallible;

    fn next(&mut self) -> Option<Result<&[u8], Self::Error>> {
        let bytes = self.range.next()?;
        Some(Ok(bytes))
    }

    fn seek(&mut self, start: &[u8]) -> Result<(), Self::Error> {
        let start = start.to_vec(); // TODO avoid copy?
        self.range = match &self.end {
            Some(end) => self.index.range::<Bytes, _>(&start..end),
            None => self.index.range::<Bytes, _>(&start..),
        };
        Ok(())
    }
}
