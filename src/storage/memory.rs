use std::collections::btree_set;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::marker::PhantomData;

use either::Either;

use crate::storage::iter::*;
use crate::storage::serde::index::RestrictedIndexRange;
use crate::storage::serde::*;
use crate::storage::*;

#[derive(Default)]
pub struct InMemoryStorage<'a> {
    eavt: BTreeSet<Bytes>,
    aevt: BTreeSet<Bytes>,
    avet: BTreeSet<Bytes>,
    marker: PhantomData<&'a Self>,
    latest_entity_id: u64,
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
            self.latest_entity_id = self.latest_entity_id.max(datom.entity);
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
        let range = RestrictedIndexRange::from(restricts);
        let iter = InMemoryStorageIter::new(self, &range);
        DatomsIterator::new(iter, range)
    }

    fn latest_entity_id(&self) -> Result<u64, Self::Error> {
        Ok(self.latest_entity_id)
    }
}

pub struct InMemoryStorageIter<'a> {
    index: &'a BTreeSet<Bytes>,
    range: btree_set::Range<'a, Bytes>,
}

impl<'a> InMemoryStorageIter<'a> {
    fn new(storage: &'a InMemoryStorage, range: &RestrictedIndexRange) -> Self {
        let index = match range.index {
            Index::Eavt => &storage.eavt,
            Index::Aevt => &storage.aevt,
            Index::Avet => &storage.avet,
        };
        let range = match &range.start {
            Some(start) => index.range::<Bytes, _>(start..),
            None => index.range::<Bytes, _>(..),
        };
        Self { index, range }
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
