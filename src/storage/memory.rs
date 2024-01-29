use std::collections::btree_set::Range;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::marker::PhantomData;

use crate::storage::serde::*;
use crate::storage::*;

#[derive(Default)]
pub struct InMemoryStorage<'a> {
    index: BTreeSet<Bytes>,
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
            self.index.insert(datom::serialize::eavt(datom));
            self.index.insert(datom::serialize::aevt(datom));
            self.index.insert(datom::serialize::avet(datom));
        }
        Ok(())
    }
}

impl<'a> ReadStorage<'a> for InMemoryStorage<'a> {
    type Error = ReadError;
    type Iter = InMemoryStorageIter<'a>;

    fn find(&'a self, restricts: Restricts) -> Self::Iter {
        InMemoryStorageIter::new(&self.index, restricts)
    }
}

pub struct InMemoryStorageIter<'a> {
    index: &'a BTreeSet<Bytes>,
    range: Range<'a, Bytes>,
    end: Bytes,
    restricts: Restricts,
}

impl<'a> InMemoryStorageIter<'a> {
    fn new(index: &'a BTreeSet<Bytes>, restricts: Restricts) -> Self {
        let range = index::key_range(&restricts);
        let end = range.end.clone();
        Self {
            index,
            range: index.range(range),
            end,
            restricts,
        }
    }

    fn seek(&mut self, start: Bytes) {
        self.range = self.index.range::<Bytes, _>(&start..&self.end);
    }
}

impl<'a> Iterator for InMemoryStorageIter<'a> {
    type Item = Result<Datom, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.range.next()?;
        match datom::deserialize(bytes) {
            Ok(datom) if !self.restricts.test(&datom) => {
                let key = index::seek_key(&datom.value, bytes, self.restricts.tx.value());
                self.seek(key);
                self.next()
            }
            result => Some(result),
        }
    }
}
