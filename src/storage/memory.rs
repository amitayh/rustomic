use std::collections::btree_set::Range;
use std::collections::BTreeSet;
use std::convert::Infallible;

use crate::storage::serde::*;
use crate::storage::*;

pub struct InMemoryStorage {
    index: BTreeSet<Vec<u8>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            index: BTreeSet::new(),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteStorage for InMemoryStorage {
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

impl<'a> ReadStorage<'a> for InMemoryStorage {
    type Error = ReadError;
    type Iter = InMemoryStorageIter<'a>;

    fn find(&'a self, clause: &Clause) -> Self::Iter {
        InMemoryStorageIter::new(&self.index, clause)
    }
}

pub struct InMemoryStorageIter<'a> {
    index: &'a BTreeSet<Vec<u8>>,
    range: Range<'a, Vec<u8>>,
    end: Vec<u8>,
}

impl<'a> InMemoryStorageIter<'a> {
    fn new(index: &'a BTreeSet<Vec<u8>>, clause: &Clause) -> Self {
        let (start, end) = index::key_range(clause);
        Self {
            index,
            range: index.range(start..end.clone()),
            end,
        }
    }
}

impl<'a> Iterator for InMemoryStorageIter<'a> {
    type Item = Result<Datom, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        let datom_bytes = self.range.next()?;
        match datom::deserialize(datom_bytes) {
            Ok(datom) if datom.op == Op::Retracted => {
                let seek_key = index::seek_key(&datom, datom_bytes);
                self.range = self.index.range(seek_key..self.end.clone());
                self.next()
            }
            Ok(datom) => Some(Ok(datom)),
            Err(err) => Some(Err(err)),
        }
    }
}
