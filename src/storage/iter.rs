use either::Either;

use crate::datom::*;
use crate::storage::serde::index::IndexedRange;
use crate::storage::*;

use crate::storage::serde::*;

pub trait SeekableIterator {
    type Error: std::error::Error;

    fn next(&mut self) -> Option<Result<&[u8], Self::Error>>;

    fn seek(&mut self, key: Bytes) -> Result<(), Self::Error>;
}

pub struct DatomsIterator<T> {
    restricts: Restricts,
    index: Index,
    bytes: T,
}

impl<T> DatomsIterator<T> {
    pub fn new(bytes: T, restricts: Restricts) -> Self {
        let IndexedRange { index, .. } = IndexedRange::from(&restricts);
        Self {
            restricts,
            index,
            bytes,
        }
    }
}

impl<T: SeekableIterator> Iterator for DatomsIterator<T> {
    type Item = Result<Datom, Either<T::Error, ReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = match self.bytes.next()? {
            Ok(bytes) => bytes,
            Err(err) => {
                return Some(Err(Either::Left(err)));
            }
        };
        match datom::deserialize(self.index, bytes) {
            Ok(datom) if self.restricts.test(&datom) => Some(Ok(datom)),
            Ok(datom) => {
                if let Some(key) = index::seek_key(&datom.value, bytes, self.restricts.tx.value()) {
                    if let Err(err) = self.bytes.seek(key) {
                        return Some(Err(Either::Left(err)));
                    }
                }
                self.next()
            }
            Err(err) => Some(Err(Either::Right(err))),
        }
    }
}
