use crate::datom::*;
use crate::storage::serde::index::RestrictedIndexRange;

use crate::storage::serde::*;

pub trait BytesIterator {
    type Error: std::error::Error;

    fn next(&mut self) -> Option<Result<&[u8], Self::Error>>;

    fn seek(&mut self, key: Vec<u8>) -> Result<(), Self::Error>;
}

pub struct DatomsIterator<T> {
    range: RestrictedIndexRange,
    bytes_iterator: T,
}

impl<T> DatomsIterator<T> {
    pub fn new(bytes_iterator: T, range: RestrictedIndexRange) -> Self {
        Self {
            range,
            bytes_iterator,
        }
    }
}

impl<T> Iterator for DatomsIterator<T>
where
    T: BytesIterator,
    <T as BytesIterator>::Error: From<ReadError>,
{
    type Item = Result<Datom, T::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = match self.bytes_iterator.next()? {
            Ok(bytes) => bytes,
            Err(err) => return Some(Err(err)),
        };
        match datom::deserialize(self.range.index, bytes) {
            Ok(datom) if self.range.contains(&datom) => Some(Ok(datom)),
            Ok(datom) => {
                // Datom is out of range, seek to next one
                if let Some(key) = seek_key(&datom.value, bytes, self.range.tx_value()) {
                    if let Err(err) = self.bytes_iterator.seek(key) {
                        return Some(Err(err));
                    }
                }
                self.next()
            }
            Err(err) => Some(Err(err.into())),
        }
    }
}

/// For bytes of a given datom [e a v _ _], seek to the next immediate datom in the index which
/// differs in the [e a v] combination.
fn seek_key(value: &Value, datom_bytes: &[u8], basis_tx: u64) -> Option<Vec<u8>> {
    let mut key = next_prefix(&datom_bytes[..key_size(value)])?;
    (!basis_tx).write_to(&mut key);
    Some(key)
}

/// Returns lowest value following largest value with given prefix.
///
/// In other words, computes upper bound for a prefix scan over list of keys
/// sorted in lexicographical order.  This means that a prefix scan can be
/// expressed as range scan over a right-open `[prefix, next_prefix(prefix))`
/// range.
///
/// For example, for prefix `foo` the function returns `fop`.
fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let ffs = prefix
        .iter()
        .rev()
        .take_while(|&&byte| byte == u8::MAX)
        .count();
    let mut next = prefix[..(prefix.len() - ffs)].to_vec();
    let last = next.last_mut()?;
    *last += 1;
    Some(next)
}

/// Number of bytes used to encode a datom with value `value`.
/// Excluding `tx` and `op` (prefix only).
fn key_size(value: &Value) -> usize {
    std::mem::size_of::<u64>() // Entity
        + std::mem::size_of::<u64>() // Attribute
        + value.size_hint()
}
