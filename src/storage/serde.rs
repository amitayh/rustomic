use either::Either;
use rust_decimal::Decimal;
use std::fmt::Debug;
use std::ops::Bound;
use std::rc::Rc;
use thiserror::Error;

use crate::datom::*;
use crate::storage::*;

// -------------------------------------------------------------------------------------------------

trait SeekableIterator {
    type Error: std::error::Error;

    fn next(&mut self) -> Option<Result<&[u8], Self::Error>>;

    fn seek(&mut self, key: &[u8]) -> Result<(), Self::Error>;
}

struct DatomsIterator<T> {
    restricts: Restricts,
    range: index::IndexedRange,
    bytes_iterator: T,
}

impl<T> DatomsIterator<T> {
    fn new(bytes_iterator: T, restricts: Restricts) -> Self {
        let range = index::IndexedRange::from(&restricts);
        Self {
            restricts,
            range,
            bytes_iterator,
        }
    }
}

impl<T: SeekableIterator> Iterator for DatomsIterator<T> {
    type Item = Result<Datom, Either<T::Error, ReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = match self.bytes_iterator.next()? {
            Ok(bytes) => bytes,
            Err(err) => {
                return Some(Err(Either::Left(err)));
            }
        };
        match datom::deserialize(self.range.0, bytes) {
            Ok(datom) if self.restricts.test(&datom) => Some(Ok(datom)),
            Ok(datom) => {
                if let Some(key) = index::seek_key(&datom.value, bytes, self.restricts.tx.value()) {
                    if let Err(err) = self.bytes_iterator.seek(&key) {
                        return Some(Err(Either::Left(err)));
                    }
                }
                self.next()
            }
            Err(err) => {
                return Some(Err(Either::Right(err)));
            }
        }
    }
}

// -------------------------------------------------------------------------------------------------

use value::SIZE_DECIMAL;

macro_rules! write_to_vec {
    ($first:expr $(, $rest:expr)*) => {{
        let size = $first.size() $(+ $rest.size())*;
        let mut buffer = Vec::with_capacity(size);
        $first.write(&mut buffer);
        $($rest.write(&mut buffer);)*
        buffer
    }};
}

pub type Bytes = Vec<u8>;

/// +-------+---------------------------------+--------------------------------+
/// | Index | Sort order                      | Contains                       |
/// +-------+---------------------------------+--------------------------------+
/// | EAVT  | entity / attribute / value / tx | All datoms                     |
/// | AEVT  | attribute / entity / value / tx | All datoms                     |
/// | AVET  | attribute / value / entity / tx | Datoms with indexed attributes |
/// +-------+---------------------------------+--------------------------------+
///
/// https://docs.datomic.com/pro/query/indexes.html
#[derive(Debug, Clone, Copy)]
pub enum Index {
    /// The EAVT index provides efficient access to everything about a given entity. Conceptually
    /// this is very similar to row access style in a SQL database, except that entities can
    /// possess arbitrary attributes rather than being limited to a predefined set of columns.
    ///
    /// The example below shows all of the facts about entity 42 grouped together:
    ///
    ///  +----+----------------+------------------------+------+-------+
    ///  | E  | A              | V                      | Tx   | Op    |
    ///  +----+----------------+------------------------+------+-------+
    ///  | 41 | release/name   | "Abbey Road"           | 1100 | Added |
    ///  | 42 | release/name   | "Magical Mystery Tour" | 1007 | Added | *
    ///  | 42 | release/year   | 1967                   | 1007 | Added | *
    ///  | 42 | release/artist | "The Beatles"          | 1007 | Added | *
    ///  | 43 | release/name   | "Let It Be"            | 1234 | Added |
    ///  +----+----------------+------------------------+------+-------+
    ///
    /// EAVT is also useful in master or detail lookups, since the references to detail entities
    /// are just ordinary versus alongside the scalar attributes of the master entity. Better
    /// still, Datomic assigns entity ids so that when master and detail records are created in the
    /// same transaction, they are colocated in EAVT.
    Eavt,

    /// The AEVT index provides efficient access to all values for a given attribute, comparable to
    /// the traditional column access style. In the table below, notice how all release/name
    /// attributes are grouped together. This allows Datomic to efficiently query for all values of
    /// the release/name attribute, because they reside next to one another in this index.
    ///
    ///  +----------------+----+------------------------+------+-------+
    ///  | A              | E  | V                      | Tx   | Op    |
    ///  +----------------+----+------------------------+------+-------+
    ///  | release/artist | 42 | "The Beatles"          | 1007 | Added |
    ///  | release/name   | 41 | "Abbey Road"           | 1100 | Added | *
    ///  | release/name   | 42 | "Magical Mystery Tour" | 1007 | Added | *
    ///  | release/name   | 43 | "Let It Be"            | 1234 | Added | *
    ///  | release/year   | 42 | 1967                   | 1007 | Added |
    ///  +----------------+----+------------------------+------+-------+
    Aevt,

    /// The AVET index provides efficient access to particular combinations of attribute and value.
    /// The example below shows a portion of the AVET index allowing lookup by release/names.
    ///
    /// The AVET index is more expensive to maintain than other indexes, and as such it is the only
    /// index that is not enabled by default. To maintain AVET for an attribute, specify db/index
    /// true (or some value for db/unique) when installing or altering the attribute.
    ///
    ///  +----------------+------------------------+----+------+-------+
    ///  | A              | V                      | E  | Tx   | Op    |
    ///  +----------------+------------------------+----+------+-------+
    ///  | release/name   | "Abbey Road"           | 41 | 1100 | Added |
    ///  | release/name   | "Let It Be"            | 43 | 1234 | Added | *
    ///  | release/name   | "Let It Be"            | 55 | 2367 | Added | *
    ///  | release/name   | "Magical Mystery Tour" | 42 | 1007 | Added |
    ///  | release/year   | 1967                   | 42 | 1007 | Added |
    ///  | release/year   | 1984                   | 55 | 2367 | Added |
    ///  +----------------+------------------------+----+------+-------+
    Avet,
}

pub mod index {
    use std::ops::RangeBounds;

    use super::*;

    pub enum Range {
        Full,
        From(Bytes),
        Between(Bytes, Bytes),
    }

    impl From<Bytes> for Range {
        fn from(start: Bytes) -> Self {
            match next_prefix(&start) {
                Some(end) => Self::Between(start, end),
                None => Self::From(start),
            }
        }
    }

    impl RangeBounds<[u8]> for Range {
        fn start_bound(&self) -> Bound<&[u8]> {
            match self {
                Self::Full => Bound::Unbounded,
                Self::From(start) => Bound::Included(start),
                Self::Between(start, _) => Bound::Included(start),
            }
        }

        fn end_bound(&self) -> Bound<&[u8]> {
            match self {
                Self::Full | Self::From(_) => Bound::Unbounded,
                Self::Between(_, end) => Bound::Excluded(end),
            }
        }
    }

    pub struct IndexedRange(pub Index, pub Range);

    impl From<&Restricts> for IndexedRange {
        fn from(restricts: &Restricts) -> Self {
            match restricts {
                Restricts {
                    entity: Some(entity),
                    attribute: Some(attribute),
                    value: Some(value),
                    tx,
                    ..
                } => Self(
                    Index::Eavt,
                    Range::from(write_to_vec!(entity, attribute, value, &!(tx.value()))),
                ),
                Restricts {
                    entity: Some(entity),
                    attribute: Some(attribute),
                    ..
                } => Self(Index::Eavt, Range::from(write_to_vec!(entity, attribute))),
                Restricts {
                    entity: Some(entity),
                    ..
                } => Self(Index::Eavt, Range::from(write_to_vec!(entity))),
                Restricts {
                    attribute: Some(attribute),
                    value: Some(value),
                    ..
                } => Self(Index::Avet, Range::from(write_to_vec!(attribute, value))),
                Restricts {
                    attribute: Some(attribute),
                    ..
                } => Self(Index::Aevt, Range::from(write_to_vec!(attribute))),
                _ => Self(Index::Aevt, Range::Full),
            }
        }
    }

    /// For bytes of a given datom [e a v _ _], seek to the next immediate datom in the index which
    /// differs in the [e a v] combination.
    pub fn seek_key(value: &Value, datom_bytes: &[u8], basis_tx: u64) -> Option<Bytes> {
        next_prefix(&datom_bytes[..key_size(value)]).map(|mut key| {
            // Also include the tx ID to quickly skip datoms that don't belong to DB snapshot.
            (!basis_tx).write(&mut key);
            key
        })
    }

    /// Number of bytes used to encode a datom with value `value`.
    /// Excluding `tx` and `op` (prefix only).
    fn key_size(value: &Value) -> usize {
        std::mem::size_of::<u64>() // Entity
        + std::mem::size_of::<u64>() // Attribute
        + value.size()
    }

    /// Returns lowest value following largest value with given prefix.
    ///
    /// In other words, computes upper bound for a prefix scan over list of keys
    /// sorted in lexicographical order.  This means that a prefix scan can be
    /// expressed as range scan over a right-open `[prefix, next_prefix(prefix))`
    /// range.
    ///
    /// For example, for prefix `foo` the function returns `fop`.
    fn next_prefix(prefix: &[u8]) -> Option<Bytes> {
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
}

mod value {
    pub const TAG_NIL: u8 = 0x00;
    pub const TAG_U64: u8 = 0x01;
    pub const TAG_I64: u8 = 0x02;
    pub const TAG_DEC: u8 = 0x03;
    pub const TAG_STR: u8 = 0x04;
    pub const TAG_REF: u8 = 0x05;

    pub const SIZE_DECIMAL: usize = 16;
}

mod op {
    pub const TAG_ADDED: u8 = 0x00;
    pub const TAG_RETRACTED: u8 = 0x01;
}

pub mod datom {
    use super::*;

    pub mod serialize {
        use super::*;

        pub fn eavt(datom: &Datom) -> Bytes {
            write_to_vec!(
                datom.entity,
                datom.attribute,
                datom.value,
                !datom.tx, // Keep tx in descending order
                datom.op
            )
        }

        pub fn aevt(datom: &Datom) -> Bytes {
            write_to_vec!(
                datom.attribute,
                datom.entity,
                datom.value,
                !datom.tx, // Keep tx in descending order
                datom.op
            )
        }

        pub fn avet(datom: &Datom) -> Bytes {
            write_to_vec!(
                datom.attribute,
                datom.value,
                datom.entity,
                !datom.tx, // Keep tx in descending order
                datom.op
            )
        }
    }

    pub fn deserialize(index: Index, buffer: &[u8]) -> ReadResult<Datom> {
        let mut reader = Reader::new(buffer);
        match index {
            Index::Eavt => deserialize::eavt(&mut reader),
            Index::Aevt => deserialize::aevt(&mut reader),
            Index::Avet => deserialize::avet(&mut reader),
        }
    }

    mod deserialize {
        use super::*;

        pub fn eavt(reader: &mut Reader) -> ReadResult<Datom> {
            let entity = reader.read()?;
            let attribute = reader.read()?;
            let value = reader.read()?;
            let tx: u64 = !reader.read()?;
            let op = reader.read()?;
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }

        pub fn aevt(reader: &mut Reader) -> ReadResult<Datom> {
            let attribute = reader.read()?;
            let entity = reader.read()?;
            let value = reader.read()?;
            let tx: u64 = !reader.read()?;
            let op = reader.read()?;
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }

        pub fn avet(reader: &mut Reader) -> ReadResult<Datom> {
            let attribute = reader.read()?;
            let value = reader.read()?;
            let entity = reader.read()?;
            let tx: u64 = !reader.read()?;
            let op = reader.read()?;
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }
    }
}

// -------------------------------------------------------------------------------------------------

pub trait Writable {
    fn size(&self) -> usize;
    fn write(&self, buffer: &mut Bytes);
}

impl Writable for u8 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn write(&self, buffer: &mut Bytes) {
        buffer.push(*self);
    }
}

impl Writable for u16 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn write(&self, buffer: &mut Bytes) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for u64 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn write(&self, buffer: &mut Bytes) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for i64 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn write(&self, buffer: &mut Bytes) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

// TODO: handle longer strings?
impl Writable for str {
    fn size(&self) -> usize {
        std::mem::size_of::<u16>() + // Length
        self.len()
    }

    fn write(&self, buffer: &mut Bytes) {
        if let Ok(length) = u16::try_from(self.len()) {
            length.write(buffer);
            buffer.extend_from_slice(self.as_bytes());
        }
    }
}

impl Writable for Value {
    fn size(&self) -> usize {
        std::mem::size_of::<u8>() + // Value tag
        match self {
            Self::Nil => 0,
            Self::Decimal(_) => SIZE_DECIMAL,
            Self::U64(value) => value.size(),
            Self::I64(value) => value.size(),
            Self::Str(value) => value.size(),
            Self::Ref(value) => value.size(),
        }
    }

    fn write(&self, buffer: &mut Bytes) {
        match self {
            Self::Nil => {
                value::TAG_NIL.write(buffer);
            }
            Self::U64(value) => {
                value::TAG_U64.write(buffer);
                value.write(buffer);
            }
            Self::I64(value) => {
                value::TAG_I64.write(buffer);
                value.write(buffer);
            }
            Self::Decimal(value) => {
                value::TAG_DEC.write(buffer);
                buffer.extend(value.serialize());
            }
            Self::Str(value) => {
                value::TAG_STR.write(buffer);
                value.write(buffer);
            }
            Self::Ref(value) => {
                value::TAG_REF.write(buffer);
                value.write(buffer);
            }
        }
    }
}

impl Writable for Op {
    fn size(&self) -> usize {
        1
    }

    fn write(&self, buffer: &mut Bytes) {
        match self {
            Self::Added => op::TAG_ADDED,
            Self::Retracted => op::TAG_RETRACTED,
        }
        .write(buffer)
    }
}

// -------------------------------------------------------------------------------------------------

pub struct Reader<'a> {
    buffer: &'a [u8],
    index: usize,
}

type ReadResult<T> = Result<T, ReadError>;

impl<'a> Reader<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, index: 0 }
    }

    fn read_next(&mut self, num_bytes: usize) -> ReadResult<&[u8]> {
        if self.index + num_bytes > self.buffer.len() {
            return Err(ReadError::EndOfInput);
        }
        let from = self.index;
        self.index += num_bytes;
        Ok(&self.buffer[from..self.index])
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadError {
    #[error("end of input")]
    EndOfInput,
    #[error("invalid input")]
    InvalidInput,
    #[error("utf8 error")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("try from slice error")]
    TryFromSliceError,
}

// -------------------------------------------------------------------------------------------------

trait Readable<T> {
    fn read(&mut self) -> ReadResult<T>;
}

impl<'a> Readable<u8> for Reader<'a> {
    fn read(&mut self) -> ReadResult<u8> {
        let buffer = self.read_next(1)?;
        Ok(buffer[0])
    }
}

impl<'a> Readable<u16> for Reader<'a> {
    fn read(&mut self) -> ReadResult<u16> {
        let buffer = self.read_next(2)?;
        Ok(u16::from_be_bytes([buffer[0], buffer[1]]))
    }
}

impl<'a> Readable<u64> for Reader<'a> {
    fn read(&mut self) -> ReadResult<u64> {
        let buffer = self.read_next(8)?;
        Ok(u64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]))
    }
}

impl<'a> Readable<i64> for Reader<'a> {
    fn read(&mut self) -> ReadResult<i64> {
        let buffer = self.read_next(8)?;
        Ok(i64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]))
    }
}

impl<'a> Readable<Decimal> for Reader<'a> {
    fn read(&mut self) -> ReadResult<Decimal> {
        let buffer = self.read_next(SIZE_DECIMAL)?;
        match buffer.try_into() {
            Ok(bytes) => Ok(Decimal::deserialize(bytes)),
            Err(_) => Err(ReadError::TryFromSliceError),
        }
    }
}

impl<'a> Readable<Rc<str>> for Reader<'a> {
    fn read(&mut self) -> ReadResult<Rc<str>> {
        let length: u16 = self.read()?;
        let buffer = self.read_next(length.into())?;
        let str = std::str::from_utf8(buffer)?;
        Ok(Rc::from(str))
    }
}

impl<'a> Readable<Value> for Reader<'a> {
    fn read(&mut self) -> ReadResult<Value> {
        match self.read()? {
            value::TAG_NIL => Ok(Value::Nil),
            value::TAG_U64 => Ok(Value::U64(self.read()?)),
            value::TAG_I64 => Ok(Value::I64(self.read()?)),
            value::TAG_DEC => Ok(Value::Decimal(self.read()?)),
            value::TAG_STR => Ok(Value::Str(self.read()?)),
            value::TAG_REF => Ok(Value::Ref(self.read()?)),
            _ => Err(ReadError::InvalidInput),
        }
    }
}

impl<'a> Readable<Op> for Reader<'a> {
    fn read(&mut self) -> ReadResult<Op> {
        match self.read()? {
            op::TAG_ADDED => Ok(Op::Added),
            op::TAG_RETRACTED => Ok(Op::Retracted),
            _ => Err(ReadError::InvalidInput),
        }
    }
}
