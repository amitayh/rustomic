use rust_decimal::Decimal;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use crate::datom::*;
use crate::storage::*;

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
    use super::*;

    pub struct RestrictedIndexRange {
        pub restricts: Restricts,
        pub index: Index,
        pub start: Option<Bytes>,
    }

    impl RestrictedIndexRange {
        pub fn contains(&self, datom: &Datom) -> bool {
            self.restricts.test(datom)
        }

        pub fn tx_value(&self) -> u64 {
            self.restricts.tx.value()
        }
    }

    impl From<Restricts> for RestrictedIndexRange {
        fn from(restricts: Restricts) -> Self {
            let (index, start) = match &restricts {
                Restricts {
                    entity: Some(entity),
                    attribute: Some(attribute),
                    value: Some(value),
                    tx,
                    ..
                } => (
                    Index::Eavt,
                    Some(write_to_vec!(entity, attribute, value, &!(tx.value()))),
                ),
                Restricts {
                    entity: Some(entity),
                    attribute: Some(attribute),
                    ..
                } => (Index::Eavt, Some(write_to_vec!(entity, attribute))),
                Restricts {
                    entity: Some(entity),
                    ..
                } => (Index::Eavt, Some(write_to_vec!(entity))),
                Restricts {
                    attribute: Some(attribute),
                    value: Some(value),
                    ..
                } => (Index::Avet, Some(write_to_vec!(attribute, value))),
                Restricts {
                    attribute: Some(attribute),
                    ..
                } => (Index::Aevt, Some(write_to_vec!(attribute))),
                _ => (Index::Aevt, None),
            };
            Self {
                restricts,
                index,
                start,
            }
        }
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
    pub const TAG_ASSERT: u8 = 0x00;
    pub const TAG_RETRACT: u8 = 0x01;
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

// -------------------------------------------------------------------------------------------------

/*
impl std::io::Write for Value {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        todo!();
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

impl std::io::Read for Value {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        todo!()
    }
}
*/

// -------------------------------------------------------------------------------------------------

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
            Self::Assert => op::TAG_ASSERT,
            Self::Retract => op::TAG_RETRACT,
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
            op::TAG_ASSERT => Ok(Op::Assert),
            op::TAG_RETRACT => Ok(Op::Retract),
            _ => Err(ReadError::InvalidInput),
        }
    }
}
