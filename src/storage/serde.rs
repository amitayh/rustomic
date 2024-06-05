use rust_decimal::Decimal;
use std::fmt::Debug;
use std::mem::size_of;
use std::rc::Rc;
use std::u16;
use thiserror::Error;

use crate::datom::*;
use crate::storage::*;

macro_rules! write_to_vec {
    ($first:expr $(, $rest:expr)*) => {{
        let size = $first.size_hint() $(+ $rest.size_hint())*;
        let mut buffer = Vec::with_capacity(size);
        $first.write_to(&mut buffer);
        $($rest.write_to(&mut buffer);)*
        buffer
    }};
}

pub type Bytes = Vec<u8>;

/// | Index | Sort order                      | Contains                       |
/// |-------|---------------------------------|--------------------------------|
/// | EAVT  | entity / attribute / value / tx | All datoms                     |
/// | AEVT  | attribute / entity / value / tx | All datoms                     |
/// | AVET  | attribute / value / entity / tx | Datoms with indexed attributes |
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
    /// | E  | A              | V                      | Tx   | Op     |
    /// |----|----------------|------------------------|------|--------|
    /// | 41 | release/name   | "Abbey Road"           | 1100 | Assert |
    /// | 42 | release/name   | "Magical Mystery Tour" | 1007 | Assert |
    /// | 42 | release/year   | 1967                   | 1007 | Assert |
    /// | 42 | release/artist | "The Beatles"          | 1007 | Assert |
    /// | 43 | release/name   | "Let It Be"            | 1234 | Assert |
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
    /// | A              | E  | V                      | Tx   | Op     |
    /// |----------------|----|------------------------|------|--------|
    /// | release/artist | 42 | "The Beatles"          | 1007 | Assert |
    /// | release/name   | 41 | "Abbey Road"           | 1100 | Assert |
    /// | release/name   | 42 | "Magical Mystery Tour" | 1007 | Assert |
    /// | release/name   | 43 | "Let It Be"            | 1234 | Assert |
    /// | release/year   | 42 | 1967                   | 1007 | Assert |
    Aevt,

    /// The AVET index provides efficient access to particular combinations of attribute and value.
    /// The example below shows a portion of the AVET index allowing lookup by release/names.
    ///
    /// The AVET index is more expensive to maintain than other indexes, and as such it is the only
    /// index that is not enabled by default. To maintain AVET for an attribute, specify db/index
    /// true (or some value for db/unique) when installing or altering the attribute.
    ///
    /// | A              | V                      | E  | Tx   | Op     |
    /// |----------------|------------------------|----|------|--------|
    /// | release/name   | "Abbey Road"           | 41 | 1100 | Assert |
    /// | release/name   | "Let It Be"            | 43 | 1234 | Assert |
    /// | release/name   | "Let It Be"            | 55 | 2367 | Assert |
    /// | release/name   | "Magical Mystery Tour" | 42 | 1007 | Assert |
    /// | release/year   | 1967                   | 42 | 1007 | Assert |
    /// | release/year   | 1984                   | 55 | 2367 | Assert |
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
        let mut buffer = Buffer(buffer);
        match index {
            Index::Eavt => deserialize::eavt(&mut buffer),
            Index::Aevt => deserialize::aevt(&mut buffer),
            Index::Avet => deserialize::avet(&mut buffer),
        }
    }

    mod deserialize {
        use super::*;

        pub fn eavt(buffer: &mut Buffer) -> ReadResult<Datom> {
            let entity = u64::read_from(buffer)?;
            let attribute = u64::read_from(buffer)?;
            let value = Value::read_from(buffer)?;
            let tx = !u64::read_from(buffer)?;
            let op = Op::read_from(buffer)?;
            assert!(buffer.is_empty(), "bytes remaining in buffer");
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }

        pub fn aevt(buffer: &mut Buffer) -> ReadResult<Datom> {
            let attribute = u64::read_from(buffer)?;
            let entity = u64::read_from(buffer)?;
            let value = Value::read_from(buffer)?;
            let tx = !u64::read_from(buffer)?;
            let op = Op::read_from(buffer)?;
            assert!(buffer.is_empty(), "bytes remaining in buffer");
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }

        pub fn avet(buffer: &mut Buffer) -> ReadResult<Datom> {
            let attribute = u64::read_from(buffer)?;
            let value = Value::read_from(buffer)?;
            let entity = u64::read_from(buffer)?;
            let tx = !u64::read_from(buffer)?;
            let op = Op::read_from(buffer)?;
            assert!(buffer.is_empty(), "bytes remaining in buffer");
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
    fn size_hint(&self) -> usize;
    fn write_to(&self, buffer: &mut Bytes);
}

impl Writable for u8 {
    fn size_hint(&self) -> usize {
        size_of::<Self>()
    }

    fn write_to(&self, buffer: &mut Bytes) {
        buffer.push(*self);
    }
}

impl Writable for u16 {
    fn size_hint(&self) -> usize {
        size_of::<Self>()
    }

    fn write_to(&self, buffer: &mut Bytes) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for u64 {
    fn size_hint(&self) -> usize {
        size_of::<Self>()
    }

    fn write_to(&self, buffer: &mut Bytes) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for i64 {
    fn size_hint(&self) -> usize {
        size_of::<Self>()
    }

    fn write_to(&self, buffer: &mut Bytes) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for Decimal {
    fn size_hint(&self) -> usize {
        size_of::<Self>()
    }

    fn write_to(&self, buffer: &mut Bytes) {
        buffer.extend(self.serialize());
    }
}

impl Writable for str {
    fn size_hint(&self) -> usize {
        size_of::<u16>() + // Length
        self.len()
    }

    fn write_to(&self, buffer: &mut Bytes) {
        // TODO: handle longer strings?
        if let Ok(length) = u16::try_from(self.len()) {
            length.write_to(buffer);
            buffer.extend_from_slice(self.as_bytes());
        }
    }
}

impl Writable for Value {
    fn size_hint(&self) -> usize {
        1 + // Value tag
        match self {
            Self::Nil => 0,
            Self::Decimal(value) => value.size_hint(),
            Self::U64(value) => value.size_hint(),
            Self::I64(value) => value.size_hint(),
            Self::Str(value) => value.size_hint(),
            Self::Ref(value) => value.size_hint(),
        }
    }

    fn write_to(&self, buffer: &mut Bytes) {
        match self {
            Self::Nil => {
                value::TAG_NIL.write_to(buffer);
            }
            Self::U64(value) => {
                value::TAG_U64.write_to(buffer);
                value.write_to(buffer);
            }
            Self::I64(value) => {
                value::TAG_I64.write_to(buffer);
                value.write_to(buffer);
            }
            Self::Decimal(value) => {
                value::TAG_DEC.write_to(buffer);
                value.write_to(buffer);
            }
            Self::Str(value) => {
                value::TAG_STR.write_to(buffer);
                value.write_to(buffer);
            }
            Self::Ref(value) => {
                value::TAG_REF.write_to(buffer);
                value.write_to(buffer);
            }
        }
    }
}

impl Writable for Op {
    fn size_hint(&self) -> usize {
        1
    }

    fn write_to(&self, buffer: &mut Bytes) {
        match self {
            Self::Assert => op::TAG_ASSERT,
            Self::Retract => op::TAG_RETRACT,
        }
        .write_to(buffer)
    }
}

// -------------------------------------------------------------------------------------------------

type ReadResult<T> = Result<T, ReadError>;

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

struct Buffer<'a>(&'a [u8]);

impl<'a> Buffer<'a> {
    /// Removes and returns the first `num_bytes` from buffer.
    /// Fails with `ReadError::EndOfInput` if not enough bytes exist.
    fn consume(&mut self, num_bytes: usize) -> ReadResult<&[u8]> {
        let Buffer(buffer) = self;
        if num_bytes > buffer.len() {
            return Err(ReadError::EndOfInput);
        }
        let result = &buffer[..num_bytes];
        *buffer = &buffer[num_bytes..];
        Ok(result)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

trait Readable: Sized {
    /// Reads `Self` from buffer.
    /// Consumes as many bytes required from the buffer.
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self>;
}

impl<const N: usize> Readable for [u8; N] {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        buffer
            .consume(N)?
            .try_into()
            .map_err(|_| ReadError::TryFromSliceError)
    }
}

impl Readable for u8 {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        let buffer = buffer.consume(1)?;
        Ok(buffer[0])
    }
}

impl Readable for u16 {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        let bytes = <[u8; 2]>::read_from(buffer)?;
        Ok(Self::from_be_bytes(bytes))
    }
}

impl Readable for u64 {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        let bytes = <[u8; 8]>::read_from(buffer)?;
        Ok(Self::from_be_bytes(bytes))
    }
}

impl Readable for i64 {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        let bytes = <[u8; 8]>::read_from(buffer)?;
        Ok(Self::from_be_bytes(bytes))
    }
}

impl Readable for Decimal {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        let bytes = <[u8; 16]>::read_from(buffer)?;
        Ok(Self::deserialize(bytes))
    }
}

impl Readable for Rc<str> {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        let length = u16::read_from(buffer)?;
        let buffer = buffer.consume(length.into())?;
        let str = std::str::from_utf8(buffer)?;
        Ok(Rc::from(str))
    }
}

impl Readable for Value {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        match u8::read_from(buffer)? {
            value::TAG_NIL => Ok(Value::Nil),
            value::TAG_U64 => Ok(Value::U64(u64::read_from(buffer)?)),
            value::TAG_I64 => Ok(Value::I64(i64::read_from(buffer)?)),
            value::TAG_DEC => Ok(Value::Decimal(Decimal::read_from(buffer)?)),
            value::TAG_STR => Ok(Value::Str(<Rc<str>>::read_from(buffer)?)),
            value::TAG_REF => Ok(Value::Ref(u64::read_from(buffer)?)),
            _ => Err(ReadError::InvalidInput),
        }
    }
}

impl Readable for Op {
    fn read_from(buffer: &mut Buffer) -> ReadResult<Self> {
        match u8::read_from(buffer)? {
            op::TAG_ASSERT => Ok(Op::Assert),
            op::TAG_RETRACT => Ok(Op::Retract),
            _ => Err(ReadError::InvalidInput),
        }
    }
}
