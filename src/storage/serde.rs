use rust_decimal::Decimal;
use std::fmt::Debug;
use std::io::Cursor;
use std::io::Read;
use std::mem::size_of;
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
        pub start: Option<Vec<u8>>,
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

pub mod datom {
    use super::*;

    pub mod serialize {
        use super::*;

        pub fn eavt(datom: &Datom) -> Vec<u8> {
            write_to_vec!(
                datom.entity,
                datom.attribute,
                datom.value,
                !datom.tx, // Keep tx in descending order
                datom.op
            )
        }

        pub fn aevt(datom: &Datom) -> Vec<u8> {
            write_to_vec!(
                datom.attribute,
                datom.entity,
                datom.value,
                !datom.tx, // Keep tx in descending order
                datom.op
            )
        }

        pub fn avet(datom: &Datom) -> Vec<u8> {
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
        let mut cursor = Cursor::new(buffer);
        match index {
            Index::Eavt => deserialize::eavt(&mut cursor),
            Index::Aevt => deserialize::aevt(&mut cursor),
            Index::Avet => deserialize::avet(&mut cursor),
        }
    }

    mod deserialize {
        use super::*;

        pub fn eavt(cursor: &mut Cursor<&[u8]>) -> ReadResult<Datom> {
            let entity = u64::read_from(cursor)?;
            let attribute = u64::read_from(cursor)?;
            let value = Value::read_from(cursor)?;
            let tx = !u64::read_from(cursor)?;
            let op = Op::read_from(cursor)?;
            //assert!(buffer.().is_empty(), "bytes remaining in buffer");
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }

        pub fn aevt(cursor: &mut Cursor<&[u8]>) -> ReadResult<Datom> {
            let attribute = u64::read_from(cursor)?;
            let entity = u64::read_from(cursor)?;
            let value = Value::read_from(cursor)?;
            let tx = !u64::read_from(cursor)?;
            let op = Op::read_from(cursor)?;
            // assert!(buffer.is_empty(), "bytes remaining in buffer");
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }

        pub fn avet(cursor: &mut Cursor<&[u8]>) -> ReadResult<Datom> {
            let attribute = u64::read_from(cursor)?;
            let value = Value::read_from(cursor)?;
            let entity = u64::read_from(cursor)?;
            let tx = !u64::read_from(cursor)?;
            let op = Op::read_from(cursor)?;
            // assert!(buffer.is_empty(), "bytes remaining in buffer");
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

type ReadResult<T> = Result<T, ReadError>;

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("invalid input")]
    InvalidInput,
    #[error("UTF8 error")]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error("I/O error")]
    IoError(#[from] std::io::Error),
}

trait Readable: Sized {
    /// Reads `Self` from buffer.
    /// Consumes as many bytes required from the buffer.
    fn read_from(buffer: &mut impl Read) -> ReadResult<Self>;
}

pub trait Writable {
    /// Number of bytes required for encoding `Self`.
    fn size_hint(&self) -> usize;

    /// Writes `self` to buffer in binary format.
    fn write_to(&self, buffer: &mut Vec<u8>);
}

impl<const N: usize> Readable for [u8; N] {
    fn read_from(buffer: &mut impl Read) -> ReadResult<Self> {
        let mut bytes = [0; N];
        buffer.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

macro_rules! primitive_impl {
    ($type:ty) => {
        impl Readable for $type {
            fn read_from(buffer: &mut impl Read) -> ReadResult<Self> {
                let bytes = <[u8; size_of::<Self>()]>::read_from(buffer)?;
                Ok(Self::from_be_bytes(bytes))
            }
        }

        impl Writable for $type {
            fn size_hint(&self) -> usize {
                size_of::<Self>()
            }

            fn write_to(&self, buffer: &mut Vec<u8>) {
                buffer.extend_from_slice(&self.to_be_bytes());
            }
        }
    };
}

primitive_impl!(u8);
primitive_impl!(u16);
primitive_impl!(u64);
primitive_impl!(i64);

mod decimal {
    use super::*;

    impl Readable for Decimal {
        fn read_from(buffer: &mut impl Read) -> ReadResult<Self> {
            let bytes = <[u8; 16]>::read_from(buffer)?;
            Ok(Self::deserialize(bytes))
        }
    }

    impl Writable for Decimal {
        fn size_hint(&self) -> usize {
            size_of::<Self>()
        }

        fn write_to(&self, buffer: &mut Vec<u8>) {
            buffer.extend(self.serialize());
        }
    }
}

mod string {
    use super::*;

    impl Readable for String {
        fn read_from(buffer: &mut impl Read) -> ReadResult<Self> {
            let length = u16::read_from(buffer)?;
            let mut bytes = vec![0; length.into()];
            buffer.read_exact(&mut bytes)?;
            let string = String::from_utf8(bytes)?;
            Ok(string)
        }
    }

    impl Writable for str {
        fn size_hint(&self) -> usize {
            size_of::<u16>() + // Length
            self.len()
        }

        fn write_to(&self, buffer: &mut Vec<u8>) {
            // TODO: handle longer strings?
            u16::try_from(self.len())
                .expect("String to long")
                .write_to(buffer);

            buffer.extend_from_slice(self.as_bytes());
        }
    }
}

mod value {
    use super::*;

    const TAG_NIL: u8 = 0x00;
    const TAG_U64: u8 = 0x01;
    const TAG_I64: u8 = 0x02;
    const TAG_DEC: u8 = 0x03;
    const TAG_STR: u8 = 0x04;
    const TAG_REF: u8 = 0x05;

    impl Readable for Value {
        fn read_from(buffer: &mut impl Read) -> ReadResult<Self> {
            match u8::read_from(buffer)? {
                TAG_NIL => Ok(Value::Nil),
                TAG_U64 => Ok(Value::U64(u64::read_from(buffer)?)),
                TAG_I64 => Ok(Value::I64(i64::read_from(buffer)?)),
                TAG_DEC => Ok(Value::Decimal(Decimal::read_from(buffer)?)),
                TAG_STR => Ok(Value::Str(String::read_from(buffer)?)),
                TAG_REF => Ok(Value::Ref(u64::read_from(buffer)?)),
                _ => Err(ReadError::InvalidInput),
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

        fn write_to(&self, buffer: &mut Vec<u8>) {
            match self {
                Self::Nil => {
                    TAG_NIL.write_to(buffer);
                }
                Self::U64(value) => {
                    TAG_U64.write_to(buffer);
                    value.write_to(buffer);
                }
                Self::I64(value) => {
                    TAG_I64.write_to(buffer);
                    value.write_to(buffer);
                }
                Self::Decimal(value) => {
                    TAG_DEC.write_to(buffer);
                    value.write_to(buffer);
                }
                Self::Str(value) => {
                    TAG_STR.write_to(buffer);
                    value.write_to(buffer);
                }
                Self::Ref(value) => {
                    TAG_REF.write_to(buffer);
                    value.write_to(buffer);
                }
            }
        }
    }
}

mod op {
    use super::*;

    const TAG_ASSERT: u8 = 0x00;
    const TAG_RETRACT: u8 = 0x01;

    impl Readable for Op {
        fn read_from(buffer: &mut impl Read) -> ReadResult<Self> {
            match u8::read_from(buffer)? {
                TAG_ASSERT => Ok(Op::Assert),
                TAG_RETRACT => Ok(Op::Retract),
                _ => Err(ReadError::InvalidInput),
            }
        }
    }

    impl Writable for Op {
        fn size_hint(&self) -> usize {
            1
        }

        fn write_to(&self, buffer: &mut Vec<u8>) {
            match self {
                Self::Assert => TAG_ASSERT,
                Self::Retract => TAG_RETRACT,
            }
            .write_to(buffer)
        }
    }
}
