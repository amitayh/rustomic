use std::rc::Rc;
use thiserror::Error;

use crate::datom::*;
use crate::storage::*;

macro_rules! write_to_vec {
    ($first:expr $(, $rest:expr)*) => {{
        let size = $first.size() $(+ $rest.size())*;
        let mut buffer = Vec::with_capacity(size);
        $first.write(&mut buffer);
        $($rest.write(&mut buffer);)*
        buffer
    }};
}

// +-------+---------------------------------+--------------------------------+
// | Index | Sort order                      | Contains                       |
// +-------+---------------------------------+--------------------------------+
// | EAVT  | entity / attribute / value / tx | All datoms                     |
// | AEVT  | attribute / entity / value / tx | All datoms                     |
// | AVET  | attribute / value / entity / tx | Datoms with indexed attributes |
// +-------+---------------------------------+--------------------------------+
//
// https://docs.datomic.com/pro/query/indexes.html
pub mod index {
    use super::*;
    use crate::query::pattern::{AttributePattern, EntityPattern, TxPattern, ValuePattern};

    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    //
    // The example below shows all of the facts about entity 42 grouped together:
    //
    //   +----+----------------+------------------------+------+--------+
    //   | E  | A              | V                      | Tx   | Op    |
    //   +----+----------------+------------------------+------+--------+
    //   | 41 | release/name   | "Abbey Road"           | 1100 | Added |
    // * | 42 | release/name   | "Magical Mystery Tour" | 1007 | Added |
    // * | 42 | release/year   | 1967                   | 1007 | Added |
    // * | 42 | release/artist | "The Beatles"          | 1007 | Added |
    //   | 43 | release/name   | "Let It Be"            | 1234 | Added |
    //   +----+----------------+------------------------+------+--------+
    //
    // EAVT is also useful in master or detail lookups, since the references to detail entities are
    // just ordinary versus alongside the scalar attributes of the master entity. Better still,
    // Datomic assigns entity ids so that when master and detail records are created in the same
    // transaction, they are colocated in EAVT.
    pub const TAG_EAVT: u8 = 0x00;

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all release/name
    // attributes are grouped together. This allows Datomic to efficiently query for all values of
    // the release/name attribute, because they reside next to one another in this index.
    //
    //   +----------------+----+------------------------+------+--------+
    //   | A              | E  | V                      | Tx   | Op    |
    //   +----------------+----+------------------------+------+--------+
    //   | release/artist | 42 | "The Beatles"          | 1007 | Added |
    // * | release/name   | 41 | "Abbey Road"           | 1100 | Added |
    // * | release/name   | 42 | "Magical Mystery Tour" | 1007 | Added |
    // * | release/name   | 43 | "Let It Be"            | 1234 | Added |
    //   | release/year   | 42 | 1967                   | 1007 | Added |
    //   +----------------+----+------------------------+------+--------+
    pub const TAG_AEVT: u8 = 0x01;

    // The AVET index provides efficient access to particular combinations of attribute and value.
    // The example below shows a portion of the AVET index allowing lookup by release/names.
    //
    // The AVET index is more expensive to maintain than other indexes, and as such it is the only
    // index that is not enabled by default. To maintain AVET for an attribute, specify db/index
    // true (or some value for db/unique) when installing or altering the attribute.
    //
    //   +----------------+------------------------+----+------+--------+
    //   | A              | V                      | E  | Tx   | Op    |
    //   +----------------+------------------------+----+------+--------+
    //   | release/name   | "Abbey Road"           | 41 | 1100 | Added |
    // * | release/name   | "Let It Be"            | 43 | 1234 | Added |
    // * | release/name   | "Let It Be"            | 55 | 2367 | Added |
    //   | release/name   | "Magical Mystery Tour" | 42 | 1007 | Added |
    //   | release/year   | 1967                   | 42 | 1007 | Added |
    //   | release/year   | 1984                   | 55 | 2367 | Added |
    //   +----------------+------------------------+----+------+--------+
    pub const TAG_AVET: u8 = 0x02;

    pub fn key_range(clause: &Clause) -> (Vec<u8>, Vec<u8>) {
        let start = match clause {
            Clause {
                entity: EntityPattern::Id(entity),
                attribute: AttributePattern::Id(attribute),
                value: ValuePattern::Constant(value),
                tx: TxPattern::Constant(tx),
            } => write_to_vec!(&TAG_EAVT, entity, attribute, value, &!tx),
            Clause {
                entity: EntityPattern::Id(entity),
                attribute: AttributePattern::Id(attribute),
                value: ValuePattern::Constant(value),
                tx: _,
            } => write_to_vec!(&TAG_EAVT, entity, attribute, value),
            Clause {
                entity: EntityPattern::Id(entity),
                attribute: AttributePattern::Id(attribute),
                value: _,
                tx: _,
            } => write_to_vec!(&TAG_EAVT, entity, attribute),
            Clause {
                entity: EntityPattern::Id(entity),
                attribute: _,
                value: _,
                tx: _,
            } => write_to_vec!(&TAG_EAVT, entity),
            Clause {
                entity: _,
                attribute: AttributePattern::Id(attribute),
                value: ValuePattern::Constant(value),
                tx: _,
            } => write_to_vec!(&TAG_AVET, attribute, value),
            Clause {
                entity: _,
                attribute: AttributePattern::Id(attribute),
                value: _,
                tx: _,
            } => write_to_vec!(&TAG_AEVT, attribute),
            _ => write_to_vec!(&TAG_AEVT),
        };
        let end = next_prefix(&start);
        (start, end)
    }

    pub fn seek_key(datom: &Datom, datom_bytes: &[u8]) -> Vec<u8> {
        next_prefix(&datom_bytes[..key_size(datom)])
    }

    fn key_size(datom: &Datom) -> usize {
        std::mem::size_of::<u8>() // Index tag
            + datom.entity.size()
            + datom.attribute.size()
            + datom.value.size()
    }

    /// Returns lowest value following largest value with given prefix.
    ///
    /// In other words, computes upper bound for a prefix scan over list of keys
    /// sorted in lexicographical order.  This means that a prefix scan can be
    /// expressed as range scan over a right-open `[prefix, next_prefix(prefix))`
    /// range.
    ///
    /// For example, for prefix `foo` the function returns `fop`.
    fn next_prefix(prefix: &[u8]) -> Vec<u8> {
        let ffs = prefix
            .iter()
            .rev()
            .take_while(|&&byte| byte == u8::MAX)
            .count();
        let mut next = (&prefix[..(prefix.len() - ffs)]).to_vec();
        if let Some(last) = next.last_mut() {
            *last += 1;
        }
        next
    }
}

mod value {
    pub const TAG_U64: u8 = 0x00;
    pub const TAG_I64: u8 = 0x01;
    pub const TAG_STR: u8 = 0x02;
}

mod op {
    pub const TAG_ADDED: u8 = 0x00;
    pub const TAG_RETRACTED: u8 = 0x01;
}

pub mod datom {
    use super::*;

    pub mod serialize {
        use super::*;

        pub fn eavt(datom: &Datom) -> Vec<u8> {
            write_to_vec!(
                index::TAG_EAVT,
                datom.entity,
                datom.attribute,
                datom.value,
                !datom.tx, // Keep tx in descending order
                datom.op
            )
        }

        pub fn aevt(datom: &Datom) -> Vec<u8> {
            write_to_vec!(
                index::TAG_AEVT,
                datom.attribute,
                datom.entity,
                datom.value,
                !datom.tx, // Keep tx in descending order
                datom.op
            )
        }

        pub fn avet(datom: &Datom) -> Vec<u8> {
            write_to_vec!(
                index::TAG_AVET,
                datom.attribute,
                datom.value,
                datom.entity,
                !datom.tx, // Keep tx in descending order
                datom.op
            )
        }
    }

    pub fn deserialize(buffer: &[u8]) -> ReadResult<Datom> {
        let mut reader = Reader::new(buffer);
        match reader.read()? {
            index::TAG_EAVT => deserialize::eavt(&mut reader),
            index::TAG_AEVT => deserialize::aevt(&mut reader),
            index::TAG_AVET => deserialize::avet(&mut reader),
            _ => Err(ReadError::InvalidInput),
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
    fn write(&self, buffer: &mut Vec<u8>);
}

impl Writable for u8 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self);
    }
}

impl Writable for u16 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for u64 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for i64 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

// TODO: handle longer strings?
impl Writable for str {
    fn size(&self) -> usize {
        std::mem::size_of::<u16>() + // Length
        self.len()
    }

    fn write(&self, buffer: &mut Vec<u8>) {
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
            Self::U64(value) => value.size(),
            Self::I64(value) => value.size(),
            Self::Str(value) => value.size(),
            _ => unimplemented!(),
        }
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        match self {
            Self::U64(value) => {
                value::TAG_U64.write(buffer);
                value.write(buffer);
            }
            Self::I64(value) => {
                value::TAG_I64.write(buffer);
                value.write(buffer);
            }
            Self::Str(value) => {
                value::TAG_STR.write(buffer);
                value.write(buffer);
            }
            _ => unimplemented!(),
        }
    }
}

impl Writable for Op {
    fn size(&self) -> usize {
        1
    }

    fn write(&self, buffer: &mut Vec<u8>) {
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

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("end of input")]
    EndOfInput,
    #[error("invalid input")]
    InvalidInput,
    #[error("utf8 error")]
    Utf8Error(#[from] std::str::Utf8Error),
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
            value::TAG_U64 => Ok(Value::U64(self.read()?)),
            value::TAG_I64 => Ok(Value::I64(self.read()?)),
            value::TAG_STR => Ok(Value::Str(self.read()?)),
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
