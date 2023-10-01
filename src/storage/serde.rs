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

pub mod index {
    use super::*;
    use crate::query::pattern::{AttributePattern, EntityPattern, TxPattern, ValuePattern};

    pub const TAG_EAVT: u8 = 0x00;
    pub const TAG_AEVT: u8 = 0x01;
    pub const TAG_AVET: u8 = 0x02;

    pub fn key(clause: &Clause) -> Vec<u8> {
        match clause {
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
        }
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
            index::TAG_EAVT => eavt(&mut reader),
            index::TAG_AEVT => aevt(&mut reader),
            index::TAG_AVET => avet(&mut reader),
            _ => Err(ReadError::InvalidInput),
        }
    }

    fn eavt(reader: &mut Reader) -> ReadResult<Datom> {
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

    fn aevt(reader: &mut Reader) -> ReadResult<Datom> {
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

    fn avet(reader: &mut Reader) -> ReadResult<Datom> {
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

// -------------------------------------------------------------------------------------------------

pub trait Writable {
    fn size(&self) -> usize;
    fn write(&self, buffer: &mut Vec<u8>);
}

impl Writable for u8 {
    fn size(&self) -> usize {
        1
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self);
    }
}

impl Writable for u16 {
    fn size(&self) -> usize {
        2
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for u64 {
    fn size(&self) -> usize {
        4
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

impl Writable for i64 {
    fn size(&self) -> usize {
        4
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_be_bytes());
    }
}

// TODO: how to handle longer strings?
impl Writable for str {
    fn size(&self) -> usize {
        2 + // Length
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
        1 + // Value tag
        match self {
            Value::U64(_) | Value::I64(0) => 8,
            Value::Str(value) => value.size(),
            _ => 0,
        }
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        match self {
            Value::U64(value) => {
                value::TAG_U64.write(buffer);
                value.write(buffer);
            }
            Value::I64(value) => {
                value::TAG_I64.write(buffer);
                value.write(buffer);
            }
            Value::Str(value) => {
                value::TAG_STR.write(buffer);
                value.write(buffer);
            }
            _ => (),
        }
    }
}

impl Writable for Op {
    fn size(&self) -> usize {
        1
    }

    fn write(&self, buffer: &mut Vec<u8>) {
        match self {
            Op::Added => op::TAG_ADDED,
            Op::Retracted => op::TAG_RETRACTED,
        }
        .write(buffer)
    }
}

// -------------------------------------------------------------------------------------------------

pub struct Reader<'a>(&'a [u8]);

type ReadResult<T> = Result<T, ReadError>;

impl<'a> Reader<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Reader(buffer)
    }

    fn read_next(&mut self, num_bytes: usize) -> ReadResult<&[u8]> {
        let Reader(buffer) = self;
        if num_bytes > buffer.len() {
            return Err(ReadError::EndOfInput);
        }
        let result = &buffer[..num_bytes];
        self.0 = &buffer[num_bytes..];
        Ok(result)
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
