use std::rc::Rc;
use thiserror::Error;

use crate::datom::*;
use crate::storage::*;

pub mod index {
    use crate::query::pattern::{AttributePattern, EntityPattern, TxPattern, ValuePattern};

    use super::*;

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
            } => {
                let size = 1 + 8 + 8 + 8 + value::size(value);
                let mut writer = Writer::new(size);
                writer.write_u8(TAG_EAVT);
                writer.write_u64(*entity);
                writer.write_u64(*attribute);
                value::serialize(value, &mut writer);
                writer.write_u64(*tx);
                writer.result()
            }
            Clause {
                entity: EntityPattern::Id(entity),
                attribute: AttributePattern::Id(attribute),
                value: ValuePattern::Constant(value),
                tx: _,
            } => {
                let size = 1 + 8 + 8 + value::size(value);
                let mut writer = Writer::new(size);
                writer.write_u8(TAG_EAVT);
                writer.write_u64(*entity);
                writer.write_u64(*attribute);
                value::serialize(value, &mut writer);
                writer.result()
            }
            Clause {
                entity: EntityPattern::Id(entity),
                attribute: AttributePattern::Id(attribute),
                value: _,
                tx: _,
            } => {
                let size = 1 + 8 + 8;
                let mut writer = Writer::new(size);
                writer.write_u8(TAG_EAVT);
                writer.write_u64(*entity);
                writer.write_u64(*attribute);
                writer.result()
            }
            Clause {
                entity: EntityPattern::Id(entity),
                attribute: _,
                value: _,
                tx: _,
            } => {
                let size = 1 + 8;
                let mut writer = Writer::new(size);
                writer.write_u8(TAG_EAVT);
                writer.write_u64(*entity);
                writer.result()
            }
            Clause {
                entity: _,
                attribute: AttributePattern::Id(attribute),
                value: ValuePattern::Constant(value),
                tx: _,
            } => {
                let size = 1 + 8 + value::size(value);
                let mut writer = Writer::new(size);
                writer.write_u8(TAG_AVET);
                writer.write_u64(*attribute);
                value::serialize(value, &mut writer);
                writer.result()
            }
            Clause {
                entity: _,
                attribute: AttributePattern::Id(attribute),
                value: _,
                tx: _,
            } => {
                let size = 1 + 8;
                let mut writer = Writer::new(size);
                writer.write_u8(TAG_AEVT);
                writer.write_u64(*attribute);
                writer.result()
            }
            _ => {
                let size = 1;
                let mut writer = Writer::new(size);
                writer.write_u8(TAG_EAVT);
                writer.result()
            }
        }
    }
}

mod value {
    use super::*;

    pub const TAG_U64: u8 = 0x00;
    pub const TAG_I64: u8 = 0x01;
    pub const TAG_STR: u8 = 0x02;

    pub fn size(value: &Value) -> usize {
        1 + // Value tag
        match value {
            Value::U64(_) | Value::I64(_) => 8,
            Value::Str(str) => {
                2 + // String length
                str.len()
            },
            _ => 0,
        }
    }

    pub fn serialize(value: &Value, writer: &mut Writer) {
        match value {
            Value::U64(value) => {
                writer.write_u8(TAG_U64);
                writer.write_u64(*value);
            }
            Value::I64(value) => {
                writer.write_u8(TAG_I64);
                writer.write_i64(*value);
            }
            Value::Str(value) => {
                writer.write_u8(TAG_STR);
                writer.write_str(value);
            }
            _ => (),
        }
    }

    pub fn deserialize(reader: &mut Reader) -> ReadResult<Value> {
        match reader.read_u8()? {
            TAG_U64 => Ok(Value::U64(reader.read_u64()?)),
            TAG_I64 => Ok(Value::I64(reader.read_i64()?)),
            TAG_STR => Ok(Value::Str(reader.read_str()?)),
            _ => Err(ReadError::InvalidInput),
        }
    }
}

mod op {
    use super::*;

    const TAG_ADDED: u8 = 0x00;
    const TAG_RETRACTED: u8 = 0x01;

    pub fn serialize(op: Op, writer: &mut Writer) {
        writer.write_u8(match op {
            Op::Added => TAG_ADDED,
            Op::Retracted => TAG_RETRACTED,
        })
    }

    pub fn deserialize(reader: &mut Reader) -> ReadResult<Op> {
        match reader.read_u8()? {
            TAG_ADDED => Ok(Op::Added),
            TAG_RETRACTED => Ok(Op::Retracted),
            _ => Err(ReadError::InvalidInput),
        }
    }
}

pub mod datom {
    use super::*;

    pub fn size(datom: &Datom) -> usize {
        value::size(&datom.value) +
        1 + // Index tag
        8 + // Entity
        8 + // Attribute
        8 + // Tx
        1 // Op
    }

    pub mod serialize {
        use super::*;

        pub fn eavt(datom: &Datom) -> Vec<u8> {
            let mut writer = Writer::new(datom::size(datom));
            writer.write_u8(index::TAG_EAVT);
            writer.write_u64(datom.entity);
            writer.write_u64(datom.attribute);
            value::serialize(&datom.value, &mut writer);
            writer.write_u64(!datom.tx); // Keep tx in descending order
            op::serialize(datom.op, &mut writer);
            writer.result()
        }

        pub fn aevt(datom: &Datom) -> Vec<u8> {
            let mut writer = Writer::new(datom::size(datom));
            writer.write_u8(index::TAG_AEVT);
            writer.write_u64(datom.attribute);
            writer.write_u64(datom.entity);
            value::serialize(&datom.value, &mut writer);
            writer.write_u64(!datom.tx); // Keep tx in descending order
            op::serialize(datom.op, &mut writer);
            writer.result()
        }

        pub fn avet(datom: &Datom) -> Vec<u8> {
            let mut writer = Writer::new(datom::size(datom));
            writer.write_u8(index::TAG_AVET);
            writer.write_u64(datom.attribute);
            value::serialize(&datom.value, &mut writer);
            writer.write_u64(datom.entity);
            writer.write_u64(!datom.tx); // Keep tx in descending order
            op::serialize(datom.op, &mut writer);
            writer.result()
        }
    }

    mod deserialize {
        use super::*;

        pub fn eavt(reader: &mut Reader) -> ReadResult<Datom> {
            let entity = reader.read_u64()?;
            let attribute = reader.read_u64()?;
            let value = value::deserialize(reader)?;
            let tx = !reader.read_u64()?;
            let op = op::deserialize(reader)?;
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }

        pub fn aevt(reader: &mut Reader) -> ReadResult<Datom> {
            let attribute = reader.read_u64()?;
            let entity = reader.read_u64()?;
            let value = value::deserialize(reader)?;
            let tx = !reader.read_u64()?;
            let op = op::deserialize(reader)?;
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }

        pub fn avet(reader: &mut Reader) -> ReadResult<Datom> {
            let attribute = reader.read_u64()?;
            let value = value::deserialize(reader)?;
            let entity = reader.read_u64()?;
            let tx = !reader.read_u64()?;
            let op = op::deserialize(reader)?;
            Ok(Datom {
                entity,
                attribute,
                value,
                tx,
                op,
            })
        }
    }

    pub fn deserialize(buffer: &[u8]) -> ReadResult<Datom> {
        let mut reader = Reader::new(buffer);
        match reader.read_u8()? {
            index::TAG_EAVT => deserialize::eavt(&mut reader),
            index::TAG_AEVT => deserialize::aevt(&mut reader),
            index::TAG_AVET => deserialize::avet(&mut reader),
            _ => Err(ReadError::InvalidInput),
        }
    }
}

// -------------------------------------------------------------------------------------------------

pub struct Writer {
    buffer: Vec<u8>,
}

impl Writer {
    pub fn new(capacity: usize) -> Self {
        Writer {
            buffer: Vec::with_capacity(capacity),
        }
    }

    pub fn write_u8(&mut self, value: u8) {
        self.buffer.push(value);
    }

    pub fn write_u16(&mut self, value: u16) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u64(&mut self, value: u64) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_i64(&mut self, value: i64) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_str(&mut self, value: &str) {
        match u16::try_from(value.len()) {
            Ok(length) => {
                self.write_u16(length);
                self.buffer.extend_from_slice(value.as_bytes());
            }
            _ => (),
        }
    }

    pub fn result(self) -> Vec<u8> {
        self.buffer
    }
}

// -------------------------------------------------------------------------------------------------

pub struct Reader<'a>(&'a [u8]);

type ReadResult<T> = Result<T, ReadError>;

impl<'a> Reader<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Reader(buffer)
    }

    pub fn read_u8(&mut self) -> ReadResult<u8> {
        let buffer = self.read_next(1)?;
        Ok(buffer[0])
    }

    pub fn read_u16(&mut self) -> ReadResult<u16> {
        let buffer = self.read_next(2)?;
        Ok(u16::from_be_bytes([buffer[0], buffer[1]]))
    }

    pub fn read_u64(&mut self) -> ReadResult<u64> {
        let buffer = self.read_next(8)?;
        Ok(u64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]))
    }

    pub fn read_i64(&mut self) -> ReadResult<i64> {
        let buffer = self.read_next(8)?;
        Ok(i64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]))
    }

    pub fn read_str(&mut self) -> ReadResult<Rc<str>> {
        let length = self.read_u16()?;
        let buffer = self.read_next(length.into())?;
        let str = std::str::from_utf8(buffer)?;
        Ok(Rc::from(str))
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
