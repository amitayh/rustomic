#![allow(dead_code)]

use crate::query::pattern::EntityPattern;
use crate::schema::attribute;
use crate::storage::*;

pub struct DiskStorage {
    db: rocksdb::DB,
}

fn print_bytes(bytes: &[u8]) {
    let string = bytes
        .iter()
        .map(|byte| format!("{:02X}", byte))
        .collect::<Vec<String>>()
        .join(" ");
    println!("@@@ {}", string);
}

impl Storage for DiskStorage {
    fn save(&mut self, datoms: &[Datom]) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        for datom in datoms {
            batch.put(datom.encode_eavt(), "");
            batch.put(datom.encode_aevt(), "");
            batch.put(datom.encode_avet(), "");
        }
        self.db.write(batch).unwrap();
        Ok(())
    }

    fn find_datoms(&self, clause: &Clause, _tx_range: u64) -> Result<Vec<Datom>, StorageError> {
        let mut result = Vec::new();

        let mut read_options = rocksdb::ReadOptions::default();

        let mut lower = Vec::with_capacity(9);
        lower.push(index::TAG_EAVT);
        let mut upper = Vec::with_capacity(9);
        upper.push(index::TAG_EAVT);
        if let EntityPattern::Id(entity) = clause.entity {
            lower.extend_from_slice(&entity.to_be_bytes());
            upper.extend_from_slice(&(entity + 1).to_be_bytes());
        }
        read_options.set_iterate_lower_bound(lower);
        read_options.set_iterate_upper_bound(upper);

        for item in self
            .db
            .iterator_opt(rocksdb::IteratorMode::Start, read_options)
        {
            let (key, _value) = item.unwrap();
            //print_bytes(&key);
            //print_bytes(&value);
            let datom = Datom::parse(&key).unwrap();
            dbg!(&datom);
            result.push(datom);
        }
        Ok(result)
    }

    fn resolve_ident(&self, _ident: &str) -> Result<EntityId, StorageError> {
        todo!()
    }
}

impl DiskStorage {
    pub fn new(db: rocksdb::DB) -> Self {
        DiskStorage { db }
    }
}

mod index {
    pub const TAG_EAVT: u8 = 0x01;
    pub const TAG_AEVT: u8 = 0x02;
    pub const TAG_AVET: u8 = 0x03;
}

mod value {
    pub const TAG_U64: u8 = 0x01;
    pub const TAG_I64: u8 = 0x02;
    pub const TAG_STR: u8 = 0x03;
}

impl Value {
    fn size(&self) -> usize {
        1 + // Value tag
            match self {
            Value::U64(_) | Value::I64(_) => 8,
            Value::Str(str) => {
                2 + // String length
                str.len()
            },
            _ => 0,
        }
    }

    fn write_to(&self, writer: &mut serde::Writer) {
        match self {
            Value::U64(value) => {
                writer.write_u8(value::TAG_U64);
                writer.write_u64(*value);
            }
            Value::I64(value) => {
                writer.write_u8(value::TAG_I64);
                writer.write_i64(*value);
            }
            Value::Str(value) => {
                writer.write_u8(value::TAG_STR);
                writer.write_str(value);
            }
            _ => (),
        }
    }
}

mod op {
    pub const TAG_ADDED: u8 = 0x00;
    pub const TAG_RETRACTED: u8 = 0x01;
}

impl Op {
    fn encode(&self) -> u8 {
        match self {
            Op::Added => op::TAG_ADDED,
            Op::Retracted => op::TAG_RETRACTED,
        }
    }

    fn parse(buffer: &[u8]) -> Option<(Op, &[u8])> {
        match buffer.get(0) {
            Some(&op::TAG_ADDED) => Some((Op::Added, &buffer[1..])),
            Some(&op::TAG_RETRACTED) => Some((Op::Retracted, &buffer[1..])),
            _ => None,
        }
    }
}

fn parse_str_value(buffer: &[u8]) -> Option<(Value, &[u8])> {
    let (len, buffer) = parse_usize(buffer)?;
    if buffer.len() < len {
        return None;
    }
    let str = String::from_utf8_lossy(&buffer[..len]);
    Some((Value::Str(str.into_owned()), &buffer[len..]))
}

fn parse_usize(buffer: &[u8]) -> Option<(usize, &[u8])> {
    if buffer.len() < 2 {
        return None;
    }
    let value = u16::from_be_bytes([buffer[0], buffer[1]]);
    Some((value.into(), &buffer[2..]))
}

impl Datom {
    pub const TAG_EAVT: u8 = 0x01;
    pub const TAG_AEVT: u8 = 0x02;
    pub const TAG_AVET: u8 = 0x03;

    fn key_size(&self) -> usize {
        self.value.size() +
            1 + // Index tag
            8 + // Entity
            8 + // Attribute
            8 // Tx
    }

    fn encode_eavt(&self) -> serde::Buffer {
        let mut writer = serde::Writer::new(self.key_size());
        writer.write_u8(index::TAG_EAVT);
        writer.write_u64(self.entity);
        writer.write_u64(self.attribute);
        self.value.write_to(&mut writer);
        writer.write_u64(!self.tx); // Keep tx in descending order
        writer.write_u8(self.op.encode());
        writer.result()
    }

    fn encode_aevt(&self) -> serde::Buffer {
        let mut writer = serde::Writer::new(self.key_size());
        writer.write_u8(index::TAG_AEVT);
        writer.write_u64(self.attribute);
        writer.write_u64(self.entity);
        self.value.write_to(&mut writer);
        writer.write_u64(!self.tx); // Keep tx in descending order
        writer.write_u8(self.op.encode());
        writer.result()
    }

    fn encode_avet(&self) -> serde::Buffer {
        let mut writer = serde::Writer::new(self.key_size());
        writer.write_u8(index::TAG_AVET);
        writer.write_u64(self.attribute);
        self.value.write_to(&mut writer);
        writer.write_u64(self.entity);
        writer.write_u64(!self.tx); // Keep tx in descending order
        writer.write_u8(self.op.encode());
        writer.result()
    }

    fn parse(buffer: &[u8]) -> Result<Datom, serde::ReadError> {
        let mut reader = serde::Reader::new(buffer);
        match reader.read_u8()? {
            index::TAG_EAVT => parse_eavt(&mut reader),
            index::TAG_AEVT => Err(serde::ReadError::EndOfInput),
            index::TAG_AVET => Err(serde::ReadError::EndOfInput),
            _ => Err(serde::ReadError::EndOfInput),
        }
    }
}

fn parse_eavt(reader: &mut serde::Reader) -> Result<Datom, serde::ReadError> {
    let entity = reader.read_u64()?;
    let attribute = reader.read_u64()?;
    let value = parse_value(reader)?;
    let tx = !reader.read_u64()?;
    Ok(Datom::add(entity, attribute, value, tx))
}

fn parse_value(reader: &mut serde::Reader) -> Result<Value, serde::ReadError> {
    match reader.read_u8()? {
        value::TAG_U64 => Ok(Value::U64(reader.read_u64()?)),
        value::TAG_I64 => Ok(Value::I64(reader.read_i64()?)),
        value::TAG_STR => Ok(Value::Str(reader.read_str()?)),
        _ => Err(serde::ReadError::EndOfInput),
    }
}
