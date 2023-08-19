use crate::query::pattern::EntityPattern;
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
            batch.put(datom.encode_eavt(), b"\x00");
            batch.put(datom.encode_aevt(), b"\x00");
            batch.put(datom.encode_avet(), b"\x00");
        }
        self.db.write(batch).unwrap();
        Ok(())
    }

    fn find_datoms(&self, clause: &Clause, _tx_range: u64) -> Result<Vec<Datom>, StorageError> {
        let mut read_options = rocksdb::ReadOptions::default();

        let mut lower = Vec::with_capacity(9);
        lower.push(index::TAG_EAVT);
        if let EntityPattern::Id(entity) = clause.entity {
            lower.extend_from_slice(&entity.to_be_bytes());
        }
        read_options.set_iterate_lower_bound(lower);
        //read_options.set_iterate_upper_bound(upper.write_to_bytes().unwrap());

        for item in self
            .db
            .iterator_opt(rocksdb::IteratorMode::Start, read_options)
        {
            let (key, value) = item.unwrap();
            print_bytes(&key);
            print_bytes(&value);
            let datom = Datom::parse(&key);
            dbg!(datom);
            println!("@@@ -");
            //match parsed_key.index {
            //    Some(proto::datom::key::Index::Eavt(eavt)) => {
            //        println!("@@@ EAVT {} {} {}", eavt.entity, eavt.attribute, eavt.tx);
            //    }
            //    Some(proto::datom::key::Index::Aevt(aevt)) => {
            //        println!("@@@ AEVT {} {} {}", aevt.attribute, aevt.entity, aevt.tx);
            //    }
            //    Some(proto::datom::key::Index::Avet(avet)) => {
            //        println!("@@@ AVET {} {} {}", avet.attribute, avet.entity, avet.tx);
            //    }
            //    None => (),
            //}
            //dbg!(&datom);
        }
        Ok(vec![])
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

    fn write_to(&self, buf: &mut Vec<u8>) {
        match self {
            Value::U64(value) => {
                buf.push(value::TAG_U64);
                buf.extend_from_slice(&value.to_be_bytes());
            }
            Value::I64(value) => {
                buf.push(value::TAG_I64);
                buf.extend_from_slice(&value.to_be_bytes());
            }
            Value::Str(value) => {
                buf.push(value::TAG_STR);
                let len = u16::try_from(value.len()).unwrap();
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(value.as_bytes());
            }
            _ => (),
        }
    }

    fn parse(buffer: &[u8]) -> Option<(Value, &[u8])> {
        if buffer.len() < 2 {
            return None;
        }
        match buffer[0] {
            value::TAG_U64 => {
                let (value, buffer) = parse_u64(&buffer[1..])?;
                Some((Value::U64(value), buffer))
            }
            value::TAG_I64 => {
                let (value, buffer) = parse_i64(&buffer[1..])?;
                Some((Value::I64(value), buffer))
            }
            value::TAG_STR => parse_str_value(&buffer[1..]),
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

    fn encode_eavt(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.key_size());
        bytes.push(index::TAG_EAVT);
        bytes.extend_from_slice(&self.entity.to_be_bytes());
        bytes.extend_from_slice(&self.attribute.to_be_bytes());
        self.value.write_to(&mut bytes);
        bytes.extend_from_slice(&self.tx.to_be_bytes());
        bytes
    }

    fn encode_aevt(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.key_size());
        bytes.push(index::TAG_AEVT);
        bytes.extend_from_slice(&self.attribute.to_be_bytes());
        bytes.extend_from_slice(&self.entity.to_be_bytes());
        self.value.write_to(&mut bytes);
        bytes.extend_from_slice(&self.tx.to_be_bytes());
        bytes
    }

    fn encode_avet(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.key_size());
        bytes.push(index::TAG_AVET);
        bytes.extend_from_slice(&self.attribute.to_be_bytes());
        self.value.write_to(&mut bytes);
        bytes.extend_from_slice(&self.entity.to_be_bytes());
        bytes.extend_from_slice(&self.tx.to_be_bytes());
        bytes
    }

    fn parse(buffer: &[u8]) -> Option<Datom> {
        if buffer.len() < 2 {
            return None;
        }
        match buffer[0] {
            index::TAG_EAVT => parse_eavt(&buffer[1..]),
            index::TAG_AEVT => None,
            index::TAG_AVET => None,
            _ => None,
        }
    }
}

fn parse_eavt(buffer: &[u8]) -> Option<Datom> {
    let (entity, buffer) = parse_u64(buffer)?;
    let (attribute, buffer) = parse_u64(buffer)?;
    let (value, buffer) = Value::parse(buffer)?;
    let (tx, _) = parse_u64(buffer)?;
    Some(Datom::add(entity, attribute, value, tx))
}

fn parse_u64(buffer: &[u8]) -> Option<(u64, &[u8])> {
    if buffer.len() < 8 {
        return None;
    }
    let value = u64::from_be_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
    ]);
    Some((value, &buffer[8..]))
}

fn parse_i64(buffer: &[u8]) -> Option<(i64, &[u8])> {
    if buffer.len() < 8 {
        return None;
    }
    let value = i64::from_be_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
    ]);
    Some((value, &buffer[8..]))
}
