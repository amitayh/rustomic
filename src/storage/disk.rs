use protobuf::Message;
use protobuf::MessageField;

use crate::storage::*;

pub struct DiskStorage {
    db: rocksdb::DB,
}

impl Storage for DiskStorage {
    fn save(&mut self, datoms: &[Datom]) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        for datom in datoms {
            let bytes = datom.to_eavt_proto().write_to_bytes().unwrap();
            batch.put(bytes, b"hello");
        }
        self.db.write(batch).unwrap();
        Ok(())
    }

    fn find_datoms(&self, _clause: &Clause, _tx_range: u64) -> Result<Vec<Datom>, StorageError> {
        //let mut read_options = rocksdb::ReadOptions::default();
        for item in self.db.iterator(rocksdb::IteratorMode::Start) {
            let datom = proto::datom::Eavt::parse_from_bytes(&item.unwrap().0).unwrap();
            dbg!(&datom);
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

impl Datom {
    fn to_eavt_proto(&self) -> proto::datom::Eavt {
        let mut eavt = proto::datom::Eavt::new();
        eavt.index = proto::datom::Index::INDEX_EAVT.into();
        eavt.entity = self.entity;
        eavt.attribute = self.attribute;
        eavt.value = self.value.to_proto();
        eavt.tx = self.tx;
        eavt
    }
}

impl Value {
    fn to_proto(&self) -> MessageField<proto::datom::Value> {
        let mut value = proto::datom::Value::new();
        match self {
            Value::I64(v) => value.set_int64_value(*v),
            Value::U64(v) => value.set_uint64_value(*v),
            Value::Str(v) => value.set_string_value(v.clone()),
            _ => (),
        }
        if value.value.is_some() {
            MessageField::some(value)
        } else {
            MessageField::none()
        }
    }
}
