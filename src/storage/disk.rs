use crate::storage::*;

pub struct DiskStorage {
    db: rocksdb::DB
}

impl Storage for DiskStorage {
    fn save(&mut self, datoms: &[Datom]) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        for datom in datoms {
            batch.put(datom.entity.to_be_bytes(), b"foo");
        }
        self.db.write(batch).unwrap();
        Ok(())
    }

    fn find_datoms(&self, _clause: &Clause, _tx_range: u64) -> Result<Vec<Datom>, StorageError> {
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
