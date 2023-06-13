use crate::storage::*;

pub struct DiskStorage {}

impl Storage for DiskStorage {
    fn save(&mut self, _datoms: &[Datom]) -> Result<(), StorageError> {
        todo!()
    }

    fn find_datoms(&self, _clause: &Clause, _tx_range: u64) -> Result<Vec<Datom>, StorageError> {
        todo!()
    }

    fn resolve_ident(&self, _ident: &str) -> Result<EntityId, StorageError> {
        todo!()
    }
}
