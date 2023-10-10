pub mod disk;
pub mod memory;
pub mod serde;

use crate::datom::*;
use crate::query::clause::Clause;
use thiserror::Error;

// TODO: create structs?
type EntityId = u64;
type AttributeId = u64;
type TransactionId = u64;

// TODO: separate read / write?
pub trait Storage {
    type Error: std::error::Error;
    //type Iter: Iterator<Item = Datom>;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;

    fn find_datoms(&self, clause: &Clause, tx_range: u64) -> Result<Vec<Datom>, Self::Error>;

    fn find(&self, clause: &Clause) -> Result<Vec<Datom>, Self::Error> {
        self.find_datoms(clause, u64::MAX)
    }
    //fn find_datoms(&self, clause: &Clause) -> Result<Self::Iter, StorageError>;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("ident `{0}` not found")]
    IdentNotFound(String),
}
