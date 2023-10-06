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
pub trait Storage<'a> {
    type Error;

    type Iter: Iterator<Item = &'a Datom>;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;

    fn find(&'a self, clause: &Clause) -> Result<Self::Iter, Self::Error>;

    //fn resolve_ident(&self, ident: &str) -> Result<EntityId, StorageError>;

    //fn find_datoms(&self, clause: &Clause, tx_range: u64) -> Result<Vec<Datom>, StorageError>;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("ident `{0}` not found")]
    IdentNotFound(String),
}
