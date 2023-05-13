use crate::datom;

pub enum EntityIdentifier {
    Existing(u64),
    Temp(String),
}

pub enum Operation {
    Add {
        entity: EntityIdentifier,
        attribute: u64,
        value: datom::Value,
    },
    Retract {
        entity: u64,
        attribute: u64,
    },
}
pub struct Transaction {
    pub operations: Vec<Operation>,
}
