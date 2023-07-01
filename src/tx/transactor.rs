use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use crate::clock::Clock;
use crate::datom::*;
use crate::query::clause::Clause;
use crate::query::pattern::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::*;
use crate::tx::*;

pub struct Transactor<S: Storage, C: Clock> {
    next_entity_id: u64,
    storage: Arc<RwLock<S>>,
    clock: C,
}

impl<S: Storage, C: Clock> Transactor<S, C> {
    pub fn new(storage: Arc<RwLock<S>>, clock: C) -> Self {
        Transactor {
            next_entity_id: 100,
            storage,
            clock,
        }
    }

    pub fn transact(
        &mut self,
        transaction: Transaction,
    ) -> Result<TransctionResult, TransactionError> {
        // TODO: add reverse index for attribute of type `Ref`
        let last_tx = self.next_entity_id;
        let temp_ids = self.generate_temp_ids(&transaction)?;
        let datoms = self.transaction_datoms(&transaction, &temp_ids, last_tx)?;
        self.write_storage()?.save(&datoms)?;

        Ok(TransctionResult {
            tx_id: datoms[0].tx,
            tx_data: datoms,
            temp_ids,
        })
    }

    fn read_storage(&self) -> Result<RwLockReadGuard<S>, TransactionError> {
        self.storage.read().map_err(|_| TransactionError::Error)
    }

    fn write_storage(&self) -> Result<RwLockWriteGuard<S>, TransactionError> {
        self.storage.write().map_err(|_| TransactionError::Error)
    }

    fn generate_temp_ids(
        &mut self,
        transaction: &Transaction,
    ) -> Result<HashMap<String, u64>, TransactionError> {
        let mut temp_ids = HashMap::new();
        for operation in &transaction.operations {
            if let Entity::TempId(id) = &operation.entity {
                if temp_ids.contains_key(id) {
                    return Err(TransactionError::DuplicateTempId(id.clone()));
                }
                temp_ids.insert(id.clone(), self.next_entity_id());
            };
        }
        Ok(temp_ids)
    }

    fn transaction_datoms(
        &mut self,
        transaction: &Transaction,
        temp_ids: &HashMap<String, u64>,
        last_tx: u64,
    ) -> Result<Vec<Datom>, TransactionError> {
        let mut datoms = Vec::new();
        let tx = self.create_tx_datom();
        for operation in &transaction.operations {
            for datom in self.operation_datoms(tx.entity, operation, temp_ids, last_tx)? {
                datoms.push(datom);
            }
        }
        datoms.push(tx);
        Ok(datoms)
    }

    fn next_entity_id(&mut self) -> u64 {
        self.next_entity_id += 1;
        self.next_entity_id
    }

    fn create_tx_datom(&mut self) -> Datom {
        let tx = self.next_entity_id();
        Datom::add(tx, DB_TX_TIME_ID, self.clock.now(), tx)
    }

    fn operation_datoms(
        &mut self,
        tx: u64,
        operation: &Operation,
        temp_ids: &HashMap<String, u64>,
        last_tx: u64,
    ) -> Result<Vec<Datom>, TransactionError> {
        let mut datoms = Vec::new();
        let entity = self.resolve_entity(&operation.entity, temp_ids)?;
        let storage = self.read_storage()?;
        for AttributeValue { attribute, value } in &operation.attributes {
            let attribute_id = storage.resolve_ident(attribute)?;

            let (cardinality, value_type) = self.attribute_metadata(attribute_id, last_tx)?;
            if cardinality == Cardinality::One {
                // Retract previous values
                let clause = Clause::new()
                    .with_entity(EntityPattern::Id(entity))
                    .with_attribute(AttributePattern::Id(attribute_id));
                let datoms2 = storage.find_datoms(&clause, last_tx)?;
                for datom in datoms2 {
                    datoms.push(Datom::retract(entity, attribute_id, datom.value, tx));
                }
            }

            let mut v = value.clone();
            if let Some(id) = value.as_str().and_then(|str| temp_ids.get(str)) {
                if value_type == ValueType::Ref {
                    v = Value::U64(*id);
                }
            };

            if !v.matches_type(value_type) {
                return Err(TransactionError::InvalidAttributeType);
            }

            datoms.push(Datom::add(entity, attribute_id, v, tx));
        }
        Ok(datoms)
    }

    fn resolve_entity(
        &mut self,
        entity: &Entity,
        temp_ids: &HashMap<String, u64>,
    ) -> Result<u64, TransactionError> {
        match entity {
            Entity::New => Ok(self.next_entity_id()),
            Entity::Id(id) => Ok(*id),
            Entity::TempId(temp_id) => temp_ids
                .get(temp_id)
                .copied()
                .ok_or_else(|| TransactionError::TempIdNotFound(temp_id.clone())),
        }
    }

    fn attribute_metadata(
        &self,
        attribute: u64,
        last_tx: u64,
    ) -> Result<(Cardinality, ValueType), TransactionError> {
        let clause = Clause::new().with_entity(EntityPattern::Id(attribute));
        let datoms = self.read_storage()?.find_datoms(&clause, last_tx)?;
        let mut cardinality = None;
        let mut value_type = None;
        for datom in datoms {
            match datom.attribute {
                DB_ATTR_TYPE_ID => {
                    value_type = datom
                        .value
                        .as_u64()
                        .and_then(|value| ValueType::from(*value));
                }
                DB_ATTR_CARDINALITY_ID => {
                    cardinality = datom
                        .value
                        .as_u64()
                        .and_then(|value| Cardinality::from(*value));
                }
                _ => {}
            }
        }
        match (cardinality, value_type) {
            (Some(cardinality), Some(value_type)) => Ok((cardinality, value_type)),
            _ => Err(TransactionError::Error),
        }
    }
}
