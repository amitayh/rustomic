use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use crate::clock::Clock;
use crate::datom::Datom;
use crate::datom::Op;
use crate::datom::Value;
use crate::query::AttributePattern;
use crate::query::Clause;
use crate::query::EntityPattern;
use crate::schema::*;
use crate::storage::Storage;
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
        let temp_ids = self.generate_temp_ids(&transaction)?;
        let datoms = self.transaction_datoms(&transaction, &temp_ids)?;
        let mut storage = self.storage.write().map_err(|_| TransactionError::Error)?;
        storage
            .save(&datoms)
            .map_err(|err| TransactionError::StorageError(err))?;

        Ok(TransctionResult {
            tx_data: datoms,
            temp_ids,
        })
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
    ) -> Result<Vec<Datom>, TransactionError> {
        let mut datoms = Vec::new();
        let tx = self.create_tx_datom();
        for operation in &transaction.operations {
            for datom in self.operation_datoms(tx.entity, operation, temp_ids)? {
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
        Datom::new(tx, DB_TX_TIME_ID, self.clock.now(), tx)
    }

    fn operation_datoms(
        &mut self,
        tx: u64,
        operation: &Operation,
        temp_ids: &HashMap<String, u64>,
    ) -> Result<Vec<Datom>, TransactionError> {
        let mut datoms = Vec::new();
        let entity = self.resolve_entity(&operation.entity, temp_ids)?;
        let storage = self.storage.read().map_err(|_| TransactionError::Error)?;
        for AttributeValue { attribute, value } in &operation.attributes {
            let attribute_id = storage
                .resolve_ident(attribute)
                .map_err(|err| TransactionError::StorageError(err))?;

            // TODO: retract previous value for `Cardinality::One`
            let attribute = storage
                .find_attribute(attribute_id)
                .map_err(|err| TransactionError::StorageError(err))?;

            if attribute.cardinality == Cardinality::One {
                let clause = Clause::new()
                    .with_entity(EntityPattern::Id(entity))
                    .with_attribute(AttributePattern::Id(attribute_id));
                let datoms2 = storage
                    .find_datoms(&clause)
                    .map_err(|err| TransactionError::StorageError(err))?;
                for datom in datoms2 {
                    datoms.push(Datom {
                        entity,
                        attribute: attribute_id,
                        value: datom.value.clone(),
                        tx,
                        op: Op::Retracted,
                    });
                }
            }

            let mut v = value.clone();
            if let Some(id) = value.as_str().and_then(|str| temp_ids.get(str)) {
                if attribute.value_type == ValueType::Ref {
                    v = Value::U64(*id);
                }
            };

            datoms.push(Datom::new(entity, attribute_id, v, tx));
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
}
