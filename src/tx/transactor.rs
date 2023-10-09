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
use crate::tx::attribute_resolver::*;
use crate::tx::*;

type TempIds = HashMap<Rc<str>, u64>;

pub struct Transactor<S: Storage, C: Clock> {
    next_entity_id: u64,
    storage: Arc<RwLock<S>>,
    clock: C,
    attribute_resolver: CachingAttributeResolver<StorageAttributeResolver<S>>,
}

impl<S: Storage, C: Clock> Transactor<S, C> {
    pub fn new(storage: Arc<RwLock<S>>, clock: C) -> Self {
        let attribute_resolver =
            CachingAttributeResolver::new(StorageAttributeResolver::new(storage.clone()));
        Transactor {
            next_entity_id: 100,
            storage,
            clock,
            attribute_resolver,
        }
    }

    pub fn transact(
        &mut self,
        transaction: Transaction,
    ) -> Result<TransctionResult, TransactionError> {
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
    ) -> Result<TempIds, TransactionError> {
        let mut temp_ids = HashMap::new();
        for operation in &transaction.operations {
            if let Entity::TempId(id) = &operation.entity {
                if temp_ids.insert(id.clone(), self.next_entity_id()).is_some() {
                    return Err(TransactionError::DuplicateTempId(id.clone()));
                }
            };
        }
        Ok(temp_ids)
    }

    fn transaction_datoms(
        &mut self,
        transaction: &Transaction,
        temp_ids: &TempIds,
        last_tx: u64,
    ) -> Result<Vec<Datom>, TransactionError> {
        let mut datoms = Vec::new();
        let tx = self.create_tx_datom();
        for operation in &transaction.operations {
            datoms.append(&mut self.operation_datoms(tx.entity, operation, temp_ids, last_tx)?);
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
        temp_ids: &TempIds,
        last_tx: u64,
    ) -> Result<Vec<Datom>, TransactionError> {
        let mut datoms = Vec::new();
        let entity = self.resolve_entity(&operation.entity, temp_ids)?;
        //let attribute_resolver = &mut self.attribute_resolver;
        let mut retract_attributes = Vec::new();
        for AttributeValue { attribute: ident, value } in &operation.attributes {
            let attribute = self.attribute_resolver.resolve(ident).unwrap();

            if attribute.cardinality == Cardinality::One {
                retract_attributes.push(attribute.id);
            }

            let mut v = value.clone();
            if let Some(id) = value.as_str().and_then(|str| temp_ids.get(str)) {
                if attribute.value_type == ValueType::Ref {
                    v = Value::U64(*id);
                }
            };

            if !v.matches_type(attribute.value_type) {
                return Err(TransactionError::InvalidAttributeType);
            }

            datoms.push(Datom::add(entity, attribute.id, v, tx));
        }

        for attribute_id in retract_attributes {
            let mut retracted = self.retract_old_values(entity, attribute_id, last_tx, tx)?;
            datoms.append(&mut retracted);
        }

        Ok(datoms)
    }

    fn retract_old_values(
        &self,
        entity: u64,
        attribute: u64,
        last_tx: u64,
        tx: u64,
    ) -> Result<Vec<Datom>, TransactionError> {
        let storage = self.read_storage()?;
        let mut datoms = Vec::new();
        // Retract previous values
        let clause = Clause::new()
            .with_entity(EntityPattern::Id(entity))
            .with_attribute(AttributePattern::Id(attribute));
        for datom in storage.find_datoms(&clause, last_tx)? {
            datoms.push(Datom::retract(entity, attribute, datom.value, tx));
        }
        Ok(datoms)
    }

    fn resolve_entity(
        &mut self,
        entity: &Entity,
        temp_ids: &TempIds,
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
