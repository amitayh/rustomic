use std::collections::HashMap;

use crate::clock::Clock;
use crate::datom;
use crate::query::*;
use crate::schema::*;
use crate::storage::Storage;
use crate::tx::*;

pub struct Db<S: Storage, C: Clock> {
    next_entity_id: u64,
    storage: S,
    clock: C,
}

impl<S: Storage, C: Clock> Db<S, C> {
    pub fn new(storage: S, clock: C) -> Self {
        Db {
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
        self.storage
            .save(&datoms)
            .map_err(|err| TransactionError::StorageError(err))?;

        Ok(TransctionResult {
            tx_data: datoms,
            temp_ids,
        })
    }

    pub fn query(&mut self, query: Query) -> Result<QueryResult, QueryError> {
        let mut wher = query.wher.clone();
        self.resolve_idents(&mut wher)?;
        let assignment = Assignment::empty(&query);
        let mut results = Vec::new();
        self.resolve(&mut wher, assignment, &mut results)?;
        Ok(QueryResult { results })
    }

    // --------------------------------------------------------------------------------------------

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
    ) -> Result<Vec<datom::Datom>, TransactionError> {
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

    fn operation_datoms(
        &mut self,
        tx: u64,
        operation: &Operation,
        temp_ids: &HashMap<String, u64>,
    ) -> Result<Vec<datom::Datom>, TransactionError> {
        let mut datoms = Vec::new();
        let entity = self.resolve_entity(&operation.entity, temp_ids)?;
        for AttributeValue { attribute, value } in &operation.attributes {
            let attribute_id = self
                .storage
                .resolve_ident(attribute)
                .map_err(|err| TransactionError::StorageError(err))?;

            datoms.push(datom::Datom::new(entity, attribute_id, value.clone(), tx));
        }
        Ok(datoms)
    }

    fn create_tx_datom(&mut self) -> datom::Datom {
        let tx = self.next_entity_id();
        datom::Datom::new(tx, DB_TX_TIME_ID, self.clock.now(), tx)
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

    fn next_entity_id(&mut self) -> u64 {
        self.next_entity_id += 1;
        self.next_entity_id
    }

    // --------------------------------------------------------------------------------------------

    fn resolve_idents(&self, wher: &mut Vec<Clause>) -> Result<(), QueryError> {
        for clause in wher {
            if let AttributePattern::Ident(ident) = &clause.attribute {
                let entity_id = self
                    .storage
                    .resolve_ident(ident)
                    .map_err(|err| QueryError::StorageError(err))?;
                clause.attribute = AttributePattern::Id(entity_id);
            }
        }
        Ok(())
    }

    fn resolve(
        &self,
        clauses: &mut [Clause],
        assignment: Assignment,
        results: &mut Vec<HashMap<String, datom::Value>>,
    ) -> Result<(), QueryError> {
        if assignment.is_complete() {
            results.push(assignment.assigned);
            return Ok(());
        }
        if let [clause, rest @ ..] = clauses {
            clause.substitute(&assignment);
            let datoms = self
                .storage
                .find_datoms(clause)
                .map_err(|err| QueryError::StorageError(err))?;

            // TODO can this be parallelized?
            for datom in datoms {
                self.resolve(rest, assignment.update_with(clause, datom), results)?;
            }
        }
        Ok(())
    }
}
