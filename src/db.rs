use std::collections::HashMap;

use crate::clock::Clock;
use crate::datom;
use crate::query;
use crate::query::Query;
use crate::query::QueryError;
use crate::query::QueryResult;
use crate::schema;
use crate::storage::Storage;
use crate::tx;
use crate::tx::AttributeValue;
use crate::tx::Entity;
use crate::tx::Operation;
use crate::tx::Transaction;
use crate::tx::TransactionError;
use crate::tx::TransctionResult;

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
            .map_err(|error| TransactionError::StorageError(error))?;

        Ok(TransctionResult {
            tx_data: datoms,
            temp_ids,
        })
    }

    pub fn query(&mut self, query: Query) -> Result<QueryResult, QueryError> {
        Err(QueryError::Error)
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
            let attribute_id = self.resolve_ident(attribute)?;
            datoms.push(datom::Datom {
                entity,
                attribute: attribute_id,
                value: value.clone(),
                tx,
                op: datom::Op::Added,
            });
        }
        Ok(datoms)
    }

    fn create_tx_datom(&mut self) -> datom::Datom {
        let tx = self.next_entity_id();
        datom::Datom::new(tx, schema::DB_TX_TIME_ID, self.clock.now(), tx)
    }

    fn resolve_ident(&self, ident: &str) -> Result<u64, TransactionError> {
        self.storage
            .resolve_ident(ident)
            .map_err(|error| TransactionError::StorageError(error))
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
}

// ------------------------------------------------------------------------------------------------

pub struct InMemoryDb {
    next_entity_id: u64,
    ident_to_entity_id: HashMap<String, u64>,
    datoms: Vec<datom::Datom>,
}

impl InMemoryDb {
    pub fn new() -> Self {
        let initial_tx_id = 10;
        InMemoryDb {
            next_entity_id: initial_tx_id,
            ident_to_entity_id: schema::default_ident_to_entity(),
            datoms: schema::default_datoms(initial_tx_id),
        }
    }

    pub fn transact(
        &mut self,
        transaction: tx::Transaction,
    ) -> Result<tx::TransctionResult, tx::TransactionError> {
        // TODO: validate cardinality
        let temp_ids = self.generate_temp_ids(&transaction);
        let mut datoms = self.get_datoms2(&transaction, &temp_ids)?;
        self.validate_transaction(&datoms)?;
        self.datoms.append(&mut datoms);

        Ok(tx::TransctionResult {
            tx_data: datoms,
            temp_ids,
        })
    }

    pub fn query(&self, query: query::Query) -> query::QueryResult {
        let mut wher = query.wher.clone();
        self.resolve_idents(&mut wher);
        let assignment = query::Assignment::empty(&query);
        let mut results = Vec::new();
        self.resolve(&mut wher, assignment, &mut results);
        query::QueryResult { results }
    }

    fn ident_to_entity(&mut self, ident: &str, entity: u64) {
        self.ident_to_entity_id.insert(String::from(ident), entity);
    }

    fn resolve(
        &self,
        clauses: &mut [query::Clause],
        assignment: query::Assignment,
        results: &mut Vec<HashMap<String, datom::Value>>,
    ) {
        if assignment.is_complete() {
            results.push(assignment.assigned);
            return;
        }
        if let [clause, rest @ ..] = clauses {
            clause.substitute(&assignment);
            // TODO can this be parallelized?
            for datom in self.find_matching_datoms(clause) {
                self.resolve(rest, assignment.update_with(clause, datom), results);
            }
        }
    }

    // TODO: optimize with indexes
    fn find_matching_datoms(&self, clause: &query::Clause) -> Vec<&datom::Datom> {
        self.datoms
            .iter()
            .filter(|datom| datom.satisfies(clause))
            .collect()
    }

    fn resolve_idents(&self, wher: &mut Vec<query::Clause>) {
        for clause in wher {
            if let query::AttributePattern::Ident(ident) = &clause.attribute {
                let entity_id = self.ident_to_entity_id.get(ident).unwrap();
                clause.attribute = query::AttributePattern::Id(*entity_id);
            }
        }
    }

    fn validate_transaction(&self, datoms: &Vec<datom::Datom>) -> Result<(), tx::TransactionError> {
        for datom in datoms {
            match self.value_type_of_attribute(datom.attribute) {
                Some(value_type) => {
                    if !datom.value.matches_type(value_type) {
                        return Err(tx::TransactionError::Error);
                    }
                }
                None => return Err(tx::TransactionError::Error),
            }
        }
        Ok(())
    }

    fn value_type_of_attribute(&self, attribute: u64) -> Option<schema::ValueType> {
        self.datoms
            .iter()
            .find(|datom| datom.entity == attribute && datom.attribute == schema::DB_ATTR_TYPE_ID)
            .and_then(|datom| match datom.value {
                datom::Value::U8(value) => schema::ValueType::from(value),
                _ => None,
            })
    }

    fn get_entity_id(
        &mut self,
        entity: &tx::Entity,
        temp_ids: &HashMap<String, u64>,
    ) -> Option<u64> {
        match entity {
            tx::Entity::New => Some(self.get_next_entity_id()),
            tx::Entity::Id(id) => Some(*id),
            tx::Entity::TempId(temp_id) => temp_ids.get(temp_id).copied(),
        }
    }

    fn create_tx_datom(&mut self) -> datom::Datom {
        let transaction_id = self.get_next_entity_id();
        // TODO: use clock time
        datom::Datom::new(transaction_id, schema::DB_TX_TIME_ID, 0u64, transaction_id)
    }

    fn get_datoms2(
        &mut self,
        transaction: &tx::Transaction,
        temp_ids: &HashMap<String, u64>,
    ) -> Result<Vec<datom::Datom>, TransactionError> {
        let tx = self.create_tx_datom();
        let mut datoms: Vec<datom::Datom> = transaction
            .operations
            .iter()
            .flat_map(|operation| {
                if let Some(entity_id) = self.get_entity_id(&operation.entity, &temp_ids) {
                    self.get_datoms(tx.entity, entity_id, &operation.attributes, &temp_ids)
                } else {
                    // TODO: report invalid entity ID
                    Vec::new()
                }
            })
            .collect();
        datoms.push(tx);
        datoms.iter().for_each(|datom| {
            if let datom::Datom {
                entity,
                attribute: schema::DB_ATTR_IDENT_ID,
                value: datom::Value::Str(ident),
                tx: _,
                op: _,
            } = datom
            {
                self.ident_to_entity(&ident, *entity);
            }
        });
        Ok(datoms)
    }

    fn get_datoms(
        &self,
        transaction_id: u64,
        entity_id: u64,
        attributes: &Vec<tx::AttributeValue>,
        temp_ids: &HashMap<String, u64>,
    ) -> Vec<datom::Datom> {
        attributes
            .iter()
            .map(|attribute| datom::Datom {
                entity: entity_id,
                attribute: *self.ident_to_entity_id.get(&attribute.attribute).unwrap(),
                value: attribute.value.clone(),
                tx: transaction_id,
                op: datom::Op::Added,
            })
            .collect()
    }

    fn generate_temp_ids(&mut self, transaction: &tx::Transaction) -> HashMap<String, u64> {
        transaction
            .operations
            .iter()
            .filter_map(|operation| {
                // TODO: detect duplicate temp IDs
                if let tx::Entity::TempId(id) = &operation.entity {
                    Some((id.clone(), self.get_next_entity_id()))
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_next_entity_id(&mut self) -> u64 {
        self.next_entity_id += 1;
        self.next_entity_id
    }
}
