use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;

use crate::datom;
use crate::query;
use crate::schema;
use crate::tx;

pub struct InMemoryDb {
    next_entity_id: u64,
    ident_to_entity_id: HashMap<String, u64>,
    datoms: Vec<datom::Datom>,
    // https://docs.datomic.com/pro/query/indexes.html
    eavt: BTreeMap<u64, BTreeMap<u64, BTreeMap<datom::Value, u64>>>,
    aevt: BTreeMap<u64, BTreeMap<u64, BTreeMap<datom::Value, u64>>>,
}

impl InMemoryDb {
    pub fn new() -> InMemoryDb {
        let initial_tx_id = 10;
        InMemoryDb {
            next_entity_id: initial_tx_id,
            ident_to_entity_id: schema::default_ident_to_entity(),
            datoms: schema::default_datoms(initial_tx_id),
            eavt: BTreeMap::new(),
            aevt: BTreeMap::new(),
        }
    }

    fn ident_to_entity(&mut self, ident: &str, entity: u64) {
        self.ident_to_entity_id.insert(String::from(ident), entity);
    }

    pub fn query(&self, query: query::Query) -> query::QueryResult {
        let mut wher = query.wher.clone();
        self.resolve_idents(&mut wher);
        let assignment = query::Assignment::empty(&query);
        let results = self.resolve(&mut wher, assignment);
        query::QueryResult {
            results: results
                .into_iter()
                .map(|assignment| assignment.assigned)
                .collect(),
        }
    }

    fn resolve(
        &self,
        clauses: &mut [query::Clause],
        assignment: query::Assignment,
    ) -> Vec<query::Assignment> {
        if assignment.is_complete() {
            return vec![assignment.clone()];
        }
        match clauses {
            [] => vec![],
            [clause, rest @ ..] => {
                clause.substitute(&assignment);
                let mut result = Vec::new();
                for datom in self.find_matching_datoms(clause) {
                    let new_assignment = assignment.update_with(&clause, datom);
                    result.extend(self.resolve(rest, new_assignment));
                }
                result
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

    pub fn transact(
        &mut self,
        transaction: tx::Transaction,
    ) -> Result<tx::TransctionResult, tx::TransactionError> {
        // validate cardinality
        let tx = self.create_tx_datom();
        let temp_ids = self.generate_temp_ids(&transaction.operations);
        let mut datoms: Vec<datom::Datom> = transaction
            .operations
            .iter()
            .flat_map(|operation| {
                if let Some(entity_id) = self.get_entity_id(&operation.entity, &temp_ids) {
                    self.get_datoms(tx.entity, entity_id, &operation.attributes, &temp_ids)
                } else {
                    vec![]
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
        self.validate_transaction(&datoms)?;
        self.datoms.append(&mut datoms);

        Ok(tx::TransctionResult {
            tx_data: datoms,
            temp_ids,
        })
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
        datom::Datom::new(transaction_id, schema::DB_TX_TIME_ID, 0u64, transaction_id)
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

    fn generate_temp_ids(&mut self, operations: &Vec<tx::Operation>) -> HashMap<String, u64> {
        operations
            .iter()
            .filter_map(|operation| {
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
