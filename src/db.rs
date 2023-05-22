use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::datom;
use crate::query;
use crate::schema;
use crate::tx;

#[derive(Debug)]
struct Assignment {
    variables: HashMap<String, Option<datom::Value>>,
    assigned: usize,
}

impl Assignment {
    fn empty(query: &query::Query) -> Assignment {
        let mut variables = HashMap::new();
        for clause in &query.wher {
            for variable in clause.free_variables() {
                variables.insert(variable, None);
            }
        }
        Assignment {
            variables,
            assigned: 0,
        }
    }

    fn is_satisfied(&self) -> bool {
        self.variables.len() == self.assigned
    }

    fn assign(&mut self, variable: &str, value: datom::Value) {
        self.variables.insert(String::from(variable), Some(value));
        self.assigned += 1;
    }
}

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
        let mut db = InMemoryDb {
            next_entity_id: 10,
            ident_to_entity_id: HashMap::new(),
            datoms: vec![
                // "db/attr/ident" attribute
                // TODO: unique?
                datom::Datom::new(1, 1, schema::DB_ATTR_IDENT, 6),
                datom::Datom::new(1, 2, "Human readable name of attribute", 6),
                datom::Datom::new(1, 3, schema::ValueType::Str as u8, 6),
                datom::Datom::new(1, 4, schema::Cardinality::One as u8, 6),
                // "db/attr/doc" attribute
                datom::Datom::new(2, 1, schema::DB_ATTR_DOC, 6),
                datom::Datom::new(2, 2, "Documentation of attribute", 6),
                datom::Datom::new(2, 3, schema::ValueType::Str as u8, 6),
                datom::Datom::new(2, 4, schema::Cardinality::One as u8, 6),
                // "db/attr/type" attribute
                datom::Datom::new(3, 1, schema::DB_ATTR_TYPE, 6),
                datom::Datom::new(3, 2, "Data type of attribute", 6),
                datom::Datom::new(3, 3, schema::ValueType::U8 as u8, 6),
                datom::Datom::new(3, 4, schema::Cardinality::One as u8, 6),
                // "db/attr/cardinality" attribute
                datom::Datom::new(4, 1, schema::DB_ATTR_CARDINALITY, 6),
                datom::Datom::new(4, 2, "Cardinality of attribyte", 6),
                datom::Datom::new(4, 3, schema::ValueType::U8 as u8, 6),
                datom::Datom::new(4, 4, schema::Cardinality::One as u8, 6),
                // "db/tx/time" attribute
                datom::Datom::new(5, 1, schema::DB_TX_TIME, 6),
                datom::Datom::new(5, 2, "Transaction's wall clock time", 6),
                datom::Datom::new(5, 3, schema::ValueType::U64 as u8, 6),
                datom::Datom::new(5, 4, schema::Cardinality::One as u8, 6),
                // first transaction
                datom::Datom::new(6, 5, 0u64, 6),
            ],
            eavt: BTreeMap::new(),
            aevt: BTreeMap::new(),
        };
        db.ident_to_entity(schema::DB_ATTR_IDENT, 1);
        db.ident_to_entity(schema::DB_ATTR_DOC, 2);
        db.ident_to_entity(schema::DB_ATTR_TYPE, 3);
        db.ident_to_entity(schema::DB_ATTR_CARDINALITY, 4);
        db.ident_to_entity(schema::DB_TX_TIME, 5);
        db
    }

    fn ident_to_entity(&mut self, ident: &str, entity: u64) {
        self.ident_to_entity_id.insert(String::from(ident), entity);
    }

    pub fn query(&self, query: query::Query) -> query::QueryResult {
        let mut wher = query.wher.clone();
        self.resolve_idents(&mut wher);
        wher.sort_by_key(|clause| clause.num_grounded_terms());
        wher.reverse();

        let empty = Assignment::empty(&query);

        for clause in &wher {
            for datom in self.find_matching_datoms(clause) {
                // let assignment = Assignment::
            }
        }

        let matching_datoms = wher
            .iter()
            .map(|clause| self.find_matching_datoms(&clause))
            .reduce(|a, b| a.intersection(&b).cloned().collect());

        println!("@@@ matching_datoms {:?}", matching_datoms);

        query::QueryResult {
            results: vec![vec![datom::Value::U64(0)]],
        }
    }

    // TODO: optimize with indexes
    fn find_matching_datoms(&self, clause: &query::Clause) -> HashSet<&datom::Datom> {
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

    pub fn transact(&mut self, transaction: tx::Transaction) -> tx::TransctionResult {
        // validate attributes match value
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
                attribute: 1, // "db/attr/ident" attribute
                value: datom::Value::Str(ident),
                tx: _,
                op: _,
            } = datom
            {
                self.ident_to_entity(&ident, *entity);
            }
        });
        self.datoms.append(&mut datoms);
        tx::TransctionResult {
            tx_data: datoms,
            temp_ids,
        }
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
        datom::Datom {
            entity: transaction_id,
            attribute: *self.ident_to_entity_id.get(schema::DB_TX_TIME).unwrap(),
            value: datom::Value::U64(0),
            tx: transaction_id,
            op: datom::Op::Added,
        }
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
        let entity_id = self.next_entity_id;
        self.next_entity_id += 1;
        entity_id
    }
}
