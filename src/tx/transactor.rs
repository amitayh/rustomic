use std::collections::HashMap;
use std::collections::HashSet;
use std::u64;

use crate::clock::Instant;
use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::attribute_resolver::*;
use crate::storage::restricts::*;
use crate::storage::*;
use crate::tx::*;

pub fn transact<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    attribute_resolver: &mut AttributeResolver,
    now: Instant,
    transaction: Transaction,
) -> Result<TransctionResult, S::Error> {
    let next_id = storage.latest_entity_id()? + 1;
    let builder = ResultBuilder::from(&transaction, next_id)?;
    builder.build(storage, attribute_resolver, now, transaction)
}

struct ResultBuilder {
    tx_id: u64,
    next_id: u64,
    datoms: Vec<Datom>,
    temp_ids: HashMap<Rc<str>, u64>,
    unique_values: HashSet<(u64, Value)>,
}

impl ResultBuilder {
    pub fn from<E>(transaction: &Transaction, mut next_id: u64) -> Result<Self, E> {
        let tx_id = next_id;
        let mut temp_ids = HashMap::new();
        for operation in &transaction.operations {
            if let OperatedEntity::TempId(temp_id) = &operation.entity {
                next_id += 1;
                if temp_ids.insert(Rc::clone(temp_id), next_id).is_some() {
                    return Err(TransactionError::DuplicateTempId(Rc::clone(temp_id)));
                }
            };
        }
        Ok(Self {
            tx_id,
            next_id,
            temp_ids,
            datoms: Vec::with_capacity(transaction.total_attribute_operations()),
            unique_values: HashSet::new(),
        })
    }

    pub fn build<'a, S: ReadStorage<'a>>(
        mut self,
        storage: &'a S,
        attribute_resolver: &mut AttributeResolver,
        now: Instant,
        transaction: Transaction,
    ) -> Result<TransctionResult, S::Error> {
        for operation in transaction.operations {
            self.fill_datoms(storage, attribute_resolver, operation)?;
        }
        self.push(Datom::add(self.tx_id, DB_TX_TIME_ID, now.0, self.tx_id));
        Ok(TransctionResult {
            tx_id: self.tx_id,
            tx_data: self.datoms,
            temp_ids: self.temp_ids,
        })
    }

    fn fill_datoms<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        attribute_resolver: &mut AttributeResolver,
        operation: EntityOperation,
    ) -> Result<(), S::Error> {
        let entity = self.resolve_entity(operation.entity)?;
        let mut retract_attributes = HashSet::with_capacity(operation.attributes.len());
        for attribute_value in operation.attributes {
            let attribute =
                attribute_resolver.resolve(storage, &attribute_value.attribute, self.tx_id)?;

            if attribute.definition.cardinality == Cardinality::One {
                // Values of attributes with cardinality `Cardinality::One` should be retracted
                // before asserting new values.
                retract_attributes.insert(attribute.id);
            }

            let value = self.resolve_value(attribute_value.value)?;
            verify_type(attribute, &value)?;
            if attribute.definition.unique {
                self.verify_uniqueness_tx(attribute, &value)?;
                self.verify_uniqueness_db(attribute, &value, storage)?;
            }

            self.push(Datom {
                entity,
                attribute: attribute.id,
                value,
                tx: self.tx_id,
                op: attribute_value.op,
            });
        }

        for attribute_id in retract_attributes {
            self.retract_old_values(storage, entity, attribute_id)?;
        }

        Ok(())
    }

    fn retract_old_values<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        entity: u64,
        attribute: u64,
    ) -> Result<(), S::Error> {
        // Retract previous values
        let restricts = Restricts::new(self.tx_id)
            .with_entity(entity)
            .with_attribute(attribute);
        for datom in storage.find(restricts) {
            self.push(Datom::retract(entity, attribute, datom?.value, self.tx_id));
        }
        Ok(())
    }

    fn push(&mut self, datom: Datom) {
        self.datoms.push(datom);
    }

    fn temp_id<E>(&self, temp_id: &Rc<str>) -> Result<u64, E> {
        self.temp_ids
            .get(temp_id)
            .copied()
            .ok_or_else(|| TransactionError::TempIdNotFound(Rc::clone(temp_id)))
    }

    fn resolve_entity<E>(&mut self, entity: OperatedEntity) -> Result<u64, E> {
        match entity {
            OperatedEntity::New => {
                self.next_id += 1;
                Ok(self.next_id)
            }
            OperatedEntity::Id(id) => Ok(id),
            OperatedEntity::TempId(temp_id) => self.temp_id(&temp_id),
        }
    }

    fn resolve_value<E>(&self, attribute_value: AttributeValue) -> Result<Value, E> {
        match attribute_value {
            AttributeValue::Value(value) => Ok(value),
            AttributeValue::TempId(temp_id) => self.temp_id(&temp_id).map(Value::Ref),
        }
    }

    fn verify_uniqueness_tx<E>(&mut self, attribute: &Attribute, value: &Value) -> Result<(), E> {
        // Find duplicate values within transaction.
        if !self.unique_values.insert((attribute.id, value.clone())) {
            return Err(TransactionError::DuplicateUniqueValue {
                attribute: attribute.id,
                value: value.clone(),
            });
        }
        Ok(())
    }

    fn verify_uniqueness_db<'a, S: ReadStorage<'a>>(
        &self,
        attribute: &Attribute,
        value: &Value,
        storage: &'a S,
    ) -> Result<(), S::Error> {
        // Find duplicate values previously saved.
        let restricts = Restricts::new(self.tx_id)
            .with_attribute(attribute.id)
            .with_value(value.clone());
        if storage.find(restricts).count() > 0 {
            return Err(TransactionError::DuplicateUniqueValue {
                attribute: attribute.id,
                value: value.clone(),
            });
        }
        Ok(())
    }
}

fn verify_type<E>(attribute: &Attribute, value: &Value) -> Result<(), E> {
    if attribute.definition.value_type != ValueType::from(value) {
        // Value type is incompatible with attribute, reject transaction.
        return Err(TransactionError::InvalidAttributeType {
            attribute_id: attribute.id,
            attribute_type: attribute.definition.value_type,
            value: value.clone(),
        });
    }
    Ok(())
}
