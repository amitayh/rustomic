use std::collections::HashMap;
use std::collections::HashSet;
use std::u64;

use crate::clock::Instant;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::attribute_resolver::*;
use crate::storage::restricts::*;
use crate::storage::*;
use crate::tx::{
    AttributeValue, Datom, EntityOperation, OperatedEntity, Result, Transaction, TransactionError,
    TransctionResult, Value, ValueType,
};

/// # Errors
/// Storage related errors
pub async fn transact<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    resolver: &AttributeResolver,
    now: Instant,
    transaction: Transaction,
) -> Result<TransctionResult, S::Error> {
    let next_id = NextId(storage.latest_entity_id()?);
    let mut builder = ResultBuilder::from(&transaction.operations, now, next_id)?;
    for operation in transaction.operations {
        builder.update(storage, resolver, operation).await?;
    }
    Ok(builder.build())
}

struct ResultBuilder {
    tx_id: u64,
    next_id: NextId,
    datoms: Vec<Datom>,
    temp_ids: HashMap<String, u64>,
    unique_values: HashSet<(u64, Value)>,
}

impl ResultBuilder {
    pub fn from<E>(
        operations: &[EntityOperation],
        Instant(now): Instant,
        mut next_id: NextId,
    ) -> Result<Self, E> {
        let tx_id = next_id.get();
        let temp_ids = generate_temp_ids(operations, &mut next_id)?;
        Ok(Self {
            tx_id,
            next_id,
            temp_ids,
            datoms: vec![Datom::add(tx_id, DB_TX_TIME_ID, now, tx_id)],
            unique_values: HashSet::new(),
        })
    }

    pub async fn update<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        resolver: &AttributeResolver,
        operation: EntityOperation,
    ) -> Result<(), S::Error> {
        let entity = self.resolve_entity(operation.entity)?;
        let mut retract_attributes = HashSet::with_capacity(operation.attributes.len());
        for attribute_value in operation.attributes {
            let attribute = resolver
                .resolve(storage, &attribute_value.attribute, self.tx_id)
                .await?;

            if attribute.definition.cardinality == Cardinality::One {
                // Values of attributes with cardinality `Cardinality::One` should be retracted
                // before asserting new values.
                retract_attributes.insert(attribute.id);
            }

            let value = self.resolve_value(attribute_value.value)?;
            verify_type(&attribute, &value)?;
            if attribute.definition.unique {
                self.verify_uniqueness_tx(&attribute, &value)?;
                self.verify_uniqueness_db(&attribute, &value, storage)?;
            }

            self.datoms.push(Datom {
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

    pub fn build(self) -> TransctionResult {
        TransctionResult {
            tx_id: self.tx_id,
            tx_data: self.datoms,
            temp_ids: self.temp_ids,
        }
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
            let retracted = Datom::retract(entity, attribute, datom?.value, self.tx_id);
            self.datoms.push(retracted);
        }
        Ok(())
    }

    fn temp_id<E>(&self, temp_id: &str) -> Result<u64, E> {
        self.temp_ids
            .get(temp_id)
            .copied()
            .ok_or_else(|| TransactionError::TempIdNotFound(temp_id.to_string()))
    }

    fn resolve_entity<E>(&mut self, entity: OperatedEntity) -> Result<u64, E> {
        match entity {
            OperatedEntity::New => Ok(self.next_id.get()),
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

fn generate_temp_ids<E>(
    operations: &[EntityOperation],
    next_id: &mut NextId,
) -> Result<HashMap<String, u64>, E> {
    let mut temp_ids = HashMap::with_capacity(operations.len());
    for operation in operations {
        if let OperatedEntity::TempId(temp_id) = &operation.entity {
            if temp_ids.insert(temp_id.clone(), next_id.get()).is_some() {
                return Err(TransactionError::DuplicateTempId(temp_id.clone()));
            }
        };
    }
    temp_ids.shrink_to_fit();
    Ok(temp_ids)
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

struct NextId(u64);

impl NextId {
    fn get(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}
