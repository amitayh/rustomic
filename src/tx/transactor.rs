use std::collections::HashMap;
use std::collections::HashSet;

use crate::clock::Instant;
use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::attribute_resolver::*;
use crate::storage::restricts::*;
use crate::storage::*;
use crate::tx::*;

type TempId = Rc<str>;
type EntityId = u64;

#[derive(Default)]
pub struct Transactor {
    attribute_resolver: AttributeResolver,
}

impl Transactor {
    pub fn new() -> Self {
        Self {
            attribute_resolver: AttributeResolver::new(),
        }
    }

    pub fn transact<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        now: Instant,
        transaction: Transaction,
    ) -> Result<TransctionResult, S::Error> {
        let mut id_allocator = IdAllocator(storage.latest_entity_id()?);
        let temp_ids = self.generate_temp_ids(&transaction, &mut id_allocator)?;
        let datoms =
            self.transaction_datoms(storage, now, transaction, &temp_ids, &mut id_allocator)?;

        Ok(TransctionResult {
            tx_id: datoms[0].tx,
            tx_data: datoms,
            temp_ids: temp_ids.0,
        })
    }

    fn generate_temp_ids<E>(
        &mut self,
        transaction: &Transaction,
        id_allocator: &mut IdAllocator,
    ) -> Result<TempIds, E> {
        let mut temp_ids = HashMap::new();
        for operation in &transaction.operations {
            if let OperatedEntity::TempId(temp_id) = &operation.entity {
                let entity_id = id_allocator.next();
                if temp_ids.insert(Rc::clone(temp_id), entity_id).is_some() {
                    return Err(TransactionError::DuplicateTempId(Rc::clone(temp_id)));
                }
            };
        }
        Ok(TempIds(temp_ids))
    }

    fn transaction_datoms<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        now: Instant,
        transaction: Transaction,
        temp_ids: &TempIds,
        id_allocator: &mut IdAllocator,
    ) -> Result<Vec<Datom>, S::Error> {
        let mut datoms = Vec::with_capacity(transaction.total_attribute_operations());
        let mut unique_values = HashSet::new(); // TODO capacity?
        let tx = self.create_tx_datom(now, id_allocator);
        for operation in transaction.operations {
            self.operation_datoms(
                storage,
                tx.entity,
                operation,
                temp_ids,
                &mut datoms,
                &mut unique_values,
                id_allocator,
            )?;
        }
        datoms.push(tx);
        Ok(datoms)
    }

    fn create_tx_datom(&mut self, now: Instant, id_allocator: &mut IdAllocator) -> Datom {
        let tx = id_allocator.next();
        Datom::add(tx, DB_TX_TIME_ID, now.0, tx)
    }

    fn operation_datoms<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        tx: u64,
        operation: EntityOperation,
        temp_ids: &TempIds,
        datoms: &mut Vec<Datom>,
        unique_values: &mut HashSet<(u64, Value)>,
        id_allocator: &mut IdAllocator,
    ) -> Result<(), S::Error> {
        let entity = self.resolve_entity(operation.entity, temp_ids, id_allocator)?;
        let mut retract_attributes = HashSet::with_capacity(operation.attributes.len());
        for attribute_value in operation.attributes {
            let attribute =
                self.attribute_resolver
                    .resolve(storage, attribute_value.attribute, tx)?;

            if attribute.definition.cardinality == Cardinality::One {
                // Values of attributes with cardinality `Cardinality::One` should be retracted
                // before asserting new values.
                retract_attributes.insert(attribute.id);
            }

            let value = resolve_value(attribute_value.value, temp_ids)?;
            verify_type(attribute, &value)?;
            if attribute.definition.unique {
                verify_uniqueness_tx(attribute, &value, unique_values)?;
                verify_uniqueness_db(attribute, &value, storage, tx)?;
            }

            datoms.push(Datom {
                entity,
                attribute: attribute.id,
                value,
                tx,
                op: attribute_value.op,
            });
        }

        for attribute_id in retract_attributes {
            retract_old_values(storage, entity, attribute_id, tx, datoms)?;
        }

        Ok(())
    }

    fn resolve_entity<E>(
        &mut self,
        entity: OperatedEntity,
        temp_ids: &TempIds,
        id_allocator: &mut IdAllocator,
    ) -> Result<EntityId, E> {
        match entity {
            OperatedEntity::New => Ok(id_allocator.next()),
            OperatedEntity::Id(id) => Ok(id),
            OperatedEntity::TempId(temp_id) => temp_ids.get(&temp_id),
        }
    }
}

struct IdAllocator(EntityId);

impl IdAllocator {
    fn next(&mut self) -> EntityId {
        self.0 += 1;
        self.0
    }
}

fn resolve_value<E>(attribute_value: AttributeValue, temp_ids: &TempIds) -> Result<Value, E> {
    match attribute_value {
        AttributeValue::Value(value) => Ok(value),
        AttributeValue::TempId(temp_id) => temp_ids.get(&temp_id).map(Value::Ref),
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

fn verify_uniqueness_tx<E>(
    attribute: &Attribute,
    value: &Value,
    unique_values: &mut HashSet<(u64, Value)>,
) -> Result<(), E> {
    // Find duplicate values within transaction.
    if !unique_values.insert((attribute.id, value.clone())) {
        return Err(TransactionError::DuplicateUniqueValue {
            attribute: attribute.id,
            value: value.clone(),
        });
    }
    Ok(())
}

fn verify_uniqueness_db<'a, S: ReadStorage<'a>>(
    attribute: &Attribute,
    value: &Value,
    storage: &'a S,
    basis_tx: u64,
) -> Result<(), S::Error> {
    // Find duplicate values previously saved.
    let restricts = Restricts::new(basis_tx)
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

fn retract_old_values<'a, S: ReadStorage<'a>>(
    storage: &'a S,
    entity: u64,
    attribute: u64,
    tx: u64,
    datoms: &mut Vec<Datom>,
) -> Result<(), S::Error> {
    // Retract previous values
    let restricts = Restricts::new(tx)
        .with_entity(entity)
        .with_attribute(attribute);
    for datom in storage.find(restricts) {
        datoms.push(Datom::retract(entity, attribute, datom?.value, tx));
    }
    Ok(())
}

struct TempIds(HashMap<TempId, EntityId>);

impl TempIds {
    fn get<E>(&self, temp_id: &Rc<str>) -> Result<EntityId, E> {
        self.0
            .get(temp_id)
            .copied()
            .ok_or_else(|| TransactionError::TempIdNotFound(Rc::clone(temp_id)))
    }
}
