use std::collections::HashMap;

use crate::clock::Instant;
use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;
use crate::tx::*;

type TempId = Rc<str>;
type EntityId = u64;
type TempIds = HashMap<TempId, EntityId>;

#[derive(Default)]
pub struct Transactor {
    next_entity_id: u64,
    attribute_resolver: AttributeResolver,
}

impl Transactor {
    pub fn new() -> Self {
        Self {
            next_entity_id: 100,
            attribute_resolver: AttributeResolver::new(),
        }
    }

    pub fn transact<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        now: Instant,
        transaction: Transaction,
    ) -> Result<TransctionResult, TransactionError<S::Error>> {
        let temp_ids = self.generate_temp_ids(&transaction)?;
        let datoms = self.transaction_datoms(storage, now, transaction, &temp_ids)?;

        Ok(TransctionResult {
            tx_id: datoms[0].tx,
            tx_data: datoms,
            temp_ids,
        })
    }

    fn generate_temp_ids<Error>(
        &mut self,
        transaction: &Transaction,
    ) -> Result<TempIds, TransactionError<Error>> {
        let mut temp_ids = HashMap::new();
        for operation in &transaction.operations {
            if let OperatedEntity::TempId(temp_id) = &operation.entity {
                let entity_id = self.next_entity_id();
                if temp_ids.insert(Rc::clone(temp_id), entity_id).is_some() {
                    return Err(TransactionError::DuplicateTempId(Rc::clone(temp_id)));
                }
            };
        }
        Ok(temp_ids)
    }

    fn transaction_datoms<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        now: Instant,
        transaction: Transaction,
        temp_ids: &TempIds,
    ) -> Result<Vec<Datom>, TransactionError<S::Error>> {
        let mut datoms = Vec::new();
        let tx = self.create_tx_datom(now);
        for operation in transaction.operations {
            self.operation_datoms(storage, tx.entity, operation, temp_ids, &mut datoms)?;
        }
        datoms.push(tx);
        Ok(datoms)
    }

    fn next_entity_id(&mut self) -> EntityId {
        self.next_entity_id += 1;
        self.next_entity_id
    }

    fn create_tx_datom(&mut self, now: Instant) -> Datom {
        let tx = self.next_entity_id();
        Datom::add(tx, DB_TX_TIME_ID, now.0, tx)
    }

    fn operation_datoms<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        tx: u64,
        operation: EntityOperation,
        temp_ids: &TempIds,
        datoms: &mut Vec<Datom>,
    ) -> Result<(), TransactionError<S::Error>> {
        let operation_attributes = operation.attributes.len();
        let entity = self.resolve_entity(&operation.entity, temp_ids)?;
        let mut retract_attributes = Vec::with_capacity(operation_attributes);
        for attribute_value in operation.attributes {
            let attribute =
                self.attribute_resolver
                    .resolve(storage, attribute_value.attribute, tx)?;

            if attribute.definition.cardinality == Cardinality::One {
                // Values of attributes with cardinality `Cardinality::One` should be retracted
                // before asserting new values.
                retract_attributes.push(attribute.id);
            }

            let value = match attribute_value.value {
                AttributeValue::Value(value) => Ok(value),
                AttributeValue::TempId(temp_id) => match temp_ids.get(&temp_id) {
                    Some(&entity) => Ok(Value::Ref(entity)),
                    None => Err(TransactionError::TempIdNotFound(temp_id)),
                },
            }?;

            if attribute.definition.value_type != (&value).into() {
                // Value type is incompatible with attribute, reject transaction.
                return Err(TransactionError::InvalidAttributeType);
            }

            if attribute.definition.unique {
                // TODO
            }

            datoms.push(Datom::add(entity, attribute.id, value, tx));
        }

        for attribute_id in retract_attributes {
            self.retract_old_values(storage, entity, attribute_id, tx, datoms)?;
        }

        Ok(())
    }

    fn retract_old_values<'a, S: ReadStorage<'a>>(
        &self,
        storage: &'a S,
        entity: u64,
        attribute: u64,
        tx: u64,
        datoms: &mut Vec<Datom>,
    ) -> Result<(), TransactionError<S::Error>> {
        // Retract previous values
        let restricts = Restricts::new(tx)
            .with_entity(entity)
            .with_attribute(attribute);
        for datom in storage.find(restricts) {
            datoms.push(Datom::retract(entity, attribute, datom?.value, tx));
        }
        Ok(())
    }

    fn resolve_entity<Error>(
        &mut self,
        entity: &OperatedEntity,
        temp_ids: &TempIds,
    ) -> Result<u64, TransactionError<Error>> {
        match entity {
            OperatedEntity::New => Ok(self.next_entity_id()),
            &OperatedEntity::Id(id) => Ok(id),
            OperatedEntity::TempId(temp_id) => temp_ids
                .get(temp_id)
                .copied()
                .ok_or_else(|| TransactionError::TempIdNotFound(Rc::clone(temp_id))),
        }
    }
}
