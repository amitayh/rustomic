use crate::datom::*;
use crate::query::assignment::PartialAssignment;
use crate::query::clause::*;
use crate::query::pattern::*;

#[derive(Debug, Clone)]
pub struct Restricts {
    pub entity: Option<u64>,
    pub attribute: Option<u64>,
    pub value: Option<Value>,
    pub tx: TxRestrict,
}

impl Restricts {
    pub fn new(basis_tx: u64) -> Self {
        Self {
            entity: None,
            attribute: None,
            value: None,
            tx: TxRestrict::AtMost(basis_tx),
        }
    }

    pub fn from(clause: &Clause, assignment: &PartialAssignment, basis_tx: u64) -> Self {
        let entity = match clause.entity {
            Pattern::Constant(entity) => Some(entity),
            Pattern::Variable(ref variable) => assignment.get_ref(variable),
            _ => None,
        };
        let attribute = match clause.attribute {
            Pattern::Constant(AttributeIdentifier::Id(attribute)) => Some(attribute),
            Pattern::Variable(ref variable) => assignment.get_ref(variable),
            _ => None,
        };
        let value = match clause.value {
            Pattern::Constant(ref value) => Some(value.clone()),
            Pattern::Variable(ref variable) => assignment.get(variable).cloned(),
            _ => None,
        };
        let tx = match clause.tx {
            Pattern::Constant(tx) => TxRestrict::Exact(tx),
            Pattern::Variable(ref variable) => match assignment.get_ref(variable) {
                Some(entity) => TxRestrict::Exact(entity),
                _ => TxRestrict::AtMost(basis_tx),
            },
            _ => TxRestrict::AtMost(basis_tx),
        };
        Self {
            entity,
            attribute,
            value,
            tx,
        }
    }

    pub fn with_entity(mut self, entity: u64) -> Self {
        self.entity = Some(entity);
        self
    }

    pub fn with_attribute(mut self, attribute: u64) -> Self {
        self.attribute = Some(attribute);
        self
    }

    pub fn with_value(mut self, value: Value) -> Self {
        self.value = Some(value);
        self
    }

    pub fn with_tx(mut self, tx: u64) -> Self {
        self.tx = TxRestrict::Exact(tx);
        self
    }

    pub fn test(&self, datom: &Datom) -> bool {
        datom.op == Op::Added
            && self.entity.map_or(true, |e| datom.entity == e)
            && self.attribute.map_or(true, |a| datom.attribute == a)
            && self.value.as_ref().map_or(true, |v| &datom.value == v)
            && self.tx.test(datom.tx)
    }
}

#[derive(Debug, Clone)]
pub enum TxRestrict {
    Exact(u64),
    AtMost(u64),
}

impl TxRestrict {
    pub fn value(&self) -> u64 {
        match *self {
            TxRestrict::Exact(tx) => tx,
            TxRestrict::AtMost(tx) => tx,
        }
    }

    fn test(&self, tx: u64) -> bool {
        match *self {
            TxRestrict::Exact(tx0) => tx == tx0,
            TxRestrict::AtMost(tx0) => tx <= tx0,
        }
    }
}
