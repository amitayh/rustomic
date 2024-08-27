use std::rc::Rc;

use crate::datom::*;
use crate::query::pattern::*;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Clause {
    pub entity: Pattern<u64>,
    pub attribute: Pattern<AttributeIdentifier>,
    pub value: Pattern<Value>,
    pub tx: Pattern<u64>,
}

impl Clause {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_entity(mut self, entity: Pattern<u64>) -> Self {
        self.entity = entity;
        self
    }

    pub fn with_attribute(mut self, attribute: Pattern<AttributeIdentifier>) -> Self {
        self.attribute = attribute;
        self
    }

    pub fn with_value(mut self, value: Pattern<Value>) -> Self {
        self.value = value;
        self
    }

    pub fn with_tx(mut self, tx: Pattern<u64>) -> Self {
        self.tx = tx;
        self
    }

    /// ```
    /// use std::rc::Rc;
    /// use rustomic::query::clause::*;
    /// use rustomic::query::pattern::*;
    ///
    /// let clause = Clause::new()
    ///     .with_entity(Pattern::variable("foo"))
    ///     .with_attribute(Pattern::variable("bar"))
    ///     .with_value(Pattern::variable("baz"));
    ///
    /// let free_variables = clause.free_variables();
    /// assert_eq!(3, free_variables.len());
    /// assert!(free_variables.contains(&Rc::from("foo")));
    /// assert!(free_variables.contains(&Rc::from("bar")));
    /// assert!(free_variables.contains(&Rc::from("baz")));
    /// ```
    pub fn free_variables(&self) -> Vec<Rc<str>> {
        let mut variables = Vec::with_capacity(4);
        if let Pattern::Variable(ref variable) = self.entity {
            variables.push(Rc::clone(variable));
        }
        if let Pattern::Variable(ref variable) = self.attribute {
            variables.push(Rc::clone(variable));
        }
        if let Pattern::Variable(ref variable) = self.value {
            variables.push(Rc::clone(variable));
        }
        if let Pattern::Variable(ref variable) = self.tx {
            variables.push(Rc::clone(variable));
        }
        variables.shrink_to_fit();
        variables
    }
}
