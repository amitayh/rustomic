use crate::datom::*;
use crate::query::assignment::*;
use crate::query::pattern::*;

#[derive(Clone, Debug, Default)]
pub struct Clause {
    pub entity: EntityPattern,
    pub attribute: AttributePattern,
    pub value: ValuePattern,
    pub tx: TxPattern,
}

impl Clause {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_entity(mut self, entity: EntityPattern) -> Self {
        self.entity = entity;
        self
    }

    pub fn with_attribute(mut self, attribute: AttributePattern) -> Self {
        self.attribute = attribute;
        self
    }

    pub fn with_value(mut self, value: ValuePattern) -> Self {
        self.value = value;
        self
    }

    pub fn with_tx(mut self, tx: TxPattern) -> Self {
        self.tx = tx;
        self
    }

    pub fn with_tx2(&mut self, tx: TxPattern) {
        self.tx = tx;
        //*self
    }

    /// ```
    /// use rustomic::query::clause::*;
    /// use rustomic::query::pattern::*;
    ///
    /// let clause = Clause::new()
    ///     .with_entity(EntityPattern::variable("foo"))
    ///     .with_attribute(AttributePattern::variable("bar"))
    ///     .with_value(ValuePattern::variable("baz"));
    ///
    /// let free_variables = clause.free_variables();
    /// assert_eq!(3, free_variables.len());
    /// assert!(free_variables.contains(&"foo"));
    /// assert!(free_variables.contains(&"bar"));
    /// assert!(free_variables.contains(&"baz"));
    /// ```
    pub fn free_variables(&self) -> Vec<&str> {
        let mut variables = Vec::new();
        if let Some(variable) = self.entity.variable_name() {
            variables.push(variable);
        }
        if let Some(variable) = self.attribute.variable_name() {
            variables.push(variable);
        }
        if let Some(variable) = self.value.variable_name() {
            variables.push(variable);
        }
        if let Some(variable) = self.tx.variable_name() {
            variables.push(variable);
        }
        variables
    }

    /// ```
    /// use rustomic::query::*;
    /// use rustomic::query::assignment::*;
    /// use rustomic::query::clause::*;
    /// use rustomic::query::pattern::*;
    /// use rustomic::datom::*;
    ///
    /// let clause = Clause::new()
    ///     .with_entity(EntityPattern::variable("foo"))
    ///     .with_attribute(AttributePattern::variable("bar"))
    ///     .with_value(ValuePattern::variable("baz"))
    ///     .with_tx(TxPattern::variable("qux"));
    ///
    /// let query = Query::new().wher(clause.clone());
    /// let mut assignment = Assignment::from_query(&query);
    /// assignment.assign("foo", 1u64);
    /// assignment.assign("bar", 2u64);
    /// assignment.assign("baz", 3u64);
    /// assignment.assign("qux", 4u64);
    ///
    /// let assigned = clause.assign(&assignment);
    ///
    /// assert_eq!(EntityPattern::Id(1), assigned.entity);
    /// assert_eq!(AttributePattern::Id(2), assigned.attribute);
    /// assert_eq!(ValuePattern::Constant(Value::U64(3)), assigned.value);
    /// assert_eq!(TxPattern::Constant(4), assigned.tx);
    /// ```
    pub fn assign(&self, assignment: &Assignment) -> Self {
        let mut clause = self.clone();
        if let Some(Value::U64(entity)) = assignment.assigned_value(&self.entity) {
            clause.entity = EntityPattern::Id(*entity);
        }
        if let Some(Value::U64(attribute)) = assignment.assigned_value(&self.attribute) {
            clause.attribute = AttributePattern::Id(*attribute);
        }
        if let Some(value) = assignment.assigned_value(&self.value) {
            clause.value = ValuePattern::Constant(value.clone());
        }
        if let Some(Value::U64(tx)) = assignment.assigned_value(&self.tx) {
            clause.tx = TxPattern::Constant(*tx);
        }
        clause
    }
}
