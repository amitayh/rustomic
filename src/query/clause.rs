use crate::datom::*;
use crate::query::assignment::*;
use crate::query::pattern::*;

#[derive(Clone, Debug, Default)]
pub struct Clause<'a> {
    pub entity: EntityPattern<'a>,
    pub attribute: AttributePattern<'a>,
    pub value: ValuePattern<'a>,
}

impl<'a> Clause<'a> {
    pub fn new() -> Self {
        Clause::default()
    }

    pub fn with_entity(mut self, entity: EntityPattern<'a>) -> Self {
        self.entity = entity;
        self
    }

    pub fn with_attribute(mut self, attribute: AttributePattern<'a>) -> Self {
        self.attribute = attribute;
        self
    }

    pub fn with_value(mut self, value: ValuePattern<'a>) -> Self {
        self.value = value;
        self
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
    ///     .with_value(ValuePattern::variable("baz"));
    ///
    /// let query = Query::new().wher(clause.clone());
    /// let mut assignment = Assignment::from_query(&query);
    /// assignment.assign("foo", 1u64);
    /// assignment.assign("bar", 2u64);
    /// assignment.assign("baz", 3u64);
    ///
    /// let assigned = clause.assign(&assignment);
    ///
    /// assert_eq!(EntityPattern::Id(1), assigned.entity);
    /// assert_eq!(AttributePattern::Id(2), assigned.attribute);
    /// assert_eq!(ValuePattern::Constant(&Value::U64(3)), assigned.value);
    /// ```
    pub fn assign(&self, assignment: &'a Assignment) -> Self {
        let mut clause = self.clone();
        if let Some(Value::U64(entity)) = assignment.assigned_value(&self.entity) {
            clause.entity = EntityPattern::Id(*entity);
        }
        if let Some(Value::U64(attribute)) = assignment.assigned_value(&self.attribute) {
            clause.attribute = AttributePattern::Id(*attribute);
        }
        if let Some(value) = assignment.assigned_value(&self.value) {
            clause.value = ValuePattern::Constant(value);
        }
        clause
    }
}
