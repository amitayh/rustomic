use std::marker::PhantomData;

use crate::query::pattern::AttributeIdentifier;
use crate::query::pattern::Pattern;
use crate::query::resolver::Resolver;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;

pub struct Db {
    basis_tx: u64,
    attribute_resolver: AttributeResolver,
}

impl Db {
    pub fn new(basis_tx: u64) -> Self {
        Self {
            basis_tx,
            attribute_resolver: AttributeResolver::new(),
        }
    }

    pub fn query<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        mut query: Query,
    ) -> Result<
        impl Iterator<Item = Result<Vec<Value>, QueryError<S::Error>>> + 'a,
        QueryError<S::Error>,
    > {
        self.resolve_idents(storage, &mut query)?;
        let Query { find, clauses, .. } = query;
        let iterator = Resolver::new(storage, clauses, self.basis_tx);
        Lala::new(iterator, find)
    }

    /// Resolves attribute idents. Mutates input `query` such that clauses with
    /// `AttributeIdentifier::Ident` will be replaced with `AttributeIdentifier::Id`.
    fn resolve_idents<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: &mut Query,
    ) -> Result<(), QueryError<S::Error>> {
        for clause in &mut query.clauses {
            if let Pattern::Constant(AttributeIdentifier::Ident(ident)) = &clause.attribute {
                let attribute =
                    self.attribute_resolver
                        .resolve(storage, Rc::clone(ident), self.basis_tx)?;

                clause.attribute = Pattern::id(attribute.id);
            }
        }
        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------

enum Lala<'a, S: ReadStorage<'a>> {
    Aggregate(Aggregator2<'a, S>),
    Project(Projector<'a, S>),
}

impl<'a, S: ReadStorage<'a>> Lala<'a, S> {
    fn new(resolver: Resolver<'a, S>, find: Vec<Find>) -> Result<Self, QueryError<S::Error>> {
        let is_aggregated = find.iter().any(|f| matches!(f, Find::Aggregate(_)));
        if is_aggregated {
            Ok(Self::Aggregate(Aggregator2::new(resolver, &find)?))
        } else {
            Ok(Self::Project(Projector::new(resolver, find)))
        }
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Lala<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Aggregate(aggregator) => aggregator.next(),
            Self::Project(projector) => projector.next(),
        }
    }
}

// ------------------------------------------------------------------------------------------------

struct Aggregator2<'a, S: ReadStorage<'a>> {
    aggregated: std::collections::hash_map::IntoValues<Vec<Value>, Vec<Value>>,
    marker: PhantomData<&'a S>,
}

impl<'a, S: ReadStorage<'a>> Aggregator2<'a, S> {
    fn new(resolver: Resolver<'a, S>, find: &[Find]) -> Result<Self, QueryError<S::Error>> {
        // TODO concurrent aggregation
        let mut aggregated = HashMap::new();

        let find_length = find.len();
        let mut find_variables = Vec::with_capacity(find_length);
        let mut find_aggregates = Vec::with_capacity(find_length);
        for (index, find) in find.iter().enumerate() {
            match find {
                Find::Variable(variale) => find_variables.push((index, Rc::clone(variale))),
                Find::Aggregate(aggregate) => {
                    find_aggregates.push((index, aggregate.to_aggregator()))
                }
            }
        }

        for assignment in resolver {
            let assignment = assignment?;
            let key = Self::key_of(&find_variables, &assignment);
            let entry = aggregated
                .entry(key.clone()) // TODO
                .or_insert_with(|| Self::init(&find_variables, &find_aggregates, &assignment));
            for (index, agg) in &mut find_aggregates {
                if let Some(value) = entry.get_mut(*index) {
                    agg.consume(&key, value, &assignment);
                }
            }
        }
        Ok(Self {
            aggregated: aggregated.into_values(),
            marker: PhantomData,
        })
    }

    fn key_of(variables: &[(usize, Rc<str>)], assignment: &PartialAssignment) -> Vec<Value> {
        let mut key = Vec::with_capacity(variables.len());
        for (_, variable) in variables {
            key.push(assignment[variable].clone());
        }
        key
    }

    fn init(
        find_variables: &[(usize, Rc<str>)],
        find_aggregates: &[(usize, Box<dyn Aggregator>)],
        assignment: &HashMap<Rc<str>, Value>,
    ) -> Vec<Value> {
        //let mut result = Vec::with_capacity(find_variables.len() + find_aggregates.len());
        let len = find_variables.len() + find_aggregates.len();
        let mut result = vec![Value::U64(0); len];
        for (index, variable) in find_variables {
            result[*index] = assignment[variable].clone();
        }
        for (index, agg) in find_aggregates {
            result[*index] = agg.init();
        }
        result
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Aggregator2<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.aggregated.next() {
            Some(value) => Some(Ok(value)),
            None => None,
        }
    }
}

// ------------------------------------------------------------------------------------------------

struct Projector<'a, S: ReadStorage<'a>> {
    resolver: Resolver<'a, S>,
    find: Vec<Find>,
}

impl<'a, S: ReadStorage<'a>> Projector<'a, S> {
    fn new(resolver: Resolver<'a, S>, find: Vec<Find>) -> Self {
        Self { resolver, find }
    }

    fn project(&self, mut assignment: HashMap<Rc<str>, Value>) -> Option<<Self as Iterator>::Item> {
        let mut result = Vec::with_capacity(self.find.len());
        for find in &self.find {
            if let Find::Variable(variable) = find {
                let value = assignment.remove(variable)?;
                result.push(value);
            }
        }
        Some(Ok(result))
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Projector<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.resolver.next() {
            Some(Ok(assignment)) => self.project(assignment),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

// ------------------------------------------------------------------------------------------------
