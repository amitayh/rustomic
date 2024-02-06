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
            Ok(Self::Aggregate(Aggregator2::new(resolver, find)?))
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
    aggregated: std::collections::hash_map::IntoIter<Vec<Value>, Vec<Box<dyn Aggregator>>>,
    find_variables_indices: Vec<usize>,
    find_aggregates_indices: Vec<usize>,
    marker: PhantomData<&'a S>,
}

impl<'a, S: ReadStorage<'a>> Aggregator2<'a, S> {
    fn new(resolver: Resolver<'a, S>, find: Vec<Find>) -> Result<Self, QueryError<S::Error>> {
        // TODO concurrent aggregation
        let mut aggregated = HashMap::new();

        let find_length = find.len();
        let mut find_variables = Vec::with_capacity(find_length);
        let mut find_variables_indices = Vec::with_capacity(find_length);
        let mut find_aggregates = Vec::with_capacity(find_length);
        let mut find_aggregates_indices = Vec::with_capacity(find_length);
        for (index, find) in find.into_iter().enumerate() {
            match find {
                Find::Variable(variale) => {
                    find_variables.push(Rc::clone(&variale));
                    find_variables_indices.push(index)
                }
                Find::Aggregate(aggregate) => {
                    find_aggregates.push(aggregate);
                    find_aggregates_indices.push(index);
                }
            }
        }

        for assignment in resolver {
            let assignment = assignment?;
            let key = Self::key_of(&find_variables, &assignment);
            let entry = aggregated
                .entry(key)
                .or_insert_with(|| Self::init(&find_aggregates));
            for agg in entry {
                agg.consume(&assignment);
            }
        }
        Ok(Self {
            aggregated: aggregated.into_iter(),
            find_variables_indices,
            find_aggregates_indices,
            marker: PhantomData,
        })
    }

    fn key_of(variables: &[Rc<str>], assignment: &PartialAssignment) -> Vec<Value> {
        variables
            .iter()
            .map(|variable| assignment[variable].clone())
            .collect()
    }

    fn init(find_aggregates: &[Box<dyn ToAggregator>]) -> Vec<Box<dyn Aggregator>> {
        find_aggregates
            .iter()
            .map(|agg| agg.to_aggregator())
            .collect()
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Aggregator2<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.aggregated.next() {
            Some((key, value)) => {
                let mut result = vec![Value::U64(0); key.len() + value.len()];
                for (index, value) in self.find_variables_indices.iter().zip(key.into_iter()) {
                    result[*index] = value;
                }
                for (index, agg) in self.find_aggregates_indices.iter().zip(value.into_iter()) {
                    result[*index] = agg.result();
                }
                Some(Ok(result))
            }
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
