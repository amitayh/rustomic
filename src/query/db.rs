use std::marker::PhantomData;

use crate::query::pattern::AttributeIdentifier;
use crate::query::pattern::Pattern;
use crate::query::projector::Projector;
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
    ) -> Result<impl Iterator<Item = QueryResult<S::Error>>, S::Error> {
        self.resolve_idents(storage, &mut query)?;
        let Query {
            find,
            clauses,
            predicates,
        } = query;
        let resolved = Resolver::new(storage, clauses, self.basis_tx);
        let filtered = resolved.filter(move |assignment| match assignment {
            Ok(result) => predicates.iter().all(|predicate| predicate(result)),
            Err(_) => true,
        });
        Lala::new(filtered, find)
    }

    /// Resolves attribute idents. Mutates input `query` such that clauses with
    /// `AttributeIdentifier::Ident` will be replaced with `AttributeIdentifier::Id`.
    fn resolve_idents<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: &mut Query,
    ) -> Result<(), S::Error> {
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

enum Lala<E, I> {
    Aggregate(Aggregator2<E>),
    Project(Projector<I>),
}

impl<E, I: Iterator<Item = AssignmentResult<E>>> Lala<E, I> {
    fn new(resolver: I, find: Vec<Find>) -> Result<Self, E> {
        let is_aggregated = find.iter().any(|f| matches!(f, Find::Aggregate(_)));
        if is_aggregated {
            Ok(Self::Aggregate(Aggregator2::new(resolver, find)?))
        } else {
            Ok(Self::Project(Projector::new(resolver, find)))
        }
    }
}

impl<E, I: Iterator<Item = AssignmentResult<E>>> Iterator for Lala<E, I> {
    type Item = QueryResult<E>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Aggregate(aggregator) => aggregator.next(),
            Self::Project(projector) => projector.next(),
        }
    }
}

// ------------------------------------------------------------------------------------------------

struct Aggregator2<E> {
    aggregated: std::collections::hash_map::IntoIter<Vec<Value>, Vec<Box<dyn Aggregator>>>,
    indices: Indices,
    marker: PhantomData<E>,
}

impl<E> Aggregator2<E> {
    fn new(results: impl Iterator<Item = AssignmentResult<E>>, find: Vec<Find>) -> Result<Self, E> {
        // TODO concurrent aggregation
        let mut aggregated = HashMap::new();

        let len = find.len();
        let mut variables = Vec::with_capacity(len);
        let mut aggregates = Vec::with_capacity(len);
        let indices = Indices::new(&find);
        for f in find {
            match f {
                Find::Variable(variale) => variables.push(Rc::clone(&variale)),
                Find::Aggregate(aggregate) => aggregates.push(aggregate),
            }
        }

        for assignment in results {
            let assignment = assignment?;
            let key = key_of(&variables, &assignment);
            let entry = aggregated
                .entry(key)
                .or_insert_with(|| init_aggregators(&aggregates));
            for agg in entry {
                agg.consume(&assignment);
            }
        }
        Ok(Self {
            aggregated: aggregated.into_iter(),
            indices,
            marker: PhantomData,
        })
    }
}

struct Indices {
    variables: Vec<usize>,
    aggregates: Vec<usize>,
}

impl Indices {
    fn new(find: &[Find]) -> Self {
        let len = find.len();
        let mut variables = Vec::with_capacity(len);
        let mut aggregates = Vec::with_capacity(len);
        for (index, f) in find.iter().enumerate() {
            match f {
                Find::Variable(_) => variables.push(index),
                Find::Aggregate(_) => aggregates.push(index),
            }
        }
        Self {
            variables,
            aggregates,
        }
    }

    fn arrange(&self, key: Vec<Value>, value: Vec<Box<dyn Aggregator>>) -> Vec<Value> {
        let mut result = vec![Value::U64(0); key.len() + value.len()];
        for (index, value) in self.variables.iter().zip(key.into_iter()) {
            result[*index] = value;
        }
        for (index, agg) in self.aggregates.iter().zip(value.into_iter()) {
            result[*index] = agg.result();
        }
        result
    }
}

fn key_of(variables: &[Rc<str>], assignment: &Assignment) -> Vec<Value> {
    variables
        .iter()
        .map(|variable| assignment[variable].clone())
        .collect()
}

fn init_aggregators(aggregates: &[Rc<dyn ToAggregator>]) -> Vec<Box<dyn Aggregator>> {
    aggregates
        .iter()
        .map(|aggregate| aggregate.to_aggregator())
        .collect()
}

impl<E> Iterator for Aggregator2<E> {
    type Item = QueryResult<E>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.aggregated.next() {
            Some((key, value)) => Some(Ok(self.indices.arrange(key, value))),
            None => None,
        }
    }
}
