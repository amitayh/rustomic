use std::collections::hash_map;
use std::collections::VecDeque;
use std::marker::PhantomData;

use crate::query::*;

pub struct Aggregator<E> {
    results: hash_map::IntoIter<AggregationKey, AggregatedValues>,
    type_per_index: Vec<FindType>,
    marker: PhantomData<E>,
}

impl<E> Aggregator<E> {
    pub fn new<R: Iterator<Item = AssignmentResult<E>>>(
        finds: Vec<Find>,
        results: R,
    ) -> Result<Self, E> {
        // TODO: concurrent aggregation?
        let mut aggregated = HashMap::new();
        let partition = FindPartition::new(finds);
        for result in results {
            let assignment = result?;
            aggregated
                .entry(AggregationKey::new(&partition.variables, &assignment)?)
                .or_insert_with(|| AggregatedValues::new(&partition.aggregates))
                .update_with(&assignment);
        }
        Ok(Self {
            results: aggregated.into_iter(),
            type_per_index: partition.type_per_index,
            marker: PhantomData,
        })
    }
}

impl<E> Iterator for Aggregator<E> {
    type Item = QueryResult<E>;

    fn next(&mut self) -> Option<Self::Item> {
        let (mut variables, mut aggregates) = self.results.next()?;
        let mut result = Vec::with_capacity(self.type_per_index.len());
        for find_type in &self.type_per_index {
            let value = match find_type {
                FindType::Variable => variables.take_next(),
                FindType::Aggregate => aggregates.take_next(),
            }
            .expect("value should be present");
            result.push(value);
        }
        Some(Ok(result))
    }
}

enum FindType {
    Variable,
    Aggregate,
}

struct FindPartition {
    variables: Vec<String>,
    aggregates: Vec<AggregationFunction>,
    type_per_index: Vec<FindType>,
}

impl FindPartition {
    fn new(finds: Vec<Find>) -> Self {
        let capacity = finds.len();
        let mut variables = Vec::with_capacity(capacity);
        let mut aggregates = Vec::with_capacity(capacity);
        let mut type_per_index = Vec::with_capacity(capacity);
        for find in finds {
            match find {
                Find::Variable(variable) => {
                    variables.push(variable);
                    type_per_index.push(FindType::Variable);
                }
                Find::Aggregate(aggregate) => {
                    aggregates.push(aggregate);
                    type_per_index.push(FindType::Aggregate);
                }
            }
        }
        Self {
            variables,
            aggregates,
            type_per_index,
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct AggregationKey(VecDeque<Value>);

impl AggregationKey {
    fn new<E>(variables: &[String], assignment: &Assignment) -> Result<Self, E> {
        let values: Result<_, _> = variables
            .iter()
            .map(|variable| {
                assignment
                    .get(variable)
                    .cloned()
                    .ok_or_else(|| QueryError::InvalidFindVariable(variable.clone()))
            })
            .collect();
        Ok(Self(values?))
    }

    fn take_next(&mut self) -> Option<Value> {
        self.0.pop_front()
    }
}

#[derive(Clone)]
struct AggregatedValues(VecDeque<AggregationState>);

impl AggregatedValues {
    fn new(aggregates: &[AggregationFunction]) -> Self {
        Self(
            aggregates
                .iter()
                .map(|aggregate| aggregate.empty_state())
                .collect(),
        )
    }

    fn update_with(&mut self, assignment: &Assignment) {
        for aggregation_state in self.0.iter_mut() {
            aggregation_state.update_with(assignment);
        }
    }

    fn take_next(&mut self) -> Option<Value> {
        self.0.pop_front().map(|agg| agg.result())
    }
}
