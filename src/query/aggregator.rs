use std::collections::VecDeque;

use crate::query::*;

// TODO: concurrent aggregation?
pub fn aggregate<E>(
    finds: Vec<Find>,
    results: impl Iterator<Item = AssignmentResult<E>>,
) -> Result<impl Iterator<Item = QueryResult<E>>, E> {
    let (variables, aggregates, type_per_index) = partition_by_type(finds);
    let aggregation_result = aggregate0(&variables, &aggregates, results)?;
    let query_result = project(aggregation_result, &type_per_index);
    Ok(query_result.into_iter())
}

enum FindType {
    Variable,
    Aggregate,
}

fn partition_by_type(finds: Vec<Find>) -> (Vec<String>, Vec<AggregationFunction>, Vec<FindType>) {
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
    variables.shrink_to_fit();
    aggregates.shrink_to_fit();
    (variables, aggregates, type_per_index)
}

fn aggregate0<'a, E>(
    variables: &[String],
    aggregates: &'a [AggregationFunction],
    results: impl Iterator<Item = AssignmentResult<E>>,
) -> Result<HashMap<AggregationKey, AggregatedValues<'a>>, E> {
    let mut aggregation_result = HashMap::new();
    for result in results {
        let assignment = result?;
        aggregation_result
            .entry(AggregationKey::new(variables, &assignment)?)
            .or_insert_with(|| AggregatedValues::new(aggregates))
            .update_with(&assignment);
    }
    Ok(aggregation_result)
}

fn project<E>(
    aggregation_result: HashMap<AggregationKey, AggregatedValues>,
    type_per_index: &[FindType],
) -> Vec<QueryResult<E>> {
    aggregation_result
        .into_iter()
        .map(|(mut variables, mut aggregates)| {
            let mut result = Vec::with_capacity(type_per_index.len());
            for find_type in type_per_index {
                let value = match find_type {
                    FindType::Variable => variables.take_next(),
                    FindType::Aggregate => aggregates.take_next(),
                }
                .expect("value should be present");
                result.push(value);
            }
            Ok(result)
        })
        .collect()
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
struct AggregatedValues<'a>(VecDeque<AggregationState<'a>>);

impl<'a> AggregatedValues<'a> {
    fn new(aggregates: &'a [AggregationFunction]) -> Self {
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
