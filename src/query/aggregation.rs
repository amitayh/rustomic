use crate::datom::Value;
use crate::query::*;
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::u64;

#[derive(Clone)]
pub enum AggregationState<'a> {
    Count(u64),
    Min {
        variable: &'a str,
        min: Option<i64>,
    },
    Max {
        variable: &'a str,
        max: Option<i64>,
    },
    Average {
        variable: &'a str,
        sum: i64,
        count: usize,
    },
    Sum {
        variable: &'a str,
        sum: i64,
    },
    CountDistinct {
        variable: &'a str,
        seen: HashSet<Value>,
    },
}

impl<'a> AggregationState<'a> {
    fn count() -> Self {
        Self::Count(0)
    }

    fn min(variable: &'a str) -> Self {
        Self::Min {
            variable,
            min: None,
        }
    }

    fn max(variable: &'a str) -> Self {
        Self::Max {
            variable,
            max: None,
        }
    }

    fn average(variable: &'a str) -> Self {
        Self::Average {
            variable,
            sum: 0,
            count: 0,
        }
    }

    fn sum(variable: &'a str) -> Self {
        Self::Sum { variable, sum: 0 }
    }

    fn count_distinct(variable: &'a str) -> Self {
        Self::CountDistinct {
            variable,
            seen: HashSet::new(),
        }
    }

    pub fn update_with(&mut self, assignment: &Assignment) {
        match self {
            Self::Count(count) => *count += 1,
            Self::Min { variable, min } => {
                if let Some(&Value::I64(value)) = assignment.get(*variable) {
                    *min = min.map_or_else(|| Some(value), |prev| Some(prev.min(value)));
                }
            }
            Self::Max { variable, max } => {
                if let Some(&Value::I64(value)) = assignment.get(*variable) {
                    *max = max.map_or_else(|| Some(value), |prev| Some(prev.max(value)));
                }
            }
            Self::Average {
                variable,
                sum,
                count,
            } => {
                if let Some(Value::I64(value)) = assignment.get(*variable) {
                    *sum += value;
                    *count += 1;
                }
            }
            Self::Sum { variable, sum } => {
                if let Some(Value::I64(value)) = assignment.get(*variable) {
                    *sum += value;
                }
            }
            Self::CountDistinct { variable, seen } => {
                if let Some(value) = assignment.get(*variable) {
                    if !seen.contains(value) {
                        seen.insert(value.clone());
                    }
                }
            }
        }
    }

    pub fn result(self) -> Value {
        match self {
            Self::Count(count) => Value::U64(count),
            Self::Min { min, .. } => min.map(Value::I64).unwrap_or(Value::Nil),
            Self::Max { max, .. } => max.map(Value::I64).unwrap_or(Value::Nil),
            Self::Average { sum, count, .. } => {
                if count != 0 {
                    Value::Decimal(Decimal::from(sum) / Decimal::from(count))
                } else {
                    Value::Nil
                }
            }
            Self::Sum { sum, .. } => Value::I64(sum),
            Self::CountDistinct { seen, .. } => Value::U64(seen.len() as u64),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggregationFunction {
    Count,
    Min(String),
    Max(String),
    Average(String),
    Sum(String),
    CountDistinct(String),
}

impl AggregationFunction {
    pub fn empty_state(&self) -> AggregationState {
        match self {
            AggregationFunction::Count => AggregationState::count(),
            AggregationFunction::Min(variable) => AggregationState::min(variable),
            AggregationFunction::Max(variable) => AggregationState::max(variable),
            AggregationFunction::Average(variable) => AggregationState::average(variable),
            AggregationFunction::Sum(variable) => AggregationState::sum(variable),
            AggregationFunction::CountDistinct(variable) => {
                AggregationState::count_distinct(variable)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::prelude::*;
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    use crate::datom::Value;
    use crate::query::aggregation::AggregationFunction;

    mod count {
        use super::*;

        #[test]
        fn empty() {
            let count = AggregationFunction::Count;
            assert_eq!(Value::U64(0), count.empty_state().result());
        }

        #[test]
        fn non_empty() {
            let count = AggregationFunction::Count;
            let assignment = HashMap::new();

            let mut state = count.empty_state();
            state.update_with(&assignment);
            state.update_with(&assignment);

            assert_eq!(Value::U64(2), state.result());
        }
    }

    mod min {
        use super::*;

        #[test]
        fn empty() {
            let min = AggregationFunction::Min("foo".to_string());
            assert_eq!(Value::Nil, min.empty_state().result());
        }

        #[test]
        fn non_empty() {
            let variable = "foo".to_string();
            let min = AggregationFunction::Min(variable.clone());

            let mut state = min.empty_state();
            state.update_with(&HashMap::from([(variable.clone(), Value::I64(1))]));
            state.update_with(&HashMap::from([(variable.clone(), Value::I64(2))]));

            assert_eq!(Value::I64(1), state.result());
        }
    }

    mod max {
        use super::*;

        #[test]
        fn empty() {
            let max = AggregationFunction::Max("foo".to_string());
            assert_eq!(Value::Nil, max.empty_state().result());
        }

        #[test]
        fn non_empty() {
            let variable = "foo".to_string();
            let max = AggregationFunction::Max(variable.clone());

            let mut state = max.empty_state();
            state.update_with(&HashMap::from([(variable.clone(), Value::I64(1))]));
            state.update_with(&HashMap::from([(variable.clone(), Value::I64(2))]));

            assert_eq!(Value::I64(2), state.result());
        }
    }

    mod average {
        use super::*;

        #[test]
        fn empty() {
            let average = AggregationFunction::Average("foo".to_string());
            assert_eq!(Value::Nil, average.empty_state().result());
        }

        #[test]
        fn non_empty() {
            let variable = "foo".to_string();
            let average = AggregationFunction::Average(variable.clone());

            let mut state = average.empty_state();
            state.update_with(&HashMap::from([(variable.clone(), Value::I64(1))]));
            state.update_with(&HashMap::from([(variable.clone(), Value::I64(2))]));

            assert_eq!(
                Value::Decimal(Decimal::from_f64(1.5).unwrap()),
                state.result()
            );
        }
    }

    mod sum {
        use super::*;

        #[test]
        fn empty() {
            let sum = AggregationFunction::Sum("foo".to_string());
            assert_eq!(Value::I64(0), sum.empty_state().result());
        }

        #[test]
        fn non_empty() {
            let variable = "foo".to_string();
            let sum = AggregationFunction::Sum(variable.clone());

            let mut state = sum.empty_state();
            state.update_with(&HashMap::from([(variable.clone(), Value::I64(1))]));
            state.update_with(&HashMap::from([(variable.clone(), Value::I64(2))]));

            assert_eq!(Value::I64(3), state.result());
        }
    }

    mod count_distinct {
        use super::*;

        #[test]
        fn empty() {
            let count_distinct = AggregationFunction::CountDistinct("foo".to_string());
            assert_eq!(Value::U64(0), count_distinct.empty_state().result());
        }

        #[test]
        fn equal_values() {
            let variable = "foo".to_string();
            let count_distinct = AggregationFunction::CountDistinct(variable.clone());

            let mut state = count_distinct.empty_state();
            state.update_with(&HashMap::from([(variable.clone(), Value::U64(1))]));
            state.update_with(&HashMap::from([(variable.clone(), Value::U64(1))]));

            assert_eq!(Value::U64(1), state.result());
        }

        #[test]
        fn distinct_values() {
            let variable = "foo".to_string();
            let count_distinct = AggregationFunction::CountDistinct(variable.clone());

            let mut state = count_distinct.empty_state();
            state.update_with(&HashMap::from([(variable.clone(), Value::U64(1))]));
            state.update_with(&HashMap::from([(variable.clone(), Value::U64(2))]));

            assert_eq!(Value::U64(2), state.result());
        }
    }
}
