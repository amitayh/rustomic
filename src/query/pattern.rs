use std::ops::Bound;
use std::ops::RangeBounds;

use crate::datom::*;

pub trait Pattern {
    fn variable_name(&self) -> Option<&str>;
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum EntityPattern<'a> {
    Variable(&'a str),
    Id(u64),
    #[default]
    Blank,
}

impl<'a> EntityPattern<'a> {
    pub fn variable(name: &'a str) -> Self {
        EntityPattern::Variable(name)
    }
}

impl<'a> Pattern for EntityPattern<'a> {
    fn variable_name(&self) -> Option<&str> {
        match *self {
            EntityPattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum AttributePattern<'a> {
    Variable(&'a str),
    Ident(&'a str),
    Id(u64),
    #[default]
    Blank,
}

impl<'a> AttributePattern<'a> {
    pub fn variable(name: &str) -> AttributePattern {
        AttributePattern::Variable(name)
    }

    pub fn ident(name: &str) -> AttributePattern {
        AttributePattern::Ident(name)
    }
}

impl<'a> Pattern for AttributePattern<'a> {
    fn variable_name(&self) -> Option<&str> {
        match *self {
            AttributePattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum ValuePattern<'a> {
    Variable(&'a str),
    Constant(&'a Value),
    Range(Bound<&'a Value>, Bound<&'a Value>),
    #[default]
    Blank,
}

impl<'a> ValuePattern<'a> {
    pub fn variable(name: &'a str) -> Self {
        ValuePattern::Variable(name)
    }

    pub fn constant(value: &'a Value) -> Self {
        ValuePattern::Constant(value)
    }

    pub fn range<R: RangeBounds<Value>>(range: &'a R) -> Self {
        let start = range.start_bound();
        let end = range.end_bound();
        ValuePattern::Range(start, end)
    }
}

impl<'a> Pattern for ValuePattern<'a> {
    fn variable_name(&self) -> Option<&str> {
        match *self {
            ValuePattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}
