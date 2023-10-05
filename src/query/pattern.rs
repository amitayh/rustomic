use std::ops::Bound;
use std::ops::RangeBounds;
use std::rc::Rc;

use crate::datom::*;

pub trait Pattern {
    fn variable_name(&self) -> Option<&str>;
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Pattern2<T> {
    Variable(Rc<str>),
    Constant(T),
    #[default]
    Blank,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum EntityPattern {
    Variable(Rc<str>),
    Id(u64),
    #[default]
    Blank,
}

impl EntityPattern {
    pub fn variable(name: &str) -> Self {
        EntityPattern::Variable(Rc::from(name))
    }
}

impl Pattern for EntityPattern {
    fn variable_name(&self) -> Option<&str> {
        match self {
            EntityPattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum AttributePattern {
    Variable(Rc<str>),
    Ident(Rc<str>),
    Id(u64),
    #[default]
    Blank,
}

impl AttributePattern {
    pub fn variable(name: &str) -> AttributePattern {
        AttributePattern::Variable(Rc::from(name))
    }

    pub fn ident(name: &str) -> AttributePattern {
        AttributePattern::Ident(Rc::from(name))
    }
}

impl Pattern for AttributePattern {
    fn variable_name(&self) -> Option<&str> {
        match self {
            AttributePattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum ValuePattern {
    Variable(Rc<str>),
    Constant(Value),
    //Range(Bound<&'a Value>, Bound<&'a Value>),
    #[default]
    Blank,
}

impl ValuePattern {
    pub fn variable(name: &str) -> Self {
        ValuePattern::Variable(Rc::from(name))
    }

    pub fn constant(value: Value) -> Self {
        ValuePattern::Constant(value)
    }

    //pub fn range<R: RangeBounds<Value>>(range: &'a R) -> Self {
    //    let start = range.start_bound();
    //    let end = range.end_bound();
    //    ValuePattern::Range(start, end)
    //}
}

impl Pattern for ValuePattern {
    fn variable_name(&self) -> Option<&str> {
        match self {
            ValuePattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum TxPattern {
    Variable(Rc<str>),
    Constant(u64),
    Range(Bound<u64>, Bound<u64>),
    #[default]
    Blank,
}

impl TxPattern {
    pub fn variable(name: &str) -> Self {
        TxPattern::Variable(Rc::from(name))
    }

    pub fn range<R: RangeBounds<u64>>(range: R) -> Self {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        TxPattern::Range(start, end)
    }
}

impl Pattern for TxPattern {
    fn variable_name(&self) -> Option<&str> {
        match self {
            TxPattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}
