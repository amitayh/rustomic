#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum Value {
    U8(u8),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    ShortString(&'static str),
    Str(String),
}

impl Into<Value> for &str {
    fn into(self) -> Value {
        Value::Str(String::from(self))
    }
}

impl Into<Value> for String {
    fn into(self) -> Value {
        Value::Str(self)
    }
}

impl Into<Value> for u8 {
    fn into(self) -> Value {
        Value::U8(self)
    }
}

impl Into<Value> for u64 {
    fn into(self) -> Value {
        Value::U64(self)
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum Op {
    Added,
    Retracted,
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Datom {
    pub entity: u64,
    pub attribute: u64,
    pub value: Value,
    pub tx: u64,
    pub op: Op,
}

impl Datom {
    pub fn new<V: Into<Value>>(entity: u64, attribute: u64, value: V, tx: u64) -> Datom {
        Datom {
            entity,
            attribute,
            value: value.into(),
            tx,
            op: Op::Added,
        }
    }
}
