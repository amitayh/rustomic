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

impl Value {
    pub fn as_u8(&self) -> Option<&u8> {
        if let Value::U8(value) = self {
            return Some(value);
        }
        None
    }

    pub fn as_u64(&self) -> Option<&u64> {
        if let Value::U64(value) = self {
            return Some(value);
        }
        None
    }

    pub fn as_str(&self) -> Option<&str> {
        if let Value::Str(value) = self {
            return Some(value);
        }
        None
    }
}

impl Into<Value> for u8 {
    fn into(self) -> Value {
        Value::U8(self)
    }
}

impl Into<Value> for i32 {
    fn into(self) -> Value {
        Value::I32(self)
    }
}

impl Into<Value> for u32 {
    fn into(self) -> Value {
        Value::U32(self)
    }
}

impl Into<Value> for i64 {
    fn into(self) -> Value {
        Value::I64(self)
    }
}

impl Into<Value> for u64 {
    fn into(self) -> Value {
        Value::U64(self)
    }
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

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum Op {
    Added,
    Retracted,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
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
