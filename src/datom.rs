#[derive(Hash, Eq, PartialEq, Debug)]
pub enum Value {
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    ShortString([u8; 64]),
    Str(String),
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
