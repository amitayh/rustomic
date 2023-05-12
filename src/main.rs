use std::collections::HashSet;

#[derive(Hash, Eq, Debug)]
enum DatomValue {
    I32(i32),
    // I64(i64),
    // U32(u32),
    // U64(u64),
    // Str(String),
}

#[derive(Hash, Eq, Debug)]
enum DatomOp {
    Added,
    Retracted
}

#[derive(Hash, Eq, Debug)]
struct Datom {
    e: u64,
    a: u64,
    v: DatomValue,
    tx: u64,
    op: DatomOp,
}

fn main() {
    let mut db = HashSet::new();
    db.insert(Datom {
        e: 1,
        a: 1,
        v: DatomValue::I32(1),
        tx: 1,
        op: DatomOp::Added
    });

    println!("Hello, world!");
}
