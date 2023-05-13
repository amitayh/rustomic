use datom;

pub fn create_data() {
    let mut data = Vec::new();
    data.insert(
        datom::Datom {
            entity: 1,
            attribute: 2,
            value: datom::Value::I32(1),
            tx: 1,
            op: datom::Op::Added
        }
        );
    return data ;
}

