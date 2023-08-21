use crate::query::pattern::EntityPattern;
use crate::storage::*;

pub struct DiskStorage {
    db: rocksdb::DB,
}

fn print_bytes(bytes: &[u8]) {
    let string = bytes
        .iter()
        .map(|byte| format!("{:02X}", byte))
        .collect::<Vec<String>>()
        .join(" ");
    println!("@@@ {}", string);
}

impl Storage for DiskStorage {
    fn save(&mut self, datoms: &[Datom]) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        for datom in datoms {
            batch.put(serde::datom::serialize::eavt(datom), "");
            //batch.put(datom.encode_aevt(), "");
            //batch.put(datom.encode_avet(), "");
        }
        self.db.write(batch).unwrap();
        Ok(())
    }

    fn find_datoms(&self, clause: &Clause, _tx_range: u64) -> Result<Vec<Datom>, StorageError> {
        let mut result = Vec::new();

        let mut read_options = rocksdb::ReadOptions::default();

        let mut lower = Vec::with_capacity(9);
        //lower.push(index::TAG_EAVT);
        let mut upper = Vec::with_capacity(9);
        //upper.push(index::TAG_EAVT);
        if let EntityPattern::Id(entity) = clause.entity {
            lower.extend_from_slice(&entity.to_be_bytes());
            upper.extend_from_slice(&(entity + 1).to_be_bytes());
        }
        read_options.set_iterate_lower_bound(lower);
        read_options.set_iterate_upper_bound(upper);

        for item in self
            .db
            .iterator_opt(rocksdb::IteratorMode::Start, read_options)
        {
            let (key, _value) = item.unwrap();
            //print_bytes(&key);
            //print_bytes(&value);
            //let datom = Datom::parse(&key).unwrap();
            let datom = serde::datom::deserialize(&key).unwrap();
            dbg!(&datom);
            result.push(datom);
        }
        Ok(result)
    }

    fn resolve_ident(&self, _ident: &str) -> Result<EntityId, StorageError> {
        todo!()
    }
}

impl DiskStorage {
    pub fn new(db: rocksdb::DB) -> Self {
        DiskStorage { db }
    }
}
