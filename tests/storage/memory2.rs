extern crate rustomic;

use rustomic::datom::*;
use rustomic::query::clause::*;
use rustomic::query::pattern::*;
use rustomic::storage::memory2::*;
use rustomic::storage::*;

fn create_storage() -> InMemoryStorage {
    InMemoryStorage::new()
}

#[test]
fn return_empty_result_if_no_datoms_match_search_criteria() {
    let storage = create_storage();

    let entity = 100;
    let clause = Clause::new().with_entity(EntityPattern::Id(entity));
    let read_result = storage.find(&clause);

    assert!(read_result.is_ok());
    assert!(read_result.unwrap().collect::<Vec<Datom>>().is_empty());
}
