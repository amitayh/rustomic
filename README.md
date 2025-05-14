# Rustomic ![build status](https://github.com/amitayh/rustomic/actions/workflows/rust.yml/badge.svg)

[[Docs](https://amitayh.github.io/rustomic/rustomic/index.html)]

A simplified [Datomic](https://www.datomic.com/) clone built in Rust.

*This project is for educational purposes and should not be used.*

## What is this?

This is a side project I've been working on, trying to mimic Datomic's functionality using Rust.
I'm a complete novice with Rust, so I wanted to try learn it with something that feels real:
dependencies, tests, etc.

### Why Datomic?

 * Datomic has a well defined and documented API, which can guide me.
 * Datomic is closed source, so I don't know the implementatoin details of the real product.
 * Datomic is a database - meaning it has to deal with a lot of real-world complexity (I/O,
   concurrency).
 * Datomic (and datalog) introduced very different and interesting conpects compared to traditional
   databases.
 * This project can grow in complexity as much as I want to, depending on what I'll end up
   implementing.
 * Challenging myself to translate an API initially designed for a dynamic language (Clojure) to a
   staticly typed language.

## Query Engine

The project implements a Datalog-style query engine that supports:

### Core Features

* Pattern matching against the database using entity-attribute-value-transaction patterns
* Variable binding and resolution across multiple clauses
* Custom predicate filtering
* Aggregation support (count, min, max, sum, average, count-distinct)
* Attribute name resolution
* Streaming result processing
* Early filtering through predicate evaluation

### Example Query

```rust
// Find all people born after 1980
let query = Query::new()
    .find(Find::variable("?person"))
    .where(Clause::new()
        .with_entity(Pattern::variable("?person"))
        .with_attribute(Pattern::ident("person/born"))
        .with_value(Pattern::variable("?born")))
    .value_pred(
        "?born",
        |value| matches!(value, &Value::I64(born) if born > 1980),
    );
```

## Transactor

The transactor is a core component that handles all data modifications in the database. It
implements Datomic's transactional model with ACID guarantees.

### Features

* **Entity Operations**
  - Create new entities with auto-generated IDs
  - Update existing entities by ID
  - Support for temporary IDs within transactions
  - Assert or retract attribute values

* **Transaction Processing**
  - Atomic execution of multiple operations
  - Automatic timestamp recording for each transaction
  - Value type validation against schema
  - Uniqueness constraints enforcement
  - Cardinality handling (one vs. many)

### Example Transaction

```rust
let tx = Transaction::new()
    .with(EntityOperation::on_new()
        .assert("person/name", "John Doe")
        .assert("person/age", 30))
    .with(EntityOperation::on_temp_id("employee-1")
        .assert("employee/role", "Developer")
        .assert("employee/start-date", "2024-01-01"));
```
