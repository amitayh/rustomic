# Rustomic ![build status](https://github.com/amitayh/rustomic/actions/workflows/rust.yml/badge.svg)

A simplified [Datomic](https://www.datomic.com/) clone built in Rust.

*This project is for educational purposes and should not be used.*

## What is this?

This is a side project I've been working on, trying to mimic Datomic's functionality using Rust.
I'm a complete novice with Rust, so I wanted to try learn it with something that feels real: dependencies, tests, etc.

### Why Datomic?

 * Datomic has a well defined and documented API, which can guide me.
 * Datomic is closed source, so I don't know the implementatoin details of the real product.
 * Datomic is a database - meaning it has to deal with a lot of real-world complexity (I/O, concurrency).
 * Datomic (and datalog) introduced very different and interesting conpects compared to traditional databases.
 * This project can grow in complexity as much as I want to, depending on what I'll end up implementing.
 * Challenging myself to translate an API initially designed for a dynamic language (Clojure) to a staticly typed language.
