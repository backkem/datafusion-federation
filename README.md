### DataFusion Federation

The goal of this repo is to allow [DataFusion](https://github.com/apache/arrow-datafusion) to resolve queries across remote query engines while pushing down as much compute as possible down.

Potential use-cases:

- Querying across SQLite, MySQL, PostgreSQL, ...
- Pushing down SQL or [Substrait](https://substrait.io/) plans.
- DataFusion -> Flight SQL -> DataFusion
- ..

#### Status

The project is an early WIP. Contributions welcome; land a PR = commit access.
