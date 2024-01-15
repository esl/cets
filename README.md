# Cluster ETS

[![](https://github.com/esl/cets/workflows/CI/badge.svg)](https://github.com/esl/cets/actions?query=workflow%3ACI)
[![Hex Package](http://img.shields.io/hexpm/v/cets.svg)](https://hex.pm/packages/cets)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/cets/)
[![codecov](https://codecov.io/github/esl/cets/graph/badge.svg?token=R1zXAjO7H7)](https://codecov.io/github/esl/cets)

The project adds replication support for Erlang Term Storage (ETS).

It allows to insert or delete objects into ETS tables across several Erlang nodes.

The closest comparison is Mnesia with dirty operations.

Some features are not supported:
- there is no schema
- there is no persistency
- there are no indexes
- there are no transactions (there are bulk inserts/deletes instead).

Developed by [Erlang Solutions](https://www.erlang-solutions.com/) to be used in [MongooseIM](https://github.com/esl/MongooseIM/).
It could be used in other Erlang or Elixir projects.

# Merging logic

When two database partitions are joined together, records from both partitions
are put together. So, each record would be present on each node.

The idea that each node updates only records that it owns. The node name
should be inside an ETS key for this to work (or a pid).

When some node is down, we remove all records that are owned by this node.
When a node reappears, the records are added back.

You can also use totally random keys. In this case all keys should be unique.
We also support tables with type=bag. In such case all records would remain in the table.

You can provide the `handle_conflict` function, which would be called if two records have
the same key when clustering (just be aware, this function would not protect you against
overwrites by the same node).

Without `handle_conflict`, conflicting records would overwrite each other when joining.
When joining nodes we insert all data from one node to another and vice versa without any extra check.
Check `join_works_with_existing_data_with_conflicts` test case as an example.

# API

The main module is cets.

It exports functions:

- `start(Tab, Opts)` - starts a new table manager.
   There is one gen_server for each node for each table.
- `insert(Server, Rec)` - inserts a new object into a table.
- `insert_many(Server, Records)` - inserts several objects into a table.
- `delete(Server, Key)` - deletes an object from the table.
- `delete_many(Server, Keys)` - deletes several objects from the table.

`cets_join` module contains the merging logic.

`cets_discovery` module handles search of new nodes.

It supports behaviours for different backends.

It defines two callbacks:

- `init/1` - inits the backend.
- `get_nodes/1` - gets a list of alive erlang nodes.

Once new nodes are found, `cets_discovery` calls `cets_join` module to merge two
cluster partitions.

The simplest `cets_discovery` backend is `cets_discovery_file`, which just reads
a file with a list of nodes on each line. This file could be populated by an
external program or by an admin.

# Commands

Before making a new pull request run tests:

```
rebar3 all
```
