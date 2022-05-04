# Cluster ETS

The project adds replication support for Erlang Term Storage (ETS).

It allows to insert or delete objects into ETS tables across several Erlang nodes.

The closest comparison is Mnesia with dirty operations.

Some features are not supported:
- there is no schema
- there is no persistency
- there are no indexes
- there are no transactions (there are bulk inserts/deletes instead).

# Merging logic

When two database partitions are joined together, records from both partitions
are put together. So, each record would be present on each node.

The idea that each node updates only records that it owns. The node name
should be inside an ETS key for this to work (or a pid).

When some node is down, we remove all records that are owned by this node.
When a node reappears, the records are added back.

# API

The main module is cets.

It exports functions:

- `start(Tab, Opts)` - starts a new table manager.
   There is one gen_server for each node for each table.
- `insert(Server, Rec)` - inserts a new object into a table.
- `insert_many(Server, Records)` - inserts several objects into a table.
- `delete(Server, Key)` - deletes an object from the table.
- `delete_many(Server, Keys)` - deletes several objects from the table.

cets_join module contains the merging logic.

cets_discovery module handles search of new nodes.

It supports behaviours for different backends.

It defines two callbacks:

- `init/1` - inits the backend.
- `get_nodes/1` - gets a list of alive erlang nodes.

Once new nodes are found, cets_discovery calls cets_join module to merge two
cluster partitions.

The simplest cets_discovery backend is cets_discovery_file, which just reads
a file with a list of nodes on each line. This file could be populated by an
external program or by an admin.
