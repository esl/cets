%% @doc A helper module for fast access for some server metadata.
-module(cets_metadata).
-export([
    init/1,
    set/3,
    get/2
]).

%% @doc Create a table for fast access.
init(Name) ->
    FullName = name(Name),
    FullName = ets:new(FullName, [named_table, public, {read_concurrency, true}]),
    ok.

%% @doc Sets metadata.
set(Name, K, V) ->
    ets:insert(name(Name), {K, V}).

%% @doc Reads metadata.
get(Name, K) ->
    ets:lookup_element(name(Name), K, 2).

name(Name) ->
    list_to_atom("md_" ++ atom_to_list(Name)).
