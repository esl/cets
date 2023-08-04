-module(cets_metadata).
-export([init/1,
         set/3,
         get/2]).


init(Name) ->
    FullName = name(Name),
    FullName = ets:new(FullName, [named_table, public, {read_concurrency, true}]),
    ok.


set(Name, K, V) ->
    ets:insert(name(Name), {K, V}).


get(Name, K) ->
    ets:lookup_element(name(Name), K, 2).


name(Name) ->
    list_to_atom("md_" ++ atom_to_list(Name)).
