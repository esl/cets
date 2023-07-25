-module(cets_metadata).
-export([init/1,
         set/3,
         get/2]).

init(Name) ->
    ets:new(name(Name), [named_table, public, {read_concurrency, true}]).

set(Name, K, V) ->
    ets:insert(name(Name), {K, V}).

get(Name, K) ->
    ets:lookup_element(name(Name), K, 2).

name(Name) ->
    list_to_atom("md_" ++ atom_to_list(Name)).
