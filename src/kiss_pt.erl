-module(kiss_pt).
-export([put/2, get/1]).

-compile({no_auto_import,[get/1]}).

%% Avoids GC
put(Tab, Data) ->
    try get(Tab) of
        Data ->
            ok;
        _ ->
            just_put(Tab, Data)
    catch _:_ ->
        just_put(Tab, Data)
    end.

just_put(Tab, Data) ->
    NextKey = next_key(Tab),
    persistent_term:put(NextKey, Data),
    persistent_term:put(Tab, NextKey).

get(Tab) ->
    Key = persistent_term:get(Tab),
    persistent_term:get(Key).

next_key(Tab) ->
    try
        Key = persistent_term:get(Tab),
        N = list_to_integer(get_suffix(atom_to_list(Key), atom_to_list(Tab) ++ "_")) + 1,
        list_to_atom(atom_to_list(Tab) ++ "_" ++ integer_to_list(N))
    catch _:_ ->
        list_to_atom(atom_to_list(Tab) ++ "_1")
    end.

get_suffix(Str, Prefix) ->
    lists:sublist(Str, length(Prefix) + 1, length(Str) - length(Prefix)).
