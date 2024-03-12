-module(cets_test_helper).
-include_lib("eunit/include/eunit.hrl").

-export([
    get_disco_timestamp/3,
    assert_unique/1
]).

get_disco_timestamp(Disco, MapName, NodeKey) ->
    Info = cets_discovery:system_info(Disco),
    #{MapName := #{NodeKey := Timestamp}} = Info,
    Timestamp.

%% Fails if List has duplicates
assert_unique(List) ->
    ?assertEqual([], List -- lists:usort(List)),
    List.
