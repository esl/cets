-module(cets_test_helper).
-export([get_disco_timestamp/3]).

get_disco_timestamp(Disco, MapName, NodeKey) ->
    Info = cets_discovery:system_info(Disco),
    #{MapName := #{NodeKey := Timestamp}} = Info,
    Timestamp.
