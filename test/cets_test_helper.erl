-module(cets_test_helper).
-include_lib("eunit/include/eunit.hrl").

-export([
    get_disco_timestamp/3,
    assert_unique/1
]).

-export([
    set_nodedown_timestamp/3
]).

get_disco_timestamp(Disco, MapName, NodeKey) ->
    Info = cets_discovery:system_info(Disco),
    #{MapName := #{NodeKey := Timestamp}} = Info,
    Timestamp.

%% Fails if List has duplicates
assert_unique(List) ->
    ?assertEqual([], List -- lists:usort(List)),
    List.

%% Overwrites nodedown timestamp for the Node in the discovery server state
set_nodedown_timestamp(Disco, Node, NewTimestamp) ->
    sys:replace_state(Disco, fun(#{nodedown_timestamps := Map} = State) ->
        State#{nodedown_timestamps := maps:put(Node, NewTimestamp, Map)}
    end).
