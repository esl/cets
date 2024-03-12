-module(cets_test_helper).
-include_lib("eunit/include/eunit.hrl").

-export([
    get_disco_timestamp/3,
    assert_unique/1
]).

-export([
    set_nodedown_timestamp/3,
    set_other_servers/2,
    set_join_ref/2
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

set_other_servers(Pid, Servers) ->
    sys:replace_state(Pid, fun(#{other_servers := _} = State) ->
        State#{other_servers := Servers}
    end).

set_join_ref(Pid, JoinRef) ->
    sys:replace_state(Pid, fun(#{join_ref := _} = State) -> State#{join_ref := JoinRef} end).
