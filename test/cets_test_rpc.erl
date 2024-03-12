-module(cets_test_rpc).
-export([
    rpc/4,
    insert/3,
    insert_many/3,
    delete/3,
    delete_request/3,
    delete_many/3,
    dump/2,
    other_nodes/2,
    join/4
]).

%% Apply function using rpc or peer module
rpc(Peer, M, F, Args) when is_pid(Peer) ->
    case peer:call(Peer, M, F, Args) of
        {badrpc, Error} ->
            ct:fail({badrpc, Error});
        Other ->
            Other
    end;
rpc(Node, M, F, Args) when is_atom(Node) ->
    case rpc:call(Node, M, F, Args) of
        {badrpc, Error} ->
            ct:fail({badrpc, Error});
        Other ->
            Other
    end.

insert(Node, Tab, Rec) ->
    rpc(Node, cets, insert, [Tab, Rec]).

insert_many(Node, Tab, Records) ->
    rpc(Node, cets, insert_many, [Tab, Records]).

delete(Node, Tab, Key) ->
    rpc(Node, cets, delete, [Tab, Key]).

delete_request(Node, Tab, Key) ->
    rpc(Node, cets, delete_request, [Tab, Key]).

delete_many(Node, Tab, Keys) ->
    rpc(Node, cets, delete_many, [Tab, Keys]).

dump(Node, Tab) ->
    rpc(Node, cets, dump, [Tab]).

other_nodes(Node, Tab) ->
    rpc(Node, cets, other_nodes, [Tab]).

join(Node1, Tab, Pid1, Pid2) ->
    rpc(Node1, cets_join, join, [lock1, #{table => Tab}, Pid1, Pid2]).
