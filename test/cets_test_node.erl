-module(cets_test_node).
-export([
    block_node/2,
    reconnect_node/2,
    disconnect_node/2,
    disconnect_node_by_name/2
]).

-import(cets_test_rpc, [rpc/4]).

%% Disconnect node until manually connected
block_node(Node, Peer) when is_atom(Node), is_pid(Peer) ->
    rpc(Peer, erlang, set_cookie, [node(), invalid_cookie]),
    disconnect_node(Peer, node()),
    %% Wait till node() is notified about the disconnect
    cets_test_wait:wait_until(fun() -> rpc(Peer, net_adm, ping, [node()]) end, pang),
    cets_test_wait:wait_until(fun() -> rpc(node(), net_adm, ping, [Node]) end, pang).

reconnect_node(Node, Peer) when is_atom(Node), is_pid(Peer) ->
    rpc(Peer, erlang, set_cookie, [node(), erlang:get_cookie()]),
    %% Very rarely it could return pang
    cets_test_wait:wait_until(fun() -> rpc(Peer, net_adm, ping, [node()]) end, pong),
    cets_test_wait:wait_until(fun() -> rpc(node(), net_adm, ping, [Node]) end, pong).

disconnect_node(RPCNode, DisconnectNode) ->
    rpc(RPCNode, erlang, disconnect_node, [DisconnectNode]).

disconnect_node_by_name(Config, Id) ->
    Peer = maps:get(Id, proplists:get_value(peers, Config)),
    Node = maps:get(Id, proplists:get_value(nodes, Config)),
    %% We could need to retry to disconnect, if the local node is currently trying to establish a connection
    %% with Node2 (could be triggered by the previous tests)
    F = fun() ->
        disconnect_node(Peer, node()),
        lists:member(Node, nodes())
    end,
    cets_test_wait:wait_until(F, false).
