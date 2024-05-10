-module(cets_test_peer).
-export([
    start/2,
    stop/1,
    node_to_peer/1
]).

-export([
    block_node/2,
    reconnect_node/2,
    disconnect_node/2,
    disconnect_node_by_name/2
]).

-import(cets_test_rpc, [rpc/4]).

-include_lib("common_test/include/ct.hrl").

start(Names, Config) ->
    {Nodes, Peers} = lists:unzip([start_node(N) || N <- Names]),
    [
        {nodes, maps:from_list(lists:zip(Names, Nodes))},
        {peers, maps:from_list(lists:zip(Names, Peers))}
        | Config
    ].

stop(Config) ->
    Peers = proplists:get_value(peers, Config),
    [peer:stop(Peer) || Peer <- maps:values(Peers)],
    ok.

name(Node) ->
    list_to_atom(peer:random_name(atom_to_list(Node))).

start_node(Id) ->
    {ok, Peer, Node} = ?CT_PEER(#{
        name => name(Id),
        connection => standard_io,
        args => extra_args(Id),
        shutdown => 3000
    }),
    %% Register so we can find Peer process later in code
    register(node_to_peer_name(Node), Peer),
    %% Keep nodes running after init_per_suite is finished
    unlink(Peer),
    %% Do RPC using alternative connection method
    ok = peer:call(Peer, code, add_paths, [code:get_path()]),
    {Node, Peer}.

%% Returns Peer or Node name which could be used to do RPC-s reliably
%% (regardless if Erlang Distribution works or not)
node_to_peer(Node) when Node =:= node() ->
    %% There is no peer for the local CT node
    Node;
node_to_peer(Node) when is_atom(Node) ->
    case whereis(node_to_peer_name(Node)) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            ct:fail({node_to_peer_failed, Node})
    end.

node_to_peer_name(Node) ->
    list_to_atom(atom_to_list(Node) ++ "_peer").

%% Set epmd_port for better coverage
extra_args(ct2) ->
    ["-epmd_port", "4369"];
extra_args(X) when X == ct5; X == ct6; X == ct7 ->
    ["-kernel", "prevent_overlapping_partitions", "false"];
extra_args(_) ->
    "".

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
