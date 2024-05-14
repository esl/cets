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
    {Nodes, Peers} = lists:unzip([find_or_start_node(N) || N <- Names]),
    [
        {nodes, maps:from_list(lists:zip(Names, Nodes))},
        {peers, maps:from_list(lists:zip(Names, Peers))}
        | Config
    ].

stop(Config) ->
    %% peer:stop/1 freezes in the code cover logic.
    %% So, we reuse nodes between different suites.
    %% Ensure that the nodes are connected again.
    Nodes = proplists:get_value(nodes, Config),
    [
        reconnect_node(Node, node_to_peer(Node))
     || Node <- maps:values(Nodes)
    ],
    ok.

name(Node) ->
    list_to_atom(peer:random_name(atom_to_list(Node))).

find_or_start_node(Id) ->
    case persistent_term:get({id_to_node_peer, Id}, undefined) of
        undefined ->
            start_node(Id);
        NodePeer ->
            NodePeer
    end.

start_node(Id) ->
    {ok, Peer, Node} = ?CT_PEER(#{
        name => name(Id),
        connection => standard_io,
        args => extra_args(Id),
        shutdown => 3000
    }),
    %% Register so we can find Peer process later in code
    persistent_term:put({node_to_peer, Node}, Peer),
    persistent_term:put({id_to_node_peer, Id}, {Node, Peer}),
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
    case persistent_term:get({node_to_peer, Node}) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            ct:fail({node_to_peer_failed, Node})
    end.

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
    erlang:set_cookie(Node, erlang:get_cookie()),
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
