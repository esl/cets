-module(cets_test_peer).
-export([
    start_node/1,
    node_to_peer/1
]).
-include_lib("common_test/include/ct.hrl").

start_node(Sname) ->
    {ok, Peer, Node} = ?CT_PEER(#{
        name => Sname, connection => standard_io, args => extra_args(Sname)
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
