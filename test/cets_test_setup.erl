-module(cets_test_setup).
-export([suite/0]).

-export([
    mock_epmd/0,
    mock_pause_on_remote_node_failing/0
]).

-export([
    init_cleanup_table/0,
    remove_cleanup_table/0,
    wait_for_cleanup/0
]).

-export([
    start_local/1,
    start_local/2,
    start_link_local/1,
    start_link_local/2,
    start/2,
    start_disco/2,
    start_simple_disco/0
]).

-export([
    make_name/1,
    make_name/2,
    lock_name/1,
    disco_name/1
]).

-export([
    given_two_joined_tables/1,
    given_two_joined_tables/2,
    given_3_servers/1,
    given_3_servers/2,
    given_n_servers/3,
    setup_two_nodes_and_discovery/1,
    setup_two_nodes_and_discovery/2
]).

-export([simulate_disco_restart/1]).

-export([
    make_signalling_process/0,
    make_process/0
]).

-import(cets_test_peer, [
    disconnect_node/2,
    disconnect_node_by_name/2
]).

-import(cets_test_rpc, [rpc/4]).

suite() ->
    [{timetrap, {seconds, 10}}].

mock_epmd() ->
    meck:new(erl_epmd, [passthrough, unstick]),
    meck:expect(erl_epmd, address_please, fun
        ("cetsnode1", "localhost", inet) -> {ok, {192, 168, 100, 134}};
        (Name, Host, Family) -> meck:passthrough([Name, Host, Family])
    end).

mock_pause_on_remote_node_failing() ->
    meck:new(cets_join, [passthrough, no_link]),
    meck:expect(cets_join, pause_on_remote_node, fun(_JoinerPid, _AllPids) ->
        error(mock_pause_on_remote_node_failing)
    end),
    ok.

start_local(Name) ->
    start_local(Name, #{}).

start_local(Name, Opts) ->
    catch cets:stop(Name),
    cets_test_wait:wait_for_name_to_be_free(node(), Name),
    {ok, Pid} = cets:start(Name, Opts),
    schedule_cleanup(Pid),
    {ok, Pid}.

start(Node, Tab) ->
    catch rpc(Node, cets, stop, [Tab]),
    cets_test_wait:wait_for_name_to_be_free(Node, Tab),
    {ok, Pid} = rpc(Node, cets, start, [Tab, #{}]),
    schedule_cleanup(Pid),
    {ok, Pid}.

start_link_local(Name) ->
    start_link_local(Name, #{}).

start_link_local(Name, Opts) ->
    catch cets:stop(Name),
    cets_test_wait:wait_for_name_to_be_free(node(), Name),
    {ok, Pid} = cets:start_link(Name, Opts),
    schedule_cleanup(Pid),
    {ok, Pid}.

start_disco(Node, Opts) ->
    case Opts of
        #{name := Name} ->
            catch rpc(Node, cets, stop, [Name]),
            cets_test_wait:wait_for_name_to_be_free(Node, Name);
        _ ->
            ok
    end,
    {ok, Pid} = rpc(Node, cets_discovery, start, [Opts]),
    schedule_cleanup(Pid),
    Pid.

start_simple_disco() ->
    F = fun(State) ->
        {{ok, []}, State}
    end,
    {ok, Pid} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    Pid.

schedule_cleanup(Pid) ->
    Me = self(),
    Cleaner = proc_lib:spawn(fun() ->
        Ref = erlang:monitor(process, Me),
        receive
            {'DOWN', Ref, process, Me, _} ->
                %% We do an RPC call, because erlang distribution
                %% could not be always reliable (because we test netsplits)
                rpc(cets_test_peer:node_to_peer(node(Pid)), cets, stop, [Pid]),
                ets:delete_object(cleanup_table, {Me, self()})
        end
    end),
    ets:insert(cleanup_table, {Me, Cleaner}).

init_cleanup_table() ->
    spawn(fun() ->
        ets:new(cleanup_table, [named_table, public, bag]),
        receive
            stop -> ok
        end
    end).

remove_cleanup_table() ->
    ets:info(cleanup_table, owner) ! stop.

%% schedule_cleanup is async, so this function is waiting for it to finish
wait_for_cleanup() ->
    [
        cets_test_wait:wait_for_down(Cleaner)
     || {Owner, Cleaner} <- ets:tab2list(cleanup_table), not is_process_alive(Owner)
    ].

make_name(Config) ->
    make_name(Config, 1).

make_name(Config, Num) when is_integer(Num) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_" ++ integer_to_list(Num));
make_name(Config, Atom) when is_atom(Atom) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_" ++ atom_to_list(Atom)).

lock_name(Config) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_lock").

disco_name(Config) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_disco").

given_two_joined_tables(Config) ->
    given_two_joined_tables(Config, #{}).

given_two_joined_tables(Config, Opts) ->
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, Pid1} = start_local(Tab1, Opts),
    {ok, Pid2} = start_local(Tab2, Opts),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    #{
        tab1 => Tab1,
        tab2 => Tab2,
        pid1 => Pid1,
        pid2 => Pid2,
        tabs => [Tab1, Tab2],
        pids => [Pid1, Pid2]
    }.

given_3_servers(Config) ->
    given_3_servers(Config, #{}).

given_3_servers(Config, Opts) ->
    given_n_servers(Config, 3, Opts).

given_n_servers(Config, N, Opts) ->
    Tabs = [make_name(Config, X) || X <- lists:seq(1, N)],
    Pids = [
        begin
            {ok, Pid} = start_local(Tab, Opts),
            Pid
        end
     || Tab <- Tabs
    ],
    #{pids => Pids, tabs => Tabs}.

setup_two_nodes_and_discovery(Config) ->
    setup_two_nodes_and_discovery(Config, []).

%% Flags:
%% - disco2 - start discovery on Node2
%% - wait - call wait_for_ready/2
setup_two_nodes_and_discovery(Config, Flags) ->
    Me = self(),
    Node1 = node(),
    #{ct2 := Peer2} = proplists:get_value(peers, Config),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    disconnect_node_by_name(Config, ct2),
    Tab = make_name(Config),
    {ok, _Pid1} = start(Node1, Tab),
    {ok, _Pid2} = start(Peer2, Tab),
    F = fun(State) ->
        case lists:member(notify_get_nodes, Flags) of
            true ->
                Me ! get_nodes;
            false ->
                ok
        end,
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    DiscoOpts = #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    },
    Disco = start_disco(Node1, DiscoOpts),
    %% Start Disco on second node (it is not always needed)
    Res =
        case lists:member(disco2, Flags) of
            true ->
                Disco2 = start_disco(Node2, DiscoOpts),
                cets_discovery:add_table(Disco2, Tab),
                #{disco2 => Disco2};
            false ->
                #{}
        end,
    cets_discovery:add_table(Disco, Tab),
    case lists:member(wait, Flags) of
        true ->
            cets_test_wait:wait_for_ready(Disco, 5000);
        false ->
            ok
    end,
    case lists:member(netsplit, Flags) of
        true ->
            %% Simulate a loss of connection between nodes
            disconnect_node_by_name(Config, ct2);
        false ->
            ok
    end,
    Res#{
        disco_name => DiscoName,
        disco_opts => DiscoOpts,
        disco => Disco,
        node1 => Node1,
        node2 => Node2,
        peer2 => Peer2
    }.

simulate_disco_restart(#{
    disco_opts := DiscoOpts,
    disco2 := Disco2,
    node1 := Node1,
    node2 := Node2,
    peer2 := Peer2
}) ->
    %% Instead of restart the node, restart the process. It is enough to get
    %% a new start_time.
    disconnect_node(Peer2, Node1),
    rpc(Peer2, cets, stop, [Disco2]),
    %% We actually would not detect the case of us just stopping the remote disco
    %% server. Because we use nodeup/nodedown to detect downs, not monitors.
    _RestartedDisco2 = start_disco(Node2, DiscoOpts).

make_signalling_process() ->
    proc_lib:spawn_link(fun() ->
        receive
            stop -> ok
        end
    end).

make_process() ->
    proc_lib:spawn(fun() ->
        receive
            stop -> stop
        end
    end).
