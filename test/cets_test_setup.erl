-module(cets_test_setup).
-export([
    mock_epmd/0,
    mock_pause_on_remote_node_failing/0
]).

-export([
    start_local/1,
    start_local/2,
    start/2,
    start_disco/2,
    start_simple_disco/0
]).

-export([
    init_cleanup_table/0,
    wait_for_cleanup/0
]).

-import(cets_test_rpc, [rpc/4]).

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
        timer:sleep(infinity)
    end).

%% schedule_cleanup is async, so this function is waiting for it to finish
wait_for_cleanup() ->
    [
        cets_test_wait:wait_for_down(Cleaner)
     || {Owner, Cleaner} <- ets:tab2list(cleanup_table), not is_process_alive(Owner)
    ].
