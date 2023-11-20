%% @doc Node discovery logic
%% Joins table together when a new node appears
%%
%% Things that make discovery logic harder:
%% - A table list is dynamic (but eventually we add all the tables into it)
%% - Creating Erlang distribution connection is async, but it net_kernel:ping/1 is blocking
%% - net_kernel:ping/1 could block for unknown number of seconds
%%   (but net_kernel default timeout is 7 seconds)
%% - Resolving nodename could take a lot of time (5 seconds in tests).
%%   It is unpredictable blocking.
%% - join tables should be one by one to avoid OOM.
%% - Backend:get_nodes/1 could take a long time.
%% - cets_discovery:get_tables/1, cets_discovery:add_table/2 should be fast.
%% - The most important net_kernel flags for us to consider are:
%%   - dist_auto_connect=never
%%   - connect_all
%%   - prevent_overlapping_partitions
%% These flags change the way the discovery logic behaves.
%% Also the module would not try to connect to the hidden nodes.
%%
%% Retry logic considerations:
%% - Backend:get_nodes/1 could return an error during startup, so we have to retry fast.
%% - There are two periods of operation for this module:
%%   - startup phase, usually first 5 minutes.
%%   - regular operation phase, after the startup phase.
%% - We don't need to check for the updated get_nodes too often in the regular operation phase.
-module(cets_discovery).
-behaviour(gen_server).

-export([
    start/1,
    start_link/1,
    add_table/2,
    delete_table/2,
    get_tables/1,
    info/1,
    system_info/1,
    wait_for_ready/2
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ignore_xref([
    start/1,
    start_link/1,
    add_table/2,
    delete_table/2,
    get_tables/1,
    info/1,
    system_info/1,
    wait_for_ready/2,
    behaviour_info/1
]).

-include_lib("kernel/include/logger.hrl").

-type backend_state() :: term().
-type get_nodes_result() :: {ok, [node()]} | {error, term()}.
-type retry_type() :: initial | after_error | regular.

-export_type([get_nodes_result/0, system_info/0]).

-type from() :: {pid(), reference()}.
-type join_result() :: #{
    node := node(),
    table := atom(),
    what := join_result | pid_not_found,
    result => ok | {error, _},
    reason => term()
}.
-type state() :: #{
    phase := initial | regular,
    results := [join_result()],
    nodes := ordsets:ordset(node()),
    %% The nodes that returned pang, sorted
    unavailable_nodes := ordsets:ordset(node()),
    tables := [atom()],
    backend_module := module(),
    backend_state := state(),
    get_nodes_status := not_running | running,
    should_retry_get_nodes := boolean(),
    last_get_nodes_result := not_called_yet | get_nodes_result(),
    last_get_nodes_retry_type := retry_type(),
    join_status := not_running | running,
    should_retry_join := boolean(),
    timer_ref := reference() | undefined,
    pending_wait_for_ready := [gen_server:from()],
    nodeup_timestamps := #{node() => milliseconds()},
    nodedown_timestamps := #{node() => milliseconds()},
    node_start_timestamps := #{node() => milliseconds()},
    start_time := milliseconds()
}.
-type milliseconds() :: integer().

%% Backend could define its own options
-type opts() :: #{name := atom(), _ := _}.
-type start_result() :: {ok, pid()} | {error, term()}.
-type server() :: pid() | atom().
-type system_info() :: map().

-callback init(map()) -> backend_state().
-callback get_nodes(backend_state()) -> {get_nodes_result(), backend_state()}.

-spec start(opts()) -> start_result().
start(Opts) ->
    start_common(start, Opts).

-spec start_link(opts()) -> start_result().
start_link(Opts) ->
    start_common(start_link, Opts).

start_common(F, Opts) ->
    Args =
        case Opts of
            #{name := Name} ->
                [{local, Name}, ?MODULE, Opts, []];
            _ ->
                [?MODULE, Opts, []]
        end,
    apply(gen_server, F, Args).

-spec add_table(server(), cets:table_name()) -> ok.
add_table(Server, Table) ->
    gen_server:cast(Server, {add_table, Table}).

-spec delete_table(server(), cets:table_name()) -> ok.
delete_table(Server, Table) ->
    gen_server:cast(Server, {delete_table, Table}).

-spec get_tables(server()) -> {ok, [cets:table_name()]}.
get_tables(Server) ->
    gen_server:call(Server, get_tables).

-spec info(server()) -> [cets:info()].
info(Server) ->
    {ok, Tables} = get_tables(Server),
    [cets:info(Tab) || Tab <- Tables].

-spec system_info(server()) -> system_info().
system_info(Server) ->
    gen_server:call(Server, system_info).

%% This calls blocks until the initial discovery is done
%% It also waits till the data is loaded from the remote nodes
-spec wait_for_ready(server(), timeout()) -> ok.
wait_for_ready(Server, Timeout) ->
    F = fun() -> gen_server:call(Server, wait_for_ready, Timeout) end,
    Info = #{task => cets_wait_for_ready},
    cets_long:run_tracked(Info, F).

-spec init(term()) -> {ok, state()}.
init(Opts) ->
    StartTime = erlang:system_time(millisecond),
    %% Sends nodeup / nodedown
    ok = net_kernel:monitor_nodes(true),
    Mod = maps:get(backend_module, Opts, cets_discovery_file),
    self() ! check,
    Tables = maps:get(tables, Opts, []),
    BackendState = Mod:init(Opts),
    %% Changes phase from initial to regular (affects the check interval)
    erlang:send_after(timer:minutes(5), self(), enter_regular_phase),
    State = #{
        phase => initial,
        results => [],
        nodes => [],
        unavailable_nodes => [],
        tables => Tables,
        backend_module => Mod,
        backend_state => BackendState,
        get_nodes_status => not_running,
        should_retry_get_nodes => false,
        last_get_nodes_result => not_called_yet,
        last_get_nodes_retry_type => initial,
        join_status => not_running,
        should_retry_join => false,
        timer_ref => undefined,
        pending_wait_for_ready => [],
        nodeup_timestamps => #{},
        node_start_timestamps => #{},
        nodedown_timestamps => #{},
        start_time => StartTime
    },
    %% Set initial timestamps because we would not receive nodeup events for
    %% already connected nodes
    State2 = lists:foldl(fun handle_nodeup/2, State, nodes()),
    {ok, State2}.

-spec handle_call(term(), from(), state()) -> {reply, term(), state()} | {noreply, state()}.
handle_call(get_tables, _From, State = #{tables := Tables}) ->
    {reply, {ok, Tables}, State};
handle_call(system_info, _From, State) ->
    {reply, handle_system_info(State), State};
handle_call(wait_for_ready, From, State = #{pending_wait_for_ready := Pending}) ->
    {noreply, trigger_verify_ready(State#{pending_wait_for_ready := [From | Pending]})};
handle_call(Msg, From, State) ->
    ?LOG_ERROR(#{what => unexpected_call, msg => Msg, from => From}),
    {reply, {error, unexpected_call}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({add_table, Table}, State = #{tables := Tables}) ->
    case lists:member(Table, Tables) of
        true ->
            {noreply, State};
        false ->
            self() ! check,
            State2 = State#{tables := ordsets:add_element(Table, Tables)},
            {noreply, State2}
    end;
handle_cast({delete_table, Table}, State = #{tables := Tables}) ->
    case lists:member(Table, Tables) of
        true ->
            State2 = State#{tables := ordsets:del_element(Table, Tables)},
            {noreply, State2};
        false ->
            {noreply, State}
    end;
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(check, State) ->
    {noreply, handle_check(State)};
handle_info({handle_check_result, Result, BackendState}, State) ->
    {noreply, handle_get_nodes_result(Result, BackendState, State)};
handle_info({nodeup, Node}, State) ->
    State2 = handle_nodeup(Node, State),
    State3 = remove_node_from_unavailable_list(Node, State2),
    {noreply, try_joining(State3)};
handle_info({nodedown, Node}, State) ->
    State2 = handle_nodedown(Node, State),
    %% Do another check to update unavailable_nodes list
    self() ! check,
    {noreply, State2};
handle_info({start_time, Node, StartTime}, State) ->
    {noreply, handle_receive_start_time(Node, StartTime, State)};
handle_info({joining_finished, Results}, State) ->
    {noreply, handle_joining_finished(Results, State)};
handle_info({ping_result, Node, Result}, State) ->
    {noreply, handle_ping_result(Node, Result, State)};
handle_info(enter_regular_phase, State) ->
    {noreply, State#{phase := regular}};
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec handle_check(state()) -> state().
handle_check(State = #{tables := []}) ->
    %% No tables to track, skip
    State;
handle_check(State = #{get_nodes_status := running}) ->
    State#{should_retry_get_nodes := true};
handle_check(State = #{backend_module := Mod, backend_state := BackendState}) ->
    Self = self(),
    spawn_link(fun() ->
        Info = #{task => cets_discovery_get_nodes, backend_module => Mod},
        F = fun() -> Mod:get_nodes(BackendState) end,
        {Result, BackendState2} = cets_long:run_tracked(Info, F),
        Self ! {handle_check_result, Result, BackendState2}
    end),
    State#{get_nodes_status := running}.

-spec handle_get_nodes_result(Result, BackendState, State) -> State when
    Result :: get_nodes_result(), BackendState :: backend_state(), State :: state().
handle_get_nodes_result(Result, BackendState, State) ->
    State2 = State#{
        backend_state := BackendState,
        get_nodes_status := not_running,
        last_get_nodes_result := Result
    },
    State3 = set_nodes(Result, State2),
    schedule_check(trigger_verify_ready(State3)).

-spec set_nodes({error, term()} | {ok, [node()]}, state()) -> state().
set_nodes({error, _Reason}, State) ->
    State;
set_nodes({ok, Nodes}, State) ->
    Nodes2 = lists:usort(Nodes),
    ping_not_connected_nodes(Nodes2),
    prune_unavailable_nodes_if_needed(try_joining(State#{nodes := Nodes2})).

%% Called when:
%% - a list of connected nodes changes (i.e. nodes() call result)
%% - a list of nodes is received from the discovery backend
-spec try_joining(state()) -> state().
try_joining(State = #{join_status := running}) ->
    State#{should_retry_join := true};
try_joining(State = #{join_status := not_running, nodes := Nodes, tables := Tables}) ->
    Self = self(),
    AvailableNodes = nodes(),
    spawn_link(fun() ->
        %% We only care about connected nodes here
        %% We do not want to try to connect here - we do it in ping_not_connected_nodes/1
        Results = [
            do_join(Tab, Node)
         || Node <- Nodes, lists:member(Node, AvailableNodes), Tab <- Tables
        ],
        Self ! {joining_finished, Results}
    end),
    State#{join_status := running, should_retry_join := false}.

%% Called when try_joining finishes the async task
-spec handle_joining_finished(list(), state()) -> state().
handle_joining_finished(Results, State = #{should_retry_join := Retry}) ->
    report_results(Results, State),
    State2 = trigger_verify_ready(State#{results := Results, join_status := not_running}),
    case Retry of
        true ->
            try_joining(State2);
        false ->
            State2
    end.

-spec prune_unavailable_nodes_if_needed(state()) -> state().
prune_unavailable_nodes_if_needed(State = #{nodes := Nodes, unavailable_nodes := UnNodes}) ->
    %% Unavailable nodes is a subset of discovered nodes
    State#{unavailable_nodes := ordsets:intersection(Nodes, UnNodes)}.

-spec ping_not_connected_nodes([node()]) -> ok.
ping_not_connected_nodes(Nodes) ->
    Self = self(),
    NotConNodes = Nodes -- [node() | nodes()],
    [
        spawn(fun() -> Self ! {ping_result, Node, cets_ping:ping(Node)} end)
     || Node <- lists:sort(NotConNodes)
    ],
    ok.

-spec handle_ping_result(node(), pong | pang, state()) -> state().
handle_ping_result(Node, pang, State = #{unavailable_nodes := UnNodes}) ->
    trigger_verify_ready(State#{unavailable_nodes := ordsets:add_element(Node, UnNodes)});
handle_ping_result(_Node, pong, State) ->
    State.

-spec remove_node_from_unavailable_list(node(), state()) -> state().
remove_node_from_unavailable_list(Node, State = #{unavailable_nodes := UnNodes}) ->
    State#{unavailable_nodes := ordsets:del_element(Node, UnNodes)}.

-spec schedule_check(state()) -> state().
schedule_check(State = #{should_retry_get_nodes := true, get_nodes_status := not_running}) ->
    %% Retry without any delay
    self() ! check,
    State#{should_retry_get_nodes := false};
schedule_check(State) ->
    cancel_old_timer(State),
    RetryType = choose_retry_type(State),
    RetryTimeout = retry_type_to_timeout(RetryType),
    TimerRef = erlang:send_after(RetryTimeout, self(), check),
    State#{timer_ref := TimerRef, last_get_nodes_retry_type := RetryType}.

-spec choose_retry_type(state()) -> retry_type().
choose_retry_type(#{last_get_nodes_result := {error, _}}) ->
    after_error;
choose_retry_type(#{phase := initial}) ->
    initial;
choose_retry_type(_) ->
    regular.

%% Returns timeout in milliseconds to retry calling the get_nodes function.
%% get_nodes is called after add_table without waiting.
%% It is also would be retried without waiting if should_retry_get_nodes set to true.
-spec retry_type_to_timeout(retry_type()) -> non_neg_integer().
retry_type_to_timeout(initial) -> timer:seconds(5);
retry_type_to_timeout(after_error) -> timer:seconds(1);
retry_type_to_timeout(regular) -> timer:minutes(5).

-spec cancel_old_timer(state()) -> ok.
cancel_old_timer(#{timer_ref := OldRef}) when is_reference(OldRef) ->
    %% Match result to prevent from Dialyzer warning
    _ = erlang:cancel_timer(OldRef),
    flush_all_checks(),
    ok;
cancel_old_timer(_State) ->
    ok.

flush_all_checks() ->
    receive
        check -> flush_all_checks()
    after 0 -> ok
    end.

-spec do_join(atom(), node()) -> join_result().
do_join(Tab, Node) ->
    LocalPid = whereis(Tab),
    %% That would trigger autoconnect for the first time
    case rpc:call(Node, erlang, whereis, [Tab]) of
        Pid when is_pid(Pid), is_pid(LocalPid) ->
            Result = cets_join:join(cets_discovery, #{table => Tab}, LocalPid, Pid),
            #{what => join_result, result => Result, node => Node, table => Tab};
        Other ->
            #{what => pid_not_found, reason => Other, node => Node, table => Tab}
    end.

-spec report_results([join_result()], state()) -> ok.
report_results(Results, _State = #{results := OldResults}) ->
    Changed = Results -- OldResults,
    lists:foreach(fun report_result/1, Changed),
    ok.

-spec report_result(join_result()) -> ok.
report_result(Map) ->
    ?LOG_INFO(Map).

-spec trigger_verify_ready(state()) -> state().
trigger_verify_ready(State = #{pending_wait_for_ready := []}) ->
    State;
trigger_verify_ready(State = #{pending_wait_for_ready := [_ | _] = Pending}) ->
    case verify_ready(State) of
        [] ->
            [gen_server:reply(From, ok) || From <- Pending],
            State#{pending_wait_for_ready := []};
        _ ->
            State
    end.

%% Returns a list of missing initial tasks
%% When the function returns [], the initial clustering is done
%% (or at least we've tried once and finished all the async tasks)
-spec verify_ready(state()) -> list().
verify_ready(State) ->
    verify_last_get_nodes_result_ok(State) ++
        verify_done_waiting_for_pangs(State) ++
        verify_tried_joining(State).

-spec verify_last_get_nodes_result_ok(state()) ->
    [{bad_last_get_nodes_result, {error, term()} | not_called_yet}].
verify_last_get_nodes_result_ok(#{last_get_nodes_result := {ok, _}}) ->
    [];
verify_last_get_nodes_result_ok(#{last_get_nodes_result := Res}) ->
    [{bad_last_get_nodes_result, Res}].

-spec verify_done_waiting_for_pangs(state()) -> [{still_waiting_for_pangs, [node()]}].
verify_done_waiting_for_pangs(#{nodes := Nodes, unavailable_nodes := UnNodes}) ->
    Expected = lists:sort(Nodes -- [node() | nodes()]),
    case UnNodes of
        Expected ->
            [];
        _ ->
            [{still_waiting_for_pangs, Expected -- UnNodes}]
    end.

-spec verify_tried_joining(state()) ->
    [{waiting_for_join_result, [{Node :: node(), Table :: atom()}]}].
verify_tried_joining(State = #{nodes := Nodes, tables := Tables}) ->
    AvailableNodes = nodes(),
    NodesToJoin = [Node || Node <- Nodes, lists:member(Node, AvailableNodes)],
    Missing = [
        {Node, Table}
     || Node <- NodesToJoin, Table <- Tables, not has_join_result_for(Node, Table, State)
    ],
    case Missing of
        [] -> [];
        _ -> [{waiting_for_join_result, Missing}]
    end.

-spec has_join_result_for(Node :: node(), Table :: atom(), state()) -> boolean().
has_join_result_for(Node, Table, #{results := Results}) ->
    [] =/= [R || R = #{node := N, table := T} <- Results, N =:= Node, T =:= Table].

-spec handle_system_info(state()) -> system_info().
handle_system_info(State) ->
    State#{verify_ready => verify_ready(State)}.

handle_nodedown(Node, State) ->
    State2 = remember_nodedown_timestamp(Node, State),
    {NodeUpTime, State3} = remove_nodeup_timestamp(Node, State2),
    ?LOG_WARNING(
        set_defined(connected_millisecond_duration, NodeUpTime, #{
            what => nodedown,
            remote_node => Node,
            alive_nodes => length(nodes()) + 1,
            time_since_startup_in_milliseconds => time_since_startup_in_milliseconds(State)
        })
    ),
    State3.

handle_nodeup(Node, State) ->
    send_start_time_to(Node, State),
    State2 = remember_nodeup_timestamp(Node, State),
    NodeDownTime = get_downtime(Node, State2),
    ?LOG_WARNING(
        set_defined(downtime_millisecond_duration, NodeDownTime, #{
            what => nodeup,
            remote_node => Node,
            alive_nodes => length(nodes()) + 1,
            %% We report that time so we could work on minimizing that time.
            %% It says how long it took to discover nodes after startup.
            time_since_startup_in_milliseconds => time_since_startup_in_milliseconds(State)
        })
    ),
    State2.

remember_nodeup_timestamp(Node, State = #{nodeup_timestamps := Map}) ->
    Time = erlang:system_time(millisecond),
    Map2 = Map#{Node => Time},
    State#{nodeup_timestamps := Map2}.

remember_nodedown_timestamp(Node, State = #{nodedown_timestamps := Map}) ->
    Time = erlang:system_time(millisecond),
    Map2 = Map#{Node => Time},
    State#{nodedown_timestamps := Map2}.

remove_nodeup_timestamp(Node, State = #{nodeup_timestamps := Map}) ->
    StartTime = maps:get(Node, Map, undefined),
    NodeUpTime = calculate_uptime(StartTime),
    Map2 = maps:remove(Node, State),
    {NodeUpTime, State#{nodeup_timestamps := Map2}}.

calculate_uptime(undefined) ->
    undefined;
calculate_uptime(StartTime) ->
    time_since(StartTime).

get_downtime(Node, #{nodedown_timestamps := Map}) ->
    case maps:get(Node, Map, undefined) of
        undefined ->
            undefined;
        WentDown ->
            time_since(WentDown)
    end.

set_defined(_Key, undefined, Map) ->
    Map;
set_defined(Key, Value, Map) ->
    Map#{Key => Value}.

time_since_startup_in_milliseconds(#{start_time := StartTime}) ->
    time_since(StartTime).

time_since(StartTime) ->
    erlang:system_time(millisecond) - StartTime.

send_start_time_to(Node, #{start_time := StartTime}) ->
    case erlang:process_info(self(), registered_name) of
        {registered_name, Name} ->
            erlang:send({Name, Node}, {start_time, node(), StartTime});
        _ ->
            ok
    end.

handle_receive_start_time(Node, StartTime, State = #{node_start_timestamps := Map}) ->
    case maps:get(Node, Map, undefined) of
        undefined ->
            ok;
        StartTime ->
            ?LOG_WARNING(#{
                what => node_reconnects,
                remote_node => Node,
                start_time => StartTime,
                text => <<"Netsplit recovery. The remote node has been connected to us before.">>
            });
        _ ->
            %% Restarted node reconnected, this is fine during the rolling updates
            ok
    end,
    State#{node_start_timestamps := maps:put(Node, StartTime, Map)}.
