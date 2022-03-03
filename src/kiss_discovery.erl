%% Joins table together when a new node appears
-module(kiss_discovery).
-export([start/1, start_link/1, add_table/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-behaviour(gen_server).

-type backend_state() :: term().
-type get_nodes_result() :: {ok, [node()]} | {error, term()}.

-callback init(map()) -> backend_state().
-callback get_nodes(backend_state()) -> {get_nodes_result(), backend_state()}.

start(Opts) ->
    start_common(start, Opts).

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

add_table(Server, Table) ->
    gen_server:call(Server, {add_table, Table}).

init(Opts) ->
    Mod = maps:get(backend_module, Opts, kiss_discovery_file),
    self() ! check,
    Tables = maps:get(tables, Opts, []),
    BackendState = Mod:init(Opts),
    {ok, #{results => [], tables => Tables,
           backend_module => Mod, backend_state => BackendState}}.

handle_call({add_table, Table}, _From, State = #{tables := Tables}) ->
    case lists:member(Table, Tables) of
        true ->
            {reply, {error, already_added}, State};
        false ->
            State2 = State#{tables => [Table | Tables]},
            {reply, ok, handle_check(State2)}
    end;
handle_call(_Reply, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, State) ->
    {noreply, handle_check(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_check(State = #{tables := []}) ->
    %% No tables to track, skip
    schedule_check(State);
handle_check(State = #{backend_module := Mod, backend_state := BackendState}) ->
    {Res, BackendState2} = Mod:get_nodes(BackendState),
    State2 = handle_get_nodes_result(Res, State),
    schedule_check(State2#{backend_state => BackendState2}).

handle_get_nodes_result({error, _Reason}, State) ->
    State;
handle_get_nodes_result({ok, Nodes}, State = #{tables := Tables}) ->
    Results = [do_join(Tab, Node) || Tab <- Tables, Node <- Nodes, node() =/= Node],
    report_results(Results, State),
    State#{results => Results}.

schedule_check(State) ->
    cancel_old_timer(State),
    TimerRef = erlang:send_after(5000, self(), check),
    State#{timer_ref => TimerRef}.

cancel_old_timer(#{timer_ref := OldRef}) ->
    erlang:cancel_timer(OldRef);
cancel_old_timer(_State) ->
    ok.

do_join(Tab, Node) ->
    %% That would trigger autoconnect for the first time
    case rpc:call(Node, erlang, whereis, [Tab]) of
        Pid when is_pid(Pid) ->
            Result = kiss:join(kiss_discovery, Tab, Pid),
            #{what => join_result, result => Result, node => Node, table => Tab};
        Other ->
            #{what => pid_not_found, reason => Other, node => Node, table => Tab}
    end.

report_results(Results, _State = #{results := OldResults}) ->
    Changed = Results -- OldResults,
    [report_result(Result) || Result <- Changed].

report_result(Map) ->
    Text = [io_lib:format("~0p=~0p ", [K, V]) || {K, V} <- maps:to_list(Map)],
    error_logger:info_msg("discovery ~s", [Text]).
