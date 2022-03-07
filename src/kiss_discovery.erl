%% Joins table together when a new node appears
-module(kiss_discovery).
-behaviour(gen_server).

-export([start/1, start_link/1, add_table/2, info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

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

get_tables(Server) ->
    gen_server:call(Server, get_tables).

info(Server) ->
    {ok, Tables} = get_tables(Server),
    [kiss:info(Tab) || Tab <- Tables].

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
handle_call(get_tables, _From, State = #{tables := Tables}) ->
    {reply, {ok, Tables}, State};
handle_call(Msg, From, State) ->
    ?LOG_ERROR(#{what => unexpected_call, msg => Msg, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

handle_info(check, State) ->
    {noreply, handle_check(State)};
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

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
    LocalPid = whereis(Tab),
    %% That would trigger autoconnect for the first time
    case rpc:call(Node, erlang, whereis, [Tab]) of
        Pid when is_pid(Pid), is_pid(LocalPid) ->
            Result = kiss_join:join(kiss_discovery, #{table => Tab}, LocalPid, Pid),
            #{what => join_result, result => Result, node => Node, table => Tab};
        Other ->
            #{what => pid_not_found, reason => Other, node => Node, table => Tab}
    end.

report_results(Results, _State = #{results := OldResults}) ->
    Changed = Results -- OldResults,
    [report_result(Result) || Result <- Changed].

report_result(Map) ->
    ?LOG_INFO(Map).
