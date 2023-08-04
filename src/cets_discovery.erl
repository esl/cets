%% @doc Node discovery logic
%% Joins table together when a new node appears
-module(cets_discovery).

-behaviour(gen_server).

-export([start/1, start_link/1, add_table/2, info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-ignore_xref([start/1, start_link/1, add_table/2, info/1, behaviour_info/1]).

-include_lib("kernel/include/logger.hrl").

-type backend_state() :: term().
-type get_nodes_result() :: {ok, [node()]} | {error, term()}.

-export_type([get_nodes_result/0]).

-type from() :: {pid(), reference()}.
-type state() ::
    #{results := [term()],
      tables := [atom()],
      backend_module := module(),
      backend_state := state(),
      timer_ref := reference() | undefined}.
%% Backend could define its own options
-type opts() :: #{name := atom(), _ := _}.
-type start_result() :: {ok, pid()} | {error, term()}.
-type server() :: pid() | atom().

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

-spec add_table(server(), cets:table_name()) -> ok | {error, already_added}.
add_table(Server, Table) ->
    gen_server:call(Server, {add_table, Table}).

-spec get_tables(server()) -> {ok, [cets:table_name()]}.
get_tables(Server) ->
    gen_server:call(Server, get_tables).

-spec info(server()) -> [cets:info()].
info(Server) ->
    {ok, Tables} = get_tables(Server),
    [cets:info(Tab) || Tab <- Tables].

-spec init(term()) -> {ok, state()}.
init(Opts) ->
    Mod = maps:get(backend_module, Opts, cets_discovery_file),
    self() ! check,
    Tables = maps:get(tables, Opts, []),
    BackendState = Mod:init(Opts),
    {ok,
     #{results => [],
       tables => Tables,
       backend_module => Mod,
       backend_state => BackendState,
       timer_ref => undefined}}.

-spec handle_call(term(), from(), state()) -> {reply, term(), state()}.
handle_call({add_table, Table}, _From, State = #{tables := Tables}) ->
    case lists:member(Table, Tables) of
        true ->
            {reply, {error, already_added}, State};
        false ->
            State2 = State#{tables := [Table | Tables]},
            {reply, ok, handle_check(State2)}
    end;
handle_call(get_tables, _From, State = #{tables := Tables}) ->
    {reply, {ok, Tables}, State};
handle_call(Msg, From, State) ->
    ?LOG_ERROR(#{what => unexpected_call,
                 msg => Msg,
                 from => From}),
    {reply, {error, unexpected_call}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(check, State) ->
    {noreply, handle_check(State)};
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
    schedule_check(State);
handle_check(State = #{backend_module := Mod, backend_state := BackendState}) ->
    {Res, BackendState2} = Mod:get_nodes(BackendState),
    State2 = handle_get_nodes_result(Res, State),
    schedule_check(State2#{backend_state := BackendState2}).

handle_get_nodes_result({error, _Reason}, State) ->
    State;
handle_get_nodes_result({ok, Nodes}, State = #{tables := Tables}) ->
    Results = [do_join(Tab, Node) || Tab <- Tables, Node <- Nodes, node() =/= Node],
    report_results(Results, State),
    State#{results := Results}.

schedule_check(State) ->
    cancel_old_timer(State),
    TimerRef = erlang:send_after(5000, self(), check),
    State#{timer_ref := TimerRef}.

cancel_old_timer(#{timer_ref := OldRef}) when is_reference(OldRef) ->
    %% Match result to prevent from Dialyzer warning
    _ = erlang:cancel_timer(OldRef),
    flush_all_checks(),
    ok;
cancel_old_timer(_State) ->
    ok.

flush_all_checks() ->
    receive
        check ->
            flush_all_checks()
    after 0 ->
        ok
    end.

do_join(Tab, Node) ->
    LocalPid = whereis(Tab),
    %% That would trigger autoconnect for the first time
    case rpc:call(Node, erlang, whereis, [Tab]) of
        Pid when is_pid(Pid), is_pid(LocalPid) ->
            Result = cets_join:join(cets_discovery, #{table => Tab}, LocalPid, Pid),
            #{what => join_result,
              result => Result,
              node => Node,
              table => Tab};
        Other ->
            #{what => pid_not_found,
              reason => Other,
              node => Node,
              table => Tab}
    end.

report_results(Results, _State = #{results := OldResults}) ->
    Changed = Results -- OldResults,
    lists:foreach(fun report_result/1, Changed),
    ok.

report_result(Map) ->
    ?LOG_INFO(Map).
