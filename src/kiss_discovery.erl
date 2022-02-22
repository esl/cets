%% AWS autodiscovery is kinda bad.
%% - UDP broadcasts do not work
%% - AWS CLI needs access
%% - DNS does not allow to list subdomains
%% So, we use a file with nodes to connect as a discovery mechanism
%% (so, you can hardcode nodes or use your method of filling it)
-module(kiss_discovery).
-export([start/1, start_link/1, add_table/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-behaviour(gen_server).

%% disco_file
%% tables
start(Opts) ->
    start_common(start, Opts).

start_link(Opts) ->
    start_common(start_link, Opts).

start_common(F, Opts = #{disco_file := _}) ->
    Args =
        case Opts of
            #{name := Name} ->
                [{local, Name}, ?MODULE, [Opts], []];
            _ ->
                [?MODULE, [Opts], []]
        end,
    apply(gen_server, F, Args).

add_table(Server, Table) ->
    gen_server:call(Server, {add_table, Table}).

init([Opts]) ->
    self() ! check,
    Tables = maps:get(tables, Opts, []),
    {ok, Opts#{results => [], tables => Tables}}.

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

handle_cast(Msg, State) ->
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
handle_check(State = #{disco_file := Filename, tables := Tables}) ->
    State2 = case file:read_file(Filename) of
                 {error, Reason} ->
                     error_logger:error_msg("what=discovery_failed filename=~0p reason=~0p",
                                            [Filename, Reason]),
                     State;
                 {ok, Text} ->
                     Lines = binary:split(Text, [<<"\r">>, <<"\n">>, <<" ">>], [global]),
                     Nodes = [binary_to_atom(X, latin1) || X <- Lines, X =/= <<>>],
                     Results = [do_join(Tab, Node) || Tab <- Tables, Node <- Nodes, node() =/= Node],
                     report_results(Results, State),
                     State#{results => Results}
             end,
    schedule_check(State2).

schedule_check(State) ->
    case State of
        #{timer_ref := OldRef} ->
            erlang:cancel_timer(OldRef);
        _ ->
            ok
    end,
    TimerRef = erlang:send_after(5000, self(), check),
    State#{timer_ref => TimerRef}.


do_join(Tab, Node) ->
    %% That would trigger autoconnect for the first time
    case rpc:call(Node, erlang, whereis, [Tab]) of
        Pid when is_pid(Pid) ->
            Result = kiss:join(kiss_discovery, Tab, Pid),
            #{what => join_result, result => Result, node => Node, table => Tab};
        Other ->
            #{what => pid_not_found, reason => Other, node => Node, table => Tab}
    end.

report_results(Results, State = #{results := OldResults}) ->
    Changed = Results -- OldResults,
    [report_result(Result) || Result <- Results].

report_result(Map) ->
    Text = [io_lib:format("~0p=~0p ", [K, V]) || {K, V} <- maps:to_list(Map)],
    error_logger:info_msg("discovery ~s", [Text]).
