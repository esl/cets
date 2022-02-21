%% AWS autodiscovery is kinda bad.
%% - UDP broadcasts do not work
%% - AWS CLI needs access
%% - DNS does not allow to list subdomains
%% So, we use a file with nodes to connect as a discovery mechanism
%% (so, you can hardcode nodes or use your method of filling it)
-module(kiss_discovery).
-export([start/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-behaviour(gen_server).

%% disco_file
%% tables
start(Opts = #{disco_file := _, tables := _}) ->
    gen_server:start(?MODULE, [Opts], []).

init([Opts]) ->
    self() ! check,
    {ok, Opts#{results => []}}.

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
    schedule_check(),
    State2.

schedule_check() ->
    erlang:send_after(5000, self(), check).


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
