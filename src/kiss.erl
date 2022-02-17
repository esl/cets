%% Very simple multinode ETS writer
%% One file, everything is simple, but we don't silently hide race conditions
%% No transactions
%% We don't use rpc module, because it is one gen_server


%% We don't use monitors to avoid round-trips (that's why we don't use calls neither)
-module(kiss).
-export([start/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([load_test/0]).

-behaviour(gen_server).

%% Table and server has the same name
start(Tab, Opts) when is_atom(Tab) ->
    ets:new(Tab, [ordered_set, named_table,
                     public, {read_concurrency, true}]),
    N = erlang:system_info(schedulers),
    Ns = lists:seq(1, N),
    Names = [list_to_atom(atom_to_list(Tab) ++ integer_to_list(NN)) || NN <- Ns],
    persistent_term:put(Tab, list_to_tuple(Names)),
    [gen_server:start({local, Name}, ?MODULE, [Tab], [])
     || Name <- Names].


stop(Tab) ->
    gen_server:stop(Tab).

load_test() ->
    start(tab1, #{}),
    timer:tc(fun() ->
            pmap(fun(X) -> load_test2(1000000) end, lists:seq(1, erlang:system_info(schedulers)))
        end).

load_test2(0) -> ok;
load_test2(N) ->
    S = erlang:system_info(scheduler_id),
    Names = persistent_term:get(tab1),
    Name = element(S, Names),
    gen_server:call(Name, {insert, {N}}),
    load_test2(N - 1).

pmap(F, List) ->
   Parent = self(),
   Pids = [spawn_link(fun() -> Parent ! {result, self(), (catch F(X))} end) || X <- List],
   [receive {result, P, Res} when Pid =:= P -> Res end
    || Pid <- Pids].

%% Key is {USR, Sid, UpdateNumber}
%% Where first UpdateNumber is 0
insert(Tab, Rec) ->
%   Nodes = other_nodes(Tab),
    ets:insert_new(Tab, Rec).
    %% Insert to other nodes and block till written

init([Tab]) ->
    {ok, #{tab => Tab}}.

handle_call({insert, Rec}, From, State = #{tab := Tab}) ->
    ets:insert_new(Tab, Rec),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

