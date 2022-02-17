%% We monitor this process instead of a remote process
-module(kiss_proxy).
-export([start/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-behaviour(gen_server).

start(RemotePid) ->
    gen_server:start(?MODULE, [RemotePid, self()], []).

init([RemotePid, ParentPid]) ->
    MonRef = erlang:monitor(process, RemotePid),
    MonRef2 = erlang:monitor(process, ParentPid),
    {ok, #{mon => MonRef, pmon => MonRef2, remote_pid => RemotePid}}.

handle_call(_Reply, _From, State) ->
    {reply, ok, State}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, Pid, _Reason}, State = #{mon := MonRef}) ->
    error_logger:error_msg("Node down ~p", [node(Pid)]),
    {stop, State};
handle_info({'DOWN', MonRef, process, Pid, _Reason}, State = #{pmon := MonRef}) ->
    error_logger:error_msg("Parent down ~p", [Pid]),
    {stop, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

