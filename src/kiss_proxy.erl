%% We monitor this process instead of a remote process
-module(kiss_proxy).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-behaviour(gen_server).

start_link(RemotePid) ->
    gen_server:start_link(?MODULE, [RemotePid], []).

init([RemotePid]) ->
    MonRef = erlang:monitor(process, RemotePid),
    {ok, #{mon => MonRef, remote_pid => RemotePid}}.

handle_call(_Reply, _From, State) ->
    {reply, ok, State}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, Pid, _Reason}, State = #{mon := MonRef}) ->
    error_logger:error_msg("Node down ~p", [node(Pid)]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

