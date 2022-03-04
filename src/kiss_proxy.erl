%% We monitor this process instead of a remote process
-module(kiss_proxy).
-behaviour(gen_server).

-export([start/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

start(RemotePid) ->
    gen_server:start(?MODULE, [RemotePid, self()], []).

init([RemotePid, ParentPid]) ->
    MonRef = erlang:monitor(process, RemotePid),
    MonRef2 = erlang:monitor(process, ParentPid),
    {ok, #{mon => MonRef, pmon => MonRef2, remote_pid => RemotePid}}.

handle_call(_Reply, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, Pid, _Reason}, State = #{mon := MonRef}) ->
    ?LOG_ERROR(#{what => node_down, remote_pid => Pid, node => node(Pid)}),
    {stop, normal, State};
handle_info({'DOWN', MonRef, process, Pid, _Reason}, State = #{pmon := MonRef}) ->
    ?LOG_ERROR(#{what => parent_process_down, parent_pid => Pid}),
    {stop, normal, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

