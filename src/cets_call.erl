%% @doc Module for extending gen_server calls.
%% Also, it contains code for sync and async multinode operations.
%% Operations are messages which could be buffered when a server is paused.
%% Operations are also broadcasted to the whole cluster.
-module(cets_call).

-export([long_call/2, long_call/3]).
-export([async_operation/2]).
-export([sync_operation/2]).
-export([wait_response/2]).

-type request_id() :: cets:request_id().
-type op() :: cets:op().
-type server_ref() :: cets:server_ref().
-type long_msg() :: cets:long_msg().

%% Does gen_server:call with better error reporting.
%% It would log a warning if the call takes too long.
-spec long_call(server_ref(), long_msg()) -> term().
long_call(Server, Msg) ->
    long_call(Server, Msg, #{msg => Msg}).

-spec long_call(server_ref(), long_msg(), map()) -> term().
long_call(Server, Msg, Info) ->
    case where(Server) of
        Pid when is_pid(Pid) ->
            Info2 = Info#{
                remote_server => Server,
                remote_pid => Pid,
                remote_node => node(Pid)
            },
            F = fun() -> gen_server:call(Pid, Msg, infinity) end,
            cets_long:run_safely(Info2, F);
        undefined ->
            {error, pid_not_found}
    end.

%% Contacts the local server to broadcast multinode operation.
%% Returns immediately.
%% You can wait for response from all nodes by calling wait_response/2.
%% You would have to call wait_response/2 to process incoming messages and to remove the monitor
%% (or the caller process can just exit to clean this up).
%%
%% (could not be implemented by an async gen_server:call, because we want
%% to keep monitoring the local gen_server till all responses are received).
-spec async_operation(server_ref(), op()) -> request_id().
async_operation(Server, Msg) ->
    case where(Server) of
        Pid when is_pid(Pid) ->
            Mon = erlang:monitor(process, Pid, [{alias, demonitor}]),
            gen_server:cast(Server, {op, {Mon, self()}, Msg}),
            Mon;
        undefined ->
            Mon = make_ref(),
            %% Simulate process down
            self() ! {'DOWN', Mon, process, undefined, pid_not_found},
            Mon
    end.

-spec sync_operation(server_ref(), op()) -> ok.
sync_operation(Server, Msg) ->
    Mon = async_operation(Server, Msg),
    %% We monitor the local server until the response from all servers is collected.
    %   wait_response(Mon, infinity).
    wait_response(Mon, 5000).

%% This function must be called to receive the result of the multinode operation.
-spec wait_response(request_id(), non_neg_integer() | infinity) -> ok.
wait_response(Mon, Timeout) ->
    receive
        {'DOWN', Mon, process, _Pid, Reason} ->
            error({cets_down, Reason});
        {cets_reply, Mon, WaitInfo} ->
            wait_for_updated(Mon, WaitInfo, Timeout)
    after Timeout ->
        erlang:demonitor(Mon, [flush]),
        error(timeout)
    end.

%% Wait for response from the remote nodes that the operation is completed.
%% remote_down is sent by the local server, if the remote server is down.
wait_for_updated(Mon, {Servers, MonTab}, Timeout) ->
    try
        do_wait_for_updated(Mon, Servers, Timeout)
    after
        erlang:demonitor(Mon, [flush]),
        ets:delete(MonTab, Mon)
    end.

do_wait_for_updated(_Mon, 0, _Timeout) ->
    ok;
do_wait_for_updated(Mon, Servers, Timeout) ->
    receive
        {cets_updated, Mon, Num} ->
            %% A replication confirmation from the remote server is received
            Servers2 = unset_flag(Num, Servers),
            do_wait_for_updated(Mon, Servers2, Timeout);
        {cets_remote_down, Mon, Num} ->
            %% This message is sent by our local server when
            %% the remote server is down condition is detected
            Servers2 = unset_flag(Num, Servers),
            do_wait_for_updated(Mon, Servers2, Timeout);
        {'DOWN', Mon, process, _Pid, Reason} ->
            %% Local server is down, this is a critical error
            error({cets_down, Reason})
    after Timeout ->
        %       error(timeout)
        error({timeout, Servers})
    end.

unset_flag(Pos, Bits) ->
    Bits band (bnot (1 bsl Pos)).

-spec where(server_ref()) -> pid() | undefined.
where(Pid) when is_pid(Pid) -> Pid;
where(Name) when is_atom(Name) -> whereis(Name);
where({global, Name}) -> global:whereis_name(Name);
where({local, Name}) -> whereis(Name);
where({via, Module, Name}) -> Module:whereis_name(Name).
