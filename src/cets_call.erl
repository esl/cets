%% @doc Module for extending gen_server calls.
%% Also, it contains code for sync and async multinode operations.
%% Operations are messages which could be buffered when a server is paused.
%% Operations are also broadcasted to the whole cluster.
-module(cets_call).

-export([long_call/2, long_call/3]).
-export([async_operation/2]).
-export([sync_operation/2]).
-export([wait_response/2]).
-export([send_leader_op/2]).

-include_lib("kernel/include/logger.hrl").

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
            Mon = erlang:monitor(process, Pid),
            gen_server:cast(Server, {op, {Mon, self()}, Msg}),
            Mon;
        undefined ->
            Mon = make_ref(),
            %% Simulate process down
            self() ! {'DOWN', Mon, process, undefined, pid_not_found},
            Mon
    end.

-spec sync_operation(server_ref(), op()) -> ok | Result :: term().
sync_operation(Server, Msg) ->
    Mon = async_operation(Server, Msg),
    %% We monitor the local server until the response from all servers is collected.
    wait_response(Mon, infinity).

%% This function must be called to receive the result of the multinode operation.
-spec wait_response(request_id(), non_neg_integer() | infinity) -> ok | Result :: term().
wait_response(Mon, Timeout) ->
    receive
        {'DOWN', Mon, process, _Pid, Reason} ->
            error({cets_down, Reason});
        {cets_reply, Mon, WaitInfo} ->
            wait_for_updated(Mon, WaitInfo);
        {cets_reply, Mon, WaitInfo, Result} ->
            wait_for_updated(Mon, WaitInfo),
            Result
    after Timeout ->
        erlang:demonitor(Mon, [flush]),
        error(timeout)
    end.

%% Wait for response from the remote nodes that the operation is completed.
%% remote_down is sent by the local server, if the remote server is down.
-spec wait_for_updated(reference(), cets:wait_info() | false) -> ok.
wait_for_updated(Mon, {Servers, MonTabInfo}) ->
    try
        do_wait_for_updated(Mon, Servers)
    after
        erlang:demonitor(Mon, [flush]),
        delete_from_mon_tab(MonTabInfo, Mon)
    end;
wait_for_updated(Mon, false) ->
    %% not replicated
    erlang:demonitor(Mon, [flush]),
    ok.

%% Edgecase: special treatment if Server is on the remote node
-spec delete_from_mon_tab(cets:local_or_remote_mon_tab(), reference()) -> ok.
delete_from_mon_tab({remote, Node, MonTab}, Mon) ->
    rpc:async_call(Node, ets, delete, [MonTab, Mon]);
delete_from_mon_tab(MonTab, Mon) ->
    ets:delete(MonTab, Mon).

do_wait_for_updated(_Mon, []) ->
    ok;
do_wait_for_updated(Mon, Servers) ->
    receive
        {updated, Mon, Pid} ->
            %% A replication confirmation from the remote server is received
            Servers2 = lists:delete(Pid, Servers),
            do_wait_for_updated(Mon, Servers2);
        {remote_down, Mon, Pid} ->
            %% This message is sent by our local server when
            %% the remote server is down condition is detected
            Servers2 = lists:delete(Pid, Servers),
            do_wait_for_updated(Mon, Servers2);
        {'DOWN', Mon, process, _Pid, Reason} ->
            %% Local server is down, this is a critical error
            error({cets_down, Reason})
    end.

-spec where(server_ref()) -> pid() | undefined.
where(Pid) when is_pid(Pid) -> Pid;
where(Name) when is_atom(Name) -> whereis(Name);
where({global, Name}) -> global:whereis_name(Name);
where({local, Name}) -> whereis(Name);
where({via, Module, Name}) -> Module:whereis_name(Name).

%% Sends all requests to a single node in the cluster
-spec send_leader_op(server_ref(), op()) -> term().
send_leader_op(Server, Op) ->
    Leader = cets:get_leader(Server),
    Res = cets_call:sync_operation(Leader, {leader_op, Op}),
    case Res of
        {error, wrong_leader} ->
            ?LOG_WARNING(#{what => wrong_leader, server => Server, operation => Op}),
            %% We are free to retry
            %% While it is infinite retries, the leader election logic is simple.
            %% The only issue could be if there are bugs in the leader election logic
            %% (i.e. our server thinks there is one leader in the cluster,
            %% while that leader has another leader selected - i.e. an impossible case)
            send_leader_op(Server, Op);
        _ ->
            Res
    end.
