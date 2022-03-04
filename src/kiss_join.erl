-module(kiss_join).
-export([join/4]).
-include_lib("kernel/include/logger.hrl").

%% Adds a node to a cluster.
%% Writes from other nodes would wait for join completion.
%% LockKey should be the same on all nodes.
join(LockKey, Info, LocalPid, RemotePid) when is_pid(LocalPid), is_pid(RemotePid) ->
    Info2 = Info#{local_pid => LocalPid, remote_pid => RemotePid, remote_node => node(RemotePid)},
    OtherPids = kiss:other_pids(LocalPid),
    case lists:member(RemotePid, OtherPids) of
        true ->
            {error, already_joined};
        false ->
                Start = os:timestamp(),
                F = fun() -> join_loop(LockKey, Info2, LocalPid, RemotePid, Start) end,
                kiss_long:run(Info2#{task => join}, F)
    end.

join_loop(LockKey, Info, LocalPid, RemotePid, Start) ->
    %% Only one join at a time:
    %% - for performance reasons, we don't want to cause too much load for active nodes
    %% - to avoid deadlocks, because joining does gen_server calls
    F = fun() ->
        Diff = timer:now_diff(os:timestamp(), Start) div 1000,
        %% Getting the lock could take really long time in case nodes are
        %% overloaded or joining is already in progress on another node
        ?LOG_INFO(Info#{what => join_got_lock, after_time_ms => Diff}),
        join2(Info, LocalPid, RemotePid)
        end,
    LockRequest = {LockKey, self()},
    %% Just lock all nodes, no magic here :)
    Nodes = [node() | nodes()],
    Retries = 1,
    case global:trans(LockRequest, F, Nodes, Retries) of
        aborted ->
            ?LOG_ERROR(Info#{what => join_retry, reason => lock_aborted}),
            join_loop(LockKey, Info, LocalPid, RemotePid, Start);
        Result ->
            Result
    end.

join2(_Info, LocalPid, RemotePid) ->
    LocalOtherPids = kiss:other_pids(LocalPid),
    RemoteOtherPids = kiss:other_pids(RemotePid),
    LocPids = [LocalPid | LocalOtherPids],
    RemPids = [RemotePid | RemoteOtherPids],
    AllPids = LocPids ++ RemPids,
    [kiss:pause(Pid) || Pid <- AllPids],
    try
        kiss:sync(LocalPid),
        kiss:sync(RemotePid),
        {ok, LocalDump} = kiss:remote_dump(LocalPid),
        {ok, RemoteDump} = kiss:remote_dump(RemotePid),
        [kiss:send_dump_to_remote_node(Pid, LocPids, LocalDump) || Pid <- RemPids],
        [kiss:send_dump_to_remote_node(Pid, RemPids, RemoteDump) || Pid <- LocPids],
        ok
    after
        [kiss:unpause(Pid) || Pid <- AllPids]
    end.
