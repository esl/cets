-module(cets_join).
-export([join/4]).
-include_lib("kernel/include/logger.hrl").

%% Adds a node to a cluster.
%% Writes from other nodes would wait for join completion.
%% LockKey should be the same on all nodes.
join(LockKey, Info, LocalPid, RemotePid) when is_pid(LocalPid), is_pid(RemotePid) ->
    Info2 = Info#{local_pid => LocalPid,
                  remote_pid => RemotePid, remote_node => node(RemotePid)},
    F = fun() -> join1(LockKey, Info2, LocalPid, RemotePid) end,
    cets_long:run_safely(Info2#{what => join_failed}, F).

join1(LockKey, Info, LocalPid, RemotePid) ->
    OtherPids = cets:other_pids(LocalPid),
    case lists:member(RemotePid, OtherPids) of
        true ->
            {error, already_joined};
        false ->
                Start = erlang:system_time(millisecond),
                F = fun() -> join_loop(LockKey, Info, LocalPid, RemotePid, Start) end,
                cets_long:run(Info#{task => join}, F)
    end.

join_loop(LockKey, Info, LocalPid, RemotePid, Start) ->
    %% Only one join at a time:
    %% - for performance reasons, we don't want to cause too much load for active nodes
    %% - to avoid deadlocks, because joining does gen_server calls
    F = fun() ->
        Diff = erlang:system_time(millisecond) - Start,
        %% Getting the lock could take really long time in case nodes are
        %% overloaded or joining is already in progress on another node
        ?LOG_INFO(Info#{what => join_got_lock, after_time_ms => Diff}),
        %% Do joining in a separate process to reduce GC
        cets_clean:blocking(fun() -> join2(Info, LocalPid, RemotePid) end)
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
    LocalOtherPids = cets:other_pids(LocalPid),
    RemoteOtherPids = cets:other_pids(RemotePid),
    LocPids = [LocalPid|LocalOtherPids],
    RemPids = [RemotePid|RemoteOtherPids],
    AllPids = LocPids ++ RemPids,
    Paused = [{Pid, cets:pause(Pid)} || Pid <- AllPids],
    %% Merges data from two partitions together.
    %% Each entry in the table is allowed to be updated by the node that owns
    %% the key only, so merging is easy.
    try
        cets:sync(LocalPid),
        cets:sync(RemotePid),
        {ok, LocalDump} = remote_or_local_dump(LocalPid),
        {ok, RemoteDump} = remote_or_local_dump(RemotePid),
        [cets:send_dump_to_remote_node(Pid, LocPids, LocalDump) || Pid <- RemPids],
        [cets:send_dump_to_remote_node(Pid, RemPids, RemoteDump) || Pid <- LocPids],
        ok
    after
        [cets:unpause(Pid, Ref) || {Pid, Ref} <- Paused]
    end.

remote_or_local_dump(Pid) when node(Pid) =:= node() ->
    {ok, Tab} = cets:table_name(Pid),
    {ok, cets:dump(Tab)}; %% Reduce copying
remote_or_local_dump(Pid) ->
    cets:remote_dump(Pid). %% We actually need to ask the remote process
