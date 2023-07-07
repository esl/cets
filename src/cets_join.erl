%% @doc Cluster join logic.
-module(cets_join).
-export([join/4]).
-include_lib("kernel/include/logger.hrl").

-type lock_key() :: term().

%% Adds a node to a cluster.
%% Writes from other nodes would wait for join completion.
%% LockKey should be the same on all nodes.
-spec join(lock_key(), cets_long:log_info(), pid(), pid()) -> ok | {error, term()}.
join(LockKey, Info, LocalPid, RemotePid) when is_pid(LocalPid), is_pid(RemotePid) ->
    Info2 = Info#{
        local_pid => LocalPid,
        remote_pid => RemotePid,
        remote_node => node(RemotePid)
    },
    F = fun() -> join1(LockKey, Info2, LocalPid, RemotePid) end,
    cets_long:run_safely(Info2#{long_task_name => join}, F).

join1(LockKey, Info, LocalPid, RemotePid) ->
    OtherPids = cets:other_pids(LocalPid),
    case lists:member(RemotePid, OtherPids) of
        true ->
            {error, already_joined};
        false ->
            Start = erlang:system_time(millisecond),
            join_loop(LockKey, Info, LocalPid, RemotePid, Start)
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
        cets_long:run_spawn(Info, fun() -> join2(Info, LocalPid, RemotePid) end)
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
    %% Joining is a symmetrical operation here - both servers exchange information between each other.
    %% We still use LocalPid/RemotePid in names
    %% (they are local and remote pids as passed from the cets_join and from the cets_discovery).
    #{opts := Opts} = cets:info(LocalPid),
    LocalOtherPids = cets:other_pids(LocalPid),
    RemoteOtherPids = cets:other_pids(RemotePid),
    LocPids = [LocalPid | LocalOtherPids],
    RemPids = [RemotePid | RemoteOtherPids],
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
        {LocalDump2, RemoteDump2} = maybe_apply_resolver(LocalDump, RemoteDump, Opts),
        RemF = fun(Pid) -> cets:send_dump(Pid, LocPids, LocalDump2) end,
        LocF = fun(Pid) -> cets:send_dump(Pid, RemPids, RemoteDump2) end,
        lists:foreach(RemF, RemPids),
        lists:foreach(LocF, LocPids),
        ok
    after
        lists:foreach(fun({Pid, Ref}) -> cets:unpause(Pid, Ref) end, Paused)
    end.

remote_or_local_dump(Pid) when node(Pid) =:= node() ->
    {ok, Tab} = cets:table_name(Pid),
    %% Reduce copying
    {ok, cets:dump(Tab)};
remote_or_local_dump(Pid) ->
    %% We actually need to ask the remote process
    cets:remote_dump(Pid).

maybe_apply_resolver(LocalDump, RemoteDump, Opts = #{handle_conflict := F}) ->
    Type = maps:get(type, Opts, ordered_set),
    Pos = maps:get(keypos, Opts, 1),
    apply_resolver(Type, LocalDump, RemoteDump, F, Pos);
maybe_apply_resolver(LocalDump, RemoteDump, Opts) ->
    {LocalDump, RemoteDump}.

%% Bags do not have conflicts, so do not define a resolver for them.
apply_resolver(ordered_set, LocalDump, RemoteDump, F, Pos) ->
    %% Both dumps are sorted by the key (the lowest key first)
    apply_resolver_for_sorted(LocalDump, RemoteDump, F, Pos, [], []).

apply_resolver_for_sorted([X|LocalDump], [X|RemoteDump], F, Pos, LocalAcc, RemoteAcc) ->
    %% Presents in both dumps, skip it at all (we don't need to insert it, it is already inserted)
    apply_resolver_for_sorted(LocalDump, RemoteDump, F, Pos, LocalAcc, RemoteAcc);
apply_resolver_for_sorted([L|LocalDump] = LocalDumpFull,
                          [R|RemoteDump] = RemoteDumpFull,
                          F, Pos, LocalAcc, RemoteAcc) ->
    LKey = element(Pos, L),
    RKey = element(Pos, R),
    if
        LKey =:= RKey ->
            New = F(L, R),
            apply_resolver_for_sorted(LocalDump, RemoteDump, F, Pos, [New|LocalAcc], [New|RemoteAcc]);
        LKey < RKey ->
            %% Record exist only in the local dump
            apply_resolver_for_sorted(LocalDump, RemoteDumpFull, F, Pos, [L|LocalAcc], RemoteAcc);
        true ->
            %% Record exist only in the remote dump
            apply_resolver_for_sorted(LocalDumpFull, RemoteDump, F, Pos, LocalAcc, [R|RemoteAcc])
    end;
apply_resolver_for_sorted(LocalDump, RemoteDump, F, Pos, LocalAcc, RemoteAcc) ->
    {lists:reverse(LocalAcc, LocalDump), lists:reverse(RemoteAcc, RemoteDump)}.
