%% @doc Cluster join logic.
-module(cets_join).
-export([join/4]).
-export([join/5]).
-include_lib("kernel/include/logger.hrl").

%% Used in tests
-ignore_xref([join/5]).

-type lock_key() :: term().

-spec join(lock_key(), cets_long:log_info(), pid(), pid()) -> ok | {error, term()}.
join(LockKey, Info, LocalPid, RemotePid) ->
    join(LockKey, Info, LocalPid, RemotePid, #{}).

%% Adds a node to a cluster.
%% Writes from other nodes would wait for join completion.
%% LockKey should be the same on all nodes.
-spec join(lock_key(), cets_long:log_info(), pid(), pid(), Opts :: #{}) -> ok | {error, term()}.
join(_LockKey, _Info, Pid, Pid, _Opts) ->
    {error, same_pid};
join(LockKey, Info, LocalPid, RemotePid, Opts) when
    is_pid(LocalPid), is_pid(RemotePid), LocalPid =/= RemotePid
->
    Info2 = Info#{
        local_pid => LocalPid,
        remote_pid => RemotePid,
        remote_node => node(RemotePid)
    },
    F = fun() -> join1(LockKey, Info2, LocalPid, RemotePid, Opts) end,
    cets_long:run_safely(Info2#{long_task_name => join}, F).

join1(LockKey, Info, LocalPid, RemotePid, Opts) ->
    OtherPids = cets:other_pids(LocalPid),
    case lists:member(RemotePid, OtherPids) of
        true ->
            {error, already_joined};
        false ->
            Start = erlang:system_time(millisecond),
            join_loop(LockKey, Info, LocalPid, RemotePid, Start, Opts)
    end.

join_loop(LockKey, Info, LocalPid, RemotePid, Start, Opts) ->
    %% Only one join at a time:
    %% - for performance reasons, we don't want to cause too much load for active nodes
    %% - to avoid deadlocks, because joining does gen_server calls
    F = fun() ->
        Diff = erlang:system_time(millisecond) - Start,
        %% Getting the lock could take really long time in case nodes are
        %% overloaded or joining is already in progress on another node
        ?LOG_INFO(Info#{what => join_got_lock, after_time_ms => Diff}),
        %% Do joining in a separate process to reduce GC
        cets_long:run_spawn(Info, fun() -> join2(Info, LocalPid, RemotePid, Opts) end)
    end,
    LockRequest = {LockKey, self()},
    %% Just lock all nodes, no magic here :)
    Nodes = [node() | nodes()],
    Retries = 1,
    case global:trans(LockRequest, F, Nodes, Retries) of
        aborted ->
            ?LOG_ERROR(Info#{what => join_retry, reason => lock_aborted}),
            join_loop(LockKey, Info, LocalPid, RemotePid, Start, Opts);
        Result ->
            Result
    end.

join2(_Info, LocalPid, RemotePid, Opts) ->
    run_step(join_start, Opts),
    %% Joining is a symmetrical operation here - both servers exchange information between each other.
    %% We still use LocalPid/RemotePid in names
    %% (they are local and remote pids as passed from the cets_join and from the cets_discovery).
    #{opts := CetsOpts} = cets:info(LocalPid),
    %% Ensure that these two servers have processed any pending check_server requests
    %% and their other_pids list is fully updated
    ok = cets:sync(LocalPid),
    ok = cets:sync(RemotePid),
    LocalOtherPids = cets:other_pids(LocalPid),
    RemoteOtherPids = cets:other_pids(RemotePid),
    LocPids = [LocalPid | LocalOtherPids],
    RemPids = [RemotePid | RemoteOtherPids],
    AllPids = LocPids ++ RemPids,
    run_step({all_pids_known, AllPids}, Opts),
    Aliases = make_aliases(AllPids),
    %% Asign server_num for each server in the new cluster.
    Nums = maps:from_list(lists:zip(AllPids, lists:seq(0, length(AllPids) - 1))),
    %% Ask processes to stop applying messages.
    [true = is_reference(cets:pause(Pid)) || Pid <- AllPids],
    run_step(paused, Opts),
    %% Merges data from two partitions together.
    %% Each entry in the table is allowed to be updated by the node that owns
    %% the key only, so merging is easy.
    Ref = make_ref(),
    ok = cets:sync(LocalPid),
    ok = cets:sync(RemotePid),
    {ok, LocalDump} = cets:remote_or_local_dump(LocalPid),
    {ok, RemoteDump} = cets:remote_or_local_dump(RemotePid),
    {LocalDump2, RemoteDump2} = maybe_apply_resolver(LocalDump, RemoteDump, CetsOpts),
    %% Don't send dumps in parallel to not cause out-of-memory.
    %% It could be faster though.
    %% We could check if pause reference is still valid here.
    RemF = fun(Pid) -> cets:send_dump(Pid, Ref, Nums, aliases_for(Pid, Aliases), LocalDump2) end,
    LocF = fun(Pid) -> cets:send_dump(Pid, Ref, Nums, aliases_for(Pid, Aliases), RemoteDump2) end,
    lists:foreach(RemF, RemPids),
    lists:foreach(LocF, LocPids),
    %% We could do voting here and nodes would apply automatically once they receive
    %% ack that other nodes have the same dump pending.
    ApplyMons = lists:map(
        fun(Pid) ->
            Num = maps:get(Pid, Nums),
            run_step({before_apply_dump, Num, Pid}, Opts),
            %% Do apply_dump in parallel to improve performance
            spawn_monitor(fun() -> cets:apply_dump(Pid, Ref) end)
        end,
        AllPids
    ),
    %% Block till all the processes process apply_dump
    Reasons = [
        receive
            {'DOWN', Mon, process, Pid, Reason} ->
                Reason
        end
     || {Pid, Mon} <- ApplyMons
    ],
    [normal] = lists:usort(Reasons),
    %% Would be unpaused automatically after this process exits
    ok.

%% Recreate all aliases
%% So we apply don't receive new updates unless we have applied a data diff
make_aliases(AllPids) ->
    %% Pid monitors Pid2
    [
        {Pid, Pid2, Alias}
     || Pid <- AllPids,
        {Pid2, Alias} <- cets:make_aliases_for(Pid, lists:delete(Pid, AllPids))
    ].

aliases_for(Pid, Aliases) ->
    %% Pid monitors these:
    PidMons = [{Pid2, Alias} || {Pid1, Pid2, Alias} <- Aliases, Pid =:= Pid1],
    %% Pid we monitor
    %% Monitor to detect that we the remote server is down
    %% Alias to send messages from Pid to Pid2
    Res = [
        {Pid2, Alias, find_destination(Pid, Pid2, Aliases)}
     || {Pid2, Alias} <- PidMons
    ],
    assert_aliases_are_different(Res),
    Res.

assert_aliases_are_different(Res) ->
    [] = [X || {_, A, A, _} = X <- Res],
    ok.

find_destination(Pid1, Pid2, Aliases) when Pid1 =/= Pid2 ->
    [Dest] = [Alias || {A, B, Alias} <- Aliases, A =:= Pid2, B =:= Pid1],
    Dest.

maybe_apply_resolver(LocalDump, RemoteDump, Opts = #{handle_conflict := F}) ->
    Type = maps:get(type, Opts, ordered_set),
    Pos = maps:get(keypos, Opts, 1),
    apply_resolver(Type, LocalDump, RemoteDump, F, Pos);
maybe_apply_resolver(LocalDump, RemoteDump, _Opts) ->
    {LocalDump, RemoteDump}.

%% Bags do not have conflicts, so do not define a resolver for them.
apply_resolver(ordered_set, LocalDump, RemoteDump, F, Pos) ->
    %% Both dumps are sorted by the key (the lowest key first)
    apply_resolver_for_sorted(LocalDump, RemoteDump, F, Pos, [], []).

apply_resolver_for_sorted([X | LocalDump], [X | RemoteDump], F, Pos, LocalAcc, RemoteAcc) ->
    %% Presents in both dumps, skip it at all (we don't need to insert it, it is already inserted)
    apply_resolver_for_sorted(LocalDump, RemoteDump, F, Pos, LocalAcc, RemoteAcc);
apply_resolver_for_sorted(
    [L | LocalDump] = LocalDumpFull,
    [R | RemoteDump] = RemoteDumpFull,
    F,
    Pos,
    LocalAcc,
    RemoteAcc
) ->
    LKey = element(Pos, L),
    RKey = element(Pos, R),
    if
        LKey =:= RKey ->
            New = F(L, R),
            apply_resolver_for_sorted(LocalDump, RemoteDump, F, Pos, [New | LocalAcc], [
                New | RemoteAcc
            ]);
        LKey < RKey ->
            %% Record exists only in the local dump
            apply_resolver_for_sorted(LocalDump, RemoteDumpFull, F, Pos, [L | LocalAcc], RemoteAcc);
        true ->
            %% Record exists only in the remote dump
            apply_resolver_for_sorted(LocalDumpFull, RemoteDump, F, Pos, LocalAcc, [R | RemoteAcc])
    end;
apply_resolver_for_sorted(LocalDump, RemoteDump, _F, _Pos, LocalAcc, RemoteAcc) ->
    {lists:reverse(LocalAcc, LocalDump), lists:reverse(RemoteAcc, RemoteDump)}.

run_step(Step, #{step_handler := F}) ->
    F(Step);
run_step(_Step, _Opts) ->
    ok.
