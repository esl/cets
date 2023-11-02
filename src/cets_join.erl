%% @doc Cluster join logic.
%% Checkpoints are used for testing and do not affect the joining logic.
-module(cets_join).
-export([join/4]).
-export([join/5]).
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-export([check_could_reach_each_other/3]).
-endif.

-type lock_key() :: term().
-type join_ref() :: reference().
-type server_pid() :: cets:server_pid().

%% Critical events during the joining procedure
-type checkpoint() ::
    join_start
    | before_retry
    | before_get_pids
    | before_check_fully_connected
    | before_unpause
    | {before_send_dump, server_pid()}.

-type checkpoint_handler() :: fun((checkpoint()) -> ok).
-type join_opts() :: #{checkpoint_handler => checkpoint_handler()}.

-export_type([join_ref/0]).

-ignore_xref([join/5]).

%% Adds a node to a cluster.
%% Writes from other nodes would wait for join completion.
%% LockKey should be the same on all nodes.
-spec join(lock_key(), cets_long:log_info(), server_pid(), server_pid()) ->
    ok | {error, term()}.
join(LockKey, Info, LocalPid, RemotePid) ->
    join(LockKey, Info, LocalPid, RemotePid, #{}).

-spec join(lock_key(), cets_long:log_info(), pid(), pid(), join_opts()) -> ok | {error, term()}.
join(_LockKey, _Info, Pid, Pid, _JoinOpts) when is_pid(Pid) ->
    {error, join_with_the_same_pid};
join(LockKey, Info, LocalPid, RemotePid, JoinOpts) when is_pid(LocalPid), is_pid(RemotePid) ->
    Info2 = Info#{
        local_pid => LocalPid,
        remote_pid => RemotePid,
        remote_node => node(RemotePid)
    },
    F = fun() -> join1(LockKey, Info2, LocalPid, RemotePid, JoinOpts) end,
    try
        cets_long:run_tracked(Info2#{long_task_name => join}, F)
    catch
        error:Reason:_ ->
            {error, Reason};
        %% Exits are thrown by gen_server:call API
        exit:Reason:_ ->
            {error, Reason}
    end.

join1(LockKey, Info, LocalPid, RemotePid, JoinOpts) ->
    OtherPids = cets:other_pids(LocalPid),
    case lists:member(RemotePid, OtherPids) of
        true ->
            {error, already_joined};
        false ->
            Start = erlang:system_time(millisecond),
            join_loop(LockKey, Info, LocalPid, RemotePid, Start, JoinOpts)
    end.

join_loop(LockKey, Info, LocalPid, RemotePid, Start, JoinOpts) ->
    %% Only one join at a time:
    %% - for performance reasons, we don't want to cause too much load for active nodes
    %% - to avoid deadlocks, because joining does gen_server calls
    F = fun() ->
        Diff = erlang:system_time(millisecond) - Start,
        %% Getting the lock could take really long time in case nodes are
        %% overloaded or joining is already in progress on another node
        ?LOG_INFO(Info#{what => join_got_lock, after_time_ms => Diff}),
        %% Do joining in a separate process to reduce GC
        FF = handle_throw(fun() -> join2(Info, LocalPid, RemotePid, JoinOpts) end),
        cets_long:run_spawn(Info, FF)
    end,
    LockRequest = {LockKey, self()},
    %% Just lock all nodes, no magic here :)
    Nodes = [node() | nodes()],
    Retries = 1,
    case global:trans(LockRequest, F, Nodes, Retries) of
        aborted ->
            checkpoint(before_retry, JoinOpts),
            Sleep = rand:uniform(1000),
            ?LOG_INFO(Info#{what => join_retry, reason => lock_aborted, sleep_ms => Sleep}),
            %% Use random sleep to reduce a chance of deadlocks.
            %% global.erl does something similar, but without logging.
            timer:sleep(Sleep),
            join_loop(LockKey, Info, LocalPid, RemotePid, Start, JoinOpts);
        Result ->
            Result
    end.

%% Exchanges data and a list of servers.
%% Pauses new operations during the exchange.
-spec join2(cets_long:log_info(), server_pid(), server_pid(), join_opts()) -> ok.
join2(Info, LocalPid, RemotePid, JoinOpts) ->
    checkpoint(join_start, JoinOpts),
    JoinRef = make_ref(),
    %% Joining is a symmetrical operation here - both servers exchange information between each other.
    %% We still use LocalPid/RemotePid in names
    %% (they are local and remote pids as passed from the cets_join and from the cets_discovery).
    #{opts := ServerOpts} = cets:info(LocalPid),
    checkpoint(before_get_pids, JoinOpts),
    LocPids = get_pids(LocalPid),
    RemPids = get_pids(RemotePid),
    check_pids(Info, LocPids, RemPids, JoinOpts),
    AllPids = LocPids ++ RemPids,
    Paused = [{Pid, cets:pause(Pid)} || Pid <- AllPids],
    %% Merges data from two partitions together.
    %% Each entry in the table is allowed to be updated by the node that owns
    %% the key only, so merging is easy.
    try
        ping_all_ok(Info, LocalPid),
        ping_all_ok(Info, RemotePid),
        {ok, LocalDump} = remote_or_local_dump(LocalPid),
        {ok, RemoteDump} = remote_or_local_dump(RemotePid),
        %% Check that still fully connected after getting the dumps
        %% and before making any changes
        check_fully_connected(Info, LocPids),
        check_fully_connected(Info, RemPids),
        {LocalDump2, RemoteDump2} = maybe_apply_resolver(LocalDump, RemoteDump, ServerOpts),
        RemF = fun(Pid) -> send_dump(Pid, LocPids, JoinRef, LocalDump2, JoinOpts) end,
        LocF = fun(Pid) -> send_dump(Pid, RemPids, JoinRef, RemoteDump2, JoinOpts) end,
        lists:foreach(LocF, LocPids),
        lists:foreach(RemF, RemPids),
        ok
    after
        checkpoint(before_unpause, JoinOpts),
        %% If unpause fails, there would be log messages
        lists:foreach(fun({Pid, Ref}) -> catch cets:unpause(Pid, Ref) end, Paused)
    end.

send_dump(Pid, Pids, JoinRef, Dump, JoinOpts) ->
    checkpoint({before_send_dump, Pid}, JoinOpts),
    %% Error reporting would be done by cets_long:call_tracked
    catch cets:send_dump(Pid, Pids, JoinRef, Dump).

remote_or_local_dump(Pid) when node(Pid) =:= node() ->
    {ok, Tab} = cets:table_name(Pid),
    %% Reduce copying
    {ok, cets:dump(Tab)};
remote_or_local_dump(Pid) ->
    %% We actually need to ask the remote process
    cets:remote_dump(Pid).

maybe_apply_resolver(LocalDump, RemoteDump, ServerOpts = #{handle_conflict := F}) ->
    Type = maps:get(type, ServerOpts, ordered_set),
    Pos = maps:get(keypos, ServerOpts, 1),
    apply_resolver(Type, LocalDump, RemoteDump, F, Pos);
maybe_apply_resolver(LocalDump, RemoteDump, _ServerOpts) ->
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

-spec get_pids(server_pid()) -> cets:servers().
get_pids(Pid) ->
    ordsets:add_element(Pid, cets:other_pids(Pid)).

-spec check_pids(cets_long:log_info(), cets:servers(), cets:servers(), join_opts()) -> ok.
check_pids(Info, LocPids, RemPids, JoinOpts) ->
    check_do_not_overlap(Info, LocPids, RemPids),
    checkpoint(before_check_fully_connected, JoinOpts),
    check_fully_connected(Info, LocPids),
    check_fully_connected(Info, RemPids),
    check_could_reach_each_other(Info, LocPids, RemPids).

-spec check_could_reach_each_other(cets_long:log_info(), cets:servers(), cets:servers()) -> ok.
check_could_reach_each_other(Info, LocPids, RemPids) ->
    LocNodes = lists:usort(lists:map(fun node/1, LocPids)),
    RemNodes = lists:usort(lists:map(fun node/1, RemPids)),
    Pairs = lists:usort([
        {min(LocNode, RemNode), max(LocNode, RemNode)}
     || LocNode <- LocNodes, RemNode <- RemNodes, LocNode =/= RemNode
    ]),
    Results =
        [
            {Node1, Node2,
                cets_long:run_tracked(
                    #{task => ping_node, node1 => Node1, node2 => Node2}, fun() ->
                        rpc:call(Node1, net_adm, ping, [Node2], 10000)
                    end
                )}
         || {Node1, Node2} <- Pairs
        ],
    NotConnected = [X || {_Node1, _Node2, Res} = X <- Results, Res =/= pong],
    case NotConnected of
        [] ->
            ok;
        _ ->
            ?LOG_ERROR(Info#{
                what => check_could_reach_each_other_failed,
                node_pairs_not_connected => NotConnected
            }),
            error(check_could_reach_each_other_failed)
    end.

-spec check_do_not_overlap(cets_long:log_info(), cets:servers(), cets:servers()) -> ok.
check_do_not_overlap(_Info, Pids, Pids) ->
    %% Same pids, looks like cluster is fully connected, just exit
    throw(skip_join_when_pids_are_the_same);
check_do_not_overlap(Info, LocPids, RemPids) ->
    case ordsets:intersection(LocPids, RemPids) of
        [] ->
            ok;
        Overlap ->
            ?LOG_ERROR(Info#{
                what => check_do_not_overlap_failed,
                local_servers => LocPids,
                remote_servers => RemPids,
                overlapped_servers => Overlap
            }),
            error(check_do_not_overlap_failed)
    end.

handle_throw(F) ->
    fun() ->
        try
            F()
        catch
            throw:skip_join_when_pids_are_the_same ->
                ok
        end
    end.

%% Checks that other_pids lists match for all nodes
%% If they are not matching - the node removal process could be in progress
-spec check_fully_connected(cets_long:log_info(), cets:servers()) -> ok.
check_fully_connected(Info, Pids) ->
    Lists = [get_pids(Pid) || Pid <- Pids],
    case lists:usort([Pids | Lists]) of
        [_] ->
            check_same_join_ref(Info, Pids);
        UniqueLists ->
            ?LOG_ERROR(Info#{
                what => check_fully_connected_failed,
                expected_pids => Pids,
                server_lists => Lists,
                unique_lists => UniqueLists
            }),
            error(check_fully_connected_failed)
    end.

ping_all_ok(Info, Pid) ->
    case cets:ping_all(Pid) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_ERROR(Info#{
                what => ping_all_pings_failed,
                text =>
                    <<"Failed to ping all CETS servers, interrupt the table joining. Would retry later">>,
                server_pid => Pid,
                reason => Reason
            }),
            error(ping_all_failed)
    end.

%% Check if all nodes have the same join_ref
%% If not - we don't want to continue joining
-spec check_same_join_ref(cets_long:log_info(), cets:servers()) -> ok.
check_same_join_ref(Info, Pids) ->
    Refs = [pid_to_join_ref(Pid) || Pid <- Pids],
    case lists:usort(Refs) of
        [_] ->
            ok;
        UniqueRefs ->
            ?LOG_ERROR(Info#{
                what => check_same_join_ref_failed,
                refs => lists:zip(Pids, Refs),
                unique_refs => UniqueRefs
            }),
            error(check_same_join_ref_failed)
    end.

-spec pid_to_join_ref(server_pid()) -> join_ref().
pid_to_join_ref(Pid) ->
    #{join_ref := JoinRef} = cets:info(Pid),
    JoinRef.

%% Checkpoints are used for testing
%% Checkpoints do nothing in production
-spec checkpoint(checkpoint(), join_opts()) -> ok.
-ifdef(TEST).
checkpoint(CheckPointName, #{checkpoint_handler := F}) ->
    F(CheckPointName);
checkpoint(_CheckPointName, _Opts) ->
    ok.
-else.
-compile({inline, [checkpoint/2]}).
checkpoint(_CheckPointName, _Opts) ->
    ok.
-endif.
