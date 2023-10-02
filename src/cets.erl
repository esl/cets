%% Very simple multinode ETS writer.
%% One file, everything is simple, but we don't silently hide race conditions.
%% No transactions support.
%% We don't use rpc module, because it is a single gen_server.
%% We use MonTab table instead of monitors to detect if one of remote servers
%% is down and would not send a replication result.
%% While Tab is an atom, we can join tables with different atoms for the local testing.
%% We pause writes when a new node is joining (we resume them again). It is to
%% ensure that all writes would be bulk copied.
%% We support merging data on join by default.
%% We do not check if we override data during join So, it is up to the user
%% to ensure that merging would survive overrides. Two ways to do it:
%% - Write each key once and only once (basically add a reference into a key)
%% - Add writer pid() or writer node() as a key. And do a proper cleanups using handle_down.
%%   (the data could still get overwritten though if a node joins back way too quick
%%    and cleaning is done outside of handle_down)
-module(cets).
-behaviour(gen_server).

-export([
    start/2,
    stop/1,
    insert/2,
    insert_many/2,
    insert_new/2,
    insert_serial/2,
    delete/2,
    delete_many/2,
    delete_object/2,
    delete_objects/2,
    dump/1,
    remote_dump/1,
    send_dump/4,
    table_name/1,
    other_nodes/1,
    get_nodes_request/1,
    other_pids/1,
    pause/1,
    unpause/2,
    get_leader/1,
    set_leader/2,
    sync/1,
    ping/1,
    info/1,
    insert_request/2,
    insert_many_request/2,
    delete_request/2,
    delete_many_request/2,
    delete_object_request/2,
    delete_objects_request/2,
    wait_response/2,
    wait_responses/2,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ignore_xref([
    start/2,
    stop/1,
    insert/2,
    insert_many/2,
    insert_new/2,
    insert_serial/2,
    delete/2,
    delete_many/2,
    delete_object/2,
    delete_objects/2,
    pause/1,
    unpause/2,
    get_leader/1,
    set_leader/2,
    sync/1,
    ping/1,
    info/1,
    other_nodes/1,
    get_nodes_request/1,
    insert_request/2,
    insert_many_request/2,
    delete_request/2,
    delete_many_request/2,
    delete_object_request/2,
    delete_objects_request/2,
    wait_response/2,
    wait_responses/2
]).

-include_lib("kernel/include/logger.hrl").

-type server_pid() :: pid().
-type server_ref() ::
    server_pid()
    | atom()
    | {local, atom()}
    | {global, term()}
    | {via, module(), term()}.
-type request_id() :: gen_server:request_id().
-type from() :: gen_server:from().
-type join_ref() :: cets_join:join_ref().
-type ack_pid() :: cets_ack:ack_pid().
-type op() ::
    {insert, tuple()}
    | {delete, term()}
    | {delete_object, term()}
    | {insert_many, [tuple()]}
    | {delete_many, [term()]}
    | {delete_objects, [term()]}
    | {insert_new, tuple()}
    | {leader_op, op()}.
-type remote_op() ::
    {remote_op, Op :: op(), From :: from(), AckPid :: ack_pid(), JoinRef :: join_ref()}.
-type backlog_entry() :: {op(), from()}.
-type table_name() :: atom().
-type pause_monitor() :: reference().
-type servers() :: ordsets:ordset(server_pid()).
-type state() :: #{
    tab := table_name(),
    ack_pid := ack_pid(),
    join_ref := join_ref(),
    %% Updated by set_other_servers/2 function only
    other_servers := servers(),
    leader := server_pid(),
    is_leader := boolean(),
    opts := start_opts(),
    backlog := [backlog_entry()],
    pause_monitors := [pause_monitor()]
}.

-type long_msg() ::
    pause
    | ping
    | remote_dump
    | sync
    | table_name
    | get_info
    | other_servers
    | get_nodes
    | {unpause, reference()}
    | get_leader
    | {set_leader, boolean()}
    | {send_dump, servers(), join_ref(), [tuple()]}.

-type info() :: #{
    table := table_name(),
    nodes := [node()],
    size := non_neg_integer(),
    memory := non_neg_integer(),
    ack_pid := ack_pid(),
    join_ref := join_ref(),
    opts := start_opts()
}.

-type handle_down_fun() :: fun((#{remote_pid := server_pid(), table := table_name()}) -> ok).
-type handle_conflict_fun() :: fun((tuple(), tuple()) -> tuple()).
-type handle_wrong_leader() :: fun((#{from := from(), op := op(), server := server_pid()}) -> ok).
-type start_opts() :: #{
    type => ordered_set | bag,
    keypos => non_neg_integer(),
    handle_down => handle_down_fun(),
    handle_conflict => handle_conflict_fun(),
    handle_wrong_leader => handle_wrong_leader()
}.
%% Reply is usually ok
-type response_return() :: {reply, Reply :: term()} | {error, {_, _}} | timeout.
-type response_timeout() :: timeout() | {abs, integer()}.

-export_type([
    request_id/0,
    op/0,
    server_pid/0,
    server_ref/0,
    long_msg/0,
    info/0,
    table_name/0,
    servers/0,
    response_return/0,
    response_timeout/0
]).

%% API functions

%% Table and server has the same name
%% Opts:
%% - handle_down = fun(#{remote_pid := Pid, table := Tab})
%%   Called when a remote node goes down. Do not update other nodes data
%%   from this function (otherwise circular locking could happen - use spawn
%%   to make a new async process if you need to update).
%%   i.e. any functions that replicate changes are not allowed (i.e. insert/2,
%%   remove/2).
%% - handle_conflict = fun(Record1, Record2) -> NewRecord
%%   Called when two records have the same key when clustering.
%%   NewRecord would be the record CETS would keep in the table under the key.
%%   Does not work for bags.
%%   We recommend to define that function if keys could have conflicts.
%%   This function would be called once for each conflicting key.
%%   We recommend to keep that function pure (or at least no blocking calls from it).
-spec start(table_name(), start_opts()) -> gen_server:start_ret().
start(Tab, Opts) when is_atom(Tab) ->
    case check_opts(Opts) of
        [] ->
            gen_server:start({local, Tab}, ?MODULE, {Tab, Opts}, []);
        Errors ->
            {error, Errors}
    end.

-spec stop(server_ref()) -> ok.
stop(Server) ->
    gen_server:stop(Server).

-spec dump(table_name()) -> Records :: [tuple()].
dump(Tab) ->
    ets:tab2list(Tab).

-spec remote_dump(server_ref()) -> {ok, Records :: [tuple()]}.
remote_dump(Server) ->
    cets_call:long_call(Server, remote_dump).

-spec table_name(server_ref()) -> {ok, table_name()}.
table_name(Tab) when is_atom(Tab) ->
    {ok, Tab};
table_name(Server) ->
    cets_call:long_call(Server, table_name).

-spec send_dump(server_ref(), servers(), join_ref(), [tuple()]) -> ok.
send_dump(Server, NewPids, JoinRef, OurDump) ->
    Info = #{msg => send_dump, join_ref => JoinRef, count => length(OurDump)},
    cets_call:long_call(Server, {send_dump, NewPids, JoinRef, OurDump}, Info).

%% Only the node that owns the data could update/remove the data.
%% Ideally, Key should contain inserter node info so cleaning and merging is simplified.
-spec insert(server_ref(), tuple()) -> ok.
insert(Server, Rec) when is_tuple(Rec) ->
    cets_call:sync_operation(Server, {insert, Rec}).

-spec insert_many(server_ref(), list(tuple())) -> ok.
insert_many(Server, Records) when is_list(Records) ->
    cets_call:sync_operation(Server, {insert_many, Records}).

%% Tries to insert a new record.
%% All inserts are sent to the leader node first.
%% It is a slightly slower comparing to just insert, because
%% extra messaging is required.
-spec insert_new(server_ref(), tuple()) -> WasInserted :: boolean().
insert_new(Server, Rec) when is_tuple(Rec) ->
    Res = cets_call:send_leader_op(Server, {leader_op, {insert_new, Rec}}),
    handle_insert_new_result(Res).

handle_insert_new_result(ok) -> true;
handle_insert_new_result({error, rejected}) -> false.

%% @doc Serialized version of `insert/2'.
%%
%% All `insert_serial' calls are sent to the leader node first.
%%
%% Similar to `insert_new/2', but overwrites the data silently on conflict.
%% It could be used to update entries, which use not node-specific keys.
-spec insert_serial(server_ref(), tuple()) -> ok.
insert_serial(Server, Rec) when is_tuple(Rec) ->
    ok = cets_call:send_leader_op(Server, {insert, Rec}).

%% Removes an object with the key from all nodes in the cluster.
%% Ideally, nodes should only remove data that they've inserted, not data from other node.
-spec delete(server_ref(), term()) -> ok.
delete(Server, Key) ->
    cets_call:sync_operation(Server, {delete, Key}).

-spec delete_object(server_ref(), tuple()) -> ok.
delete_object(Server, Object) ->
    cets_call:sync_operation(Server, {delete_object, Object}).

%% A separate function for multidelete (because key COULD be a list, so no confusion)
-spec delete_many(server_ref(), [term()]) -> ok.
delete_many(Server, Keys) ->
    cets_call:sync_operation(Server, {delete_many, Keys}).

-spec delete_objects(server_ref(), [tuple()]) -> ok.
delete_objects(Server, Objects) ->
    cets_call:sync_operation(Server, {delete_objects, Objects}).

-spec insert_request(server_ref(), tuple()) -> request_id().
insert_request(Server, Rec) ->
    cets_call:async_operation(Server, {insert, Rec}).

-spec insert_many_request(server_ref(), [tuple()]) -> request_id().
insert_many_request(Server, Records) ->
    cets_call:async_operation(Server, {insert_many, Records}).

-spec delete_request(server_ref(), term()) -> request_id().
delete_request(Server, Key) ->
    cets_call:async_operation(Server, {delete, Key}).

-spec delete_object_request(server_ref(), tuple()) -> request_id().
delete_object_request(Server, Object) ->
    cets_call:async_operation(Server, {delete_object, Object}).

-spec delete_many_request(server_ref(), [term()]) -> request_id().
delete_many_request(Server, Keys) ->
    cets_call:async_operation(Server, {delete_many, Keys}).

-spec delete_objects_request(server_ref(), [tuple()]) -> request_id().
delete_objects_request(Server, Objects) ->
    cets_call:async_operation(Server, {delete_objects, Objects}).

-spec wait_response(request_id(), timeout()) -> response_return().
wait_response(ReqId, Timeout) ->
    gen_server:wait_response(ReqId, Timeout).

%% @doc Waits for multiple responses
%% Returns results in the same order as ReqIds
%% Blocks for maximum Timeout milliseconds
-spec wait_responses([request_id()], response_timeout()) ->
    [response_return()].
wait_responses(ReqIds, Timeout) ->
    cets_call:wait_responses(ReqIds, Timeout).

-spec get_leader(server_ref()) -> server_pid().
get_leader(Tab) when is_atom(Tab) ->
    %% Optimization: replace call with ETS lookup
    try
        cets_metadata:get(Tab, leader)
    catch
        error:badarg ->
            %% Failed to get from metadata,
            %% Retry by calling the server process.
            %% Most likely would fail too, but we want the same error format
            %% when calling get_leader using either atom() or pid()
            gen_server:call(Tab, get_leader)
    end;
get_leader(Server) ->
    gen_server:call(Server, get_leader).

%% Get a list of other nodes in the cluster that are connected together.
-spec other_nodes(server_ref()) -> ordsets:ordset(node()).
other_nodes(Server) ->
    lists:usort(pids_to_nodes(other_pids(Server))).

-spec get_nodes_request(server_ref()) -> request_id().
get_nodes_request(Server) ->
    gen_server:send_request(Server, get_nodes).

%% Get a list of other CETS processes that are handling this table.
-spec other_pids(server_ref()) -> servers().
other_pids(Server) ->
    cets_call:long_call(Server, other_servers).

-spec pause(server_ref()) -> pause_monitor().
pause(Server) ->
    cets_call:long_call(Server, pause).

-spec unpause(server_ref(), pause_monitor()) -> ok | {error, unknown_pause_monitor}.
unpause(Server, PauseRef) ->
    cets_call:long_call(Server, {unpause, PauseRef}).

%% Set is_leader field in the state.
%% For debugging only.
%% Setting in in the real life would break leader election logic.
-spec set_leader(server_ref(), boolean()) -> ok.
set_leader(Server, IsLeader) ->
    cets_call:long_call(Server, {set_leader, IsLeader}).

%% Waits till all pending operations are applied.
-spec sync(server_ref()) -> ok.
sync(Server) ->
    cets_call:long_call(Server, sync).

-spec ping(server_ref()) -> pong.
ping(Server) ->
    cets_call:long_call(Server, ping).

-spec info(server_ref()) -> info().
info(Server) ->
    cets_call:long_call(Server, get_info).

%% gen_server callbacks

-spec init({table_name(), start_opts()}) -> {ok, state()}.
init({Tab, Opts}) ->
    process_flag(message_queue_data, off_heap),
    Type = maps:get(type, Opts, ordered_set),
    KeyPos = maps:get(keypos, Opts, 1),
    %% Match result to prevent the Dialyzer warning
    _ = ets:new(Tab, [Type, named_table, public, {keypos, KeyPos}]),
    cets_metadata:init(Tab),
    cets_metadata:set(Tab, leader, self()),
    {ok, AckPid} = cets_ack:start_link(Tab),
    {ok, #{
        tab => Tab,
        ack_pid => AckPid,
        other_servers => [],
        %% Initial join_ref is random
        join_ref => make_ref(),
        leader => self(),
        is_leader => true,
        opts => Opts,
        backlog => [],
        pause_monitors => []
    }}.

-spec handle_call(long_msg() | {op, op()}, from(), state()) ->
    {noreply, state()} | {reply, term(), state()}.
handle_call({op, Op}, From, State = #{pause_monitors := []}) ->
    handle_op(Op, From, State),
    {noreply, State};
handle_call({op, Op}, From, State = #{pause_monitors := [_ | _], backlog := Backlog}) ->
    %% Backlog is a list of pending operations, when our server is paused.
    %% The list would be applied, once our server is unpaused.
    {noreply, State#{backlog := [{Op, From} | Backlog]}};
handle_call(other_servers, _From, State = #{other_servers := Servers}) ->
    {reply, Servers, State};
handle_call(get_nodes, _From, State = #{other_servers := Servers}) ->
    {reply, lists:usort([node() | pids_to_nodes(Servers)]), State};
handle_call(get_leader, _From, State = #{leader := Leader}) ->
    {reply, Leader, State};
handle_call(sync, From, State = #{other_servers := Servers}) ->
    %% Do spawn to avoid any possible deadlocks
    proc_lib:spawn(fun() ->
        lists:foreach(fun ping/1, Servers),
        gen_server:reply(From, ok)
    end),
    {noreply, State};
handle_call(ping, _From, State) ->
    {reply, pong, State};
handle_call(table_name, _From, State = #{tab := Tab}) ->
    {reply, {ok, Tab}, State};
handle_call(remote_dump, From, State = #{tab := Tab}) ->
    %% Do not block the main process (also reduces GC of the main process)
    proc_lib:spawn_link(fun() -> gen_server:reply(From, {ok, dump(Tab)}) end),
    {noreply, State};
handle_call({send_dump, NewPids, JoinRef, Dump}, _From, State) ->
    handle_send_dump(NewPids, JoinRef, Dump, State);
handle_call(pause, _From = {FromPid, _}, State = #{pause_monitors := Mons}) ->
    %% We monitor who pauses our server
    Mon = erlang:monitor(process, FromPid),
    {reply, Mon, State#{pause_monitors := [Mon | Mons]}};
handle_call({unpause, Ref}, _From, State) ->
    handle_unpause(Ref, State);
handle_call({set_leader, IsLeader}, _From, State) ->
    {reply, ok, State#{is_leader := IsLeader}};
handle_call(get_info, _From, State) ->
    {reply, handle_get_info(State), State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({remote_op, Op, From, AckPid, JoinRef}, State) ->
    handle_remote_op(Op, From, AckPid, JoinRef, State),
    {noreply, State};
handle_info({'DOWN', Mon, process, Pid, _Reason}, State) ->
    {noreply, handle_down(Mon, Pid, State)};
handle_info({check_server, FromPid, JoinRef}, State) ->
    handle_check_server(FromPid, JoinRef, State),
    {noreply, State};
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State = #{ack_pid := AckPid}) ->
    ok = gen_server:stop(AckPid).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal logic

-spec handle_send_dump(servers(), join_ref(), [tuple()], state()) -> {reply, ok, state()}.
handle_send_dump(NewPids, JoinRef, Dump, State = #{tab := Tab, other_servers := Servers}) ->
    ets:insert(Tab, Dump),
    Servers2 = add_servers(NewPids, Servers),
    {reply, ok, set_other_servers(Servers2, State#{join_ref := JoinRef})}.

-spec handle_down(reference(), pid(), state()) -> state().
handle_down(Mon, Pid, State = #{pause_monitors := Mons}) ->
    case lists:member(Mon, Mons) of
        true ->
            ?LOG_ERROR(#{
                what => pause_owner_crashed,
                state => State,
                paused_by_pid => Pid
            }),
            handle_unpause2(Mon, Mons, State);
        false ->
            handle_down2(Pid, State)
    end.

-spec handle_down2(pid(), state()) -> state().
handle_down2(RemotePid, State = #{other_servers := Servers, ack_pid := AckPid}) ->
    case lists:member(RemotePid, Servers) of
        true ->
            cets_ack:send_remote_down(AckPid, RemotePid),
            call_user_handle_down(RemotePid, State),
            Servers2 = lists:delete(RemotePid, Servers),
            set_other_servers(Servers2, State);
        false ->
            %% This should not happen
            ?LOG_ERROR(#{
                what => handle_down_failed,
                remote_pid => RemotePid,
                state => State
            }),
            State
    end.

%% Merge two lists of pids, create the missing monitors.
-spec add_servers(Servers, Servers) -> Servers when Servers :: servers().
add_servers(Pids, Servers) ->
    %% Ignore ourself in the list
    %% Also filter out already added servers
    OtherServers = lists:delete(self(), lists:usort(Pids)),
    NewServers = ordsets:subtract(OtherServers, Servers),
    case ordsets:intersection(OtherServers, Servers) of
        [] ->
            ok;
        Overlap ->
            %% Should not happen (cets_join checks for it)
            %% Still log it, if that happens
            ?LOG_ERROR(#{
                what => already_added,
                already_added_servers => Overlap,
                pids => Pids,
                servers => Servers
            })
    end,
    [erlang:monitor(process, Pid) || Pid <- NewServers],
    ordsets:union(NewServers, Servers).

%% Sets other_servers field, chooses the leader
-spec set_other_servers(servers(), state()) -> state().
set_other_servers(Servers, State = #{tab := Tab, ack_pid := AckPid}) ->
    %% Choose process with highest pid.
    %% Uses total ordering of terms in Erlang
    %% (so all nodes would choose the same leader).
    %% The leader node would not receive that much extra load.
    Leader = lists:max([self() | Servers]),
    IsLeader = Leader =:= self(),
    cets_metadata:set(Tab, leader, Leader),
    %% Ask the ack process to use this list of servers as the source of replies
    %% for all new cets_ack:add/2 calls
    cets_ack:set_servers(AckPid, Servers),
    State#{leader := Leader, is_leader := IsLeader, other_servers := Servers}.

-spec pids_to_nodes([pid()]) -> [node()].
pids_to_nodes(Pids) ->
    lists:map(fun node/1, Pids).

-spec ets_delete_keys(table_name(), [term()]) -> ok.
ets_delete_keys(Tab, Keys) ->
    [ets:delete(Tab, Key) || Key <- Keys],
    ok.

-spec ets_delete_objects(table_name(), [tuple()]) -> ok.
ets_delete_objects(Tab, Objects) ->
    [ets:delete_object(Tab, Object) || Object <- Objects],
    ok.

%% Handle operation from a remote node
-spec handle_remote_op(op(), from(), ack_pid(), join_ref(), state()) -> ok.
handle_remote_op(Op, From, AckPid, JoinRef, State = #{join_ref := JoinRef}) ->
    do_op(Op, State),
    cets_ack:ack(AckPid, From, self());
handle_remote_op(Op, From, AckPid, RemoteJoinRef, #{join_ref := JoinRef}) ->
    ?LOG_ERROR(#{
        what => drop_remote_op,
        from => From,
        remote_join_ref => RemoteJoinRef,
        join_ref => JoinRef,
        op => Op
    }),
    %% We still need to reply to the remote process so it could stop waiting
    cets_ack:ack(AckPid, From, self()).

%% Apply operation for one local table only
-spec do_op(op(), state()) -> ok | boolean().
do_op(Op, #{tab := Tab}) ->
    do_table_op(Op, Tab).

-spec do_table_op(op(), table_name()) -> ok | boolean().
do_table_op({insert, Rec}, Tab) ->
    ets:insert(Tab, Rec);
do_table_op({delete, Key}, Tab) ->
    ets:delete(Tab, Key);
do_table_op({delete_object, Object}, Tab) ->
    ets:delete_object(Tab, Object);
do_table_op({insert_many, Recs}, Tab) ->
    ets:insert(Tab, Recs);
do_table_op({delete_many, Keys}, Tab) ->
    ets_delete_keys(Tab, Keys);
do_table_op({delete_objects, Objects}, Tab) ->
    ets_delete_objects(Tab, Objects);
do_table_op({insert_new, Rec}, Tab) ->
    ets:insert_new(Tab, Rec).

%% Handle operation locally and replicate it across the cluster
-spec handle_op(op(), from(), state()) -> ok.
handle_op({leader_op, Op}, From, State) ->
    handle_leader_op(Op, From, State);
handle_op(Op, From, State) ->
    do_op(Op, State),
    replicate(Op, From, State).

-spec handle_leader_op(op(), from(), state()) -> ok.
handle_leader_op(Op, From, State = #{is_leader := true}) ->
    case do_op(Op, State) of
        %% Skip the replication - insert_new returns false.
        false ->
            gen_server:reply(From, {error, rejected}),
            ok;
        true ->
            replicate(Op, From, State)
    end;
handle_leader_op(Op, From, State = #{leader := Leader}) ->
    %% Reject operation - not a leader
    gen_server:reply(From, {error, {wrong_leader, Leader}}),
    %% Call an user defined callback to notify about the error
    handle_wrong_leader(Op, From, State).

-spec replicate(op(), from(), state()) -> ok.
replicate(_Op, From, #{other_servers := []}) ->
    %% Skip replication
    gen_server:reply(From, ok);
replicate(Op, From, #{ack_pid := AckPid, other_servers := Servers, join_ref := JoinRef}) ->
    cets_ack:add(AckPid, From),
    RemoteOp = {remote_op, Op, From, AckPid, JoinRef},
    [send_remote_op(Server, RemoteOp) || Server <- Servers],
    %% AckPid would call gen_server:reply(From, ok) once all the remote servers reply
    ok.

-spec send_remote_op(server_pid(), remote_op()) -> ok.
send_remote_op(RemotePid, RemoteOp) ->
    erlang:send(RemotePid, RemoteOp, [noconnect]),
    ok.

-spec apply_backlog(state()) -> state().
apply_backlog(State = #{backlog := Backlog}) ->
    [handle_op(Op, From, State) || {Op, From} <- lists:reverse(Backlog)],
    State#{backlog := []}.

%% We support multiple pauses
%% Only when all pause requests are unpaused we continue
-spec handle_unpause(pause_monitor(), state()) -> {reply, Reply, state()} when
    Reply :: ok | {error, unknown_pause_monitor}.
handle_unpause(Mon, State = #{pause_monitors := Mons}) ->
    case lists:member(Mon, Mons) of
        true ->
            {reply, ok, handle_unpause2(Mon, Mons, State)};
        false ->
            {reply, {error, unknown_pause_monitor}, State}
    end.

-spec handle_unpause2(pause_monitor(), [pause_monitor()], state()) -> state().
handle_unpause2(Mon, Mons, State) ->
    erlang:demonitor(Mon, [flush]),
    Mons2 = lists:delete(Mon, Mons),
    State2 = State#{pause_monitors := Mons2},
    case Mons2 of
        [] ->
            send_check_servers(State2),
            apply_backlog(State2);
        _ ->
            State2
    end.

-spec send_check_servers(state()) -> ok.
send_check_servers(#{join_ref := JoinRef, other_servers := OtherPids}) ->
    [send_check_server(Pid, JoinRef) || Pid <- OtherPids],
    ok.

%% Send check_server before sending any new remote_op messages,
%% so the remote node has a chance to disconnect from us
%% (i.e. remove our pid from other_servers list and not allow remote ops)
-spec send_check_server(pid(), reference()) -> ok.
send_check_server(Pid, JoinRef) ->
    Pid ! {check_server, self(), JoinRef},
    ok.

handle_check_server(_FromPid, JoinRef, #{join_ref := JoinRef}) ->
    ok;
handle_check_server(FromPid, RemoteJoinRef, #{join_ref := JoinRef}) ->
    ?LOG_WARNING(#{
        what => cets_check_server_failed,
        text => <<"Disconnect the remote server">>,
        remote_pid => FromPid,
        remote_join_ref => RemoteJoinRef,
        join_ref => JoinRef
    }),
    %% Ask the remote server to disconnect from us
    Reason = {check_server_failed, {RemoteJoinRef, JoinRef}},
    FromPid ! {'DOWN', make_ref(), process, self(), Reason},
    ok.

-spec handle_get_info(state()) -> info().
handle_get_info(
    #{
        tab := Tab,
        other_servers := Servers,
        ack_pid := AckPid,
        join_ref := JoinRef,
        opts := Opts
    }
) ->
    #{
        table => Tab,
        nodes => lists:usort(pids_to_nodes([self() | Servers])),
        size => ets:info(Tab, size),
        memory => ets:info(Tab, memory),
        ack_pid => AckPid,
        join_ref => JoinRef,
        opts => Opts
    }.

%% Cleanup
-spec call_user_handle_down(server_pid(), state()) -> ok.
call_user_handle_down(RemotePid, #{tab := Tab, opts := Opts}) ->
    case Opts of
        #{handle_down := F} ->
            FF = fun() -> F(#{remote_pid => RemotePid, table => Tab}) end,
            Info = #{
                task => call_user_handle_down,
                table => Tab,
                remote_pid => RemotePid,
                remote_node => node(RemotePid)
            },
            %% Errors would be logged inside run_tracked
            catch cets_long:run_tracked(Info, FF);
        _ ->
            ok
    end.

-spec handle_wrong_leader(op(), from(), state()) -> ok.
handle_wrong_leader(Op, From, #{opts := #{handle_wrong_leader := F}}) ->
    %% It is used for debugging/logging
    %% Do not do anything heavy here
    catch F(#{from => From, op => Op, server => self()}),
    ok;
handle_wrong_leader(_Op, _From, _State) ->
    ok.

-type start_error() :: bag_with_conflict_handler.
-spec check_opts(start_opts()) -> [start_error()].
check_opts(#{handle_conflict := _, type := bag}) ->
    [bag_with_conflict_handler];
check_opts(_) ->
    [].
