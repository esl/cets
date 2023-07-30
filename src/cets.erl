%% Very simple multinode ETS writer.
%% One file, everything is simple, but we don't silently hide race conditions.
%% No transactions support.
%% We don't use rpc module, because it is a single gen_server.
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
    delete/2,
    delete_many/2,
    delete_object/2,
    delete_objects/2,
    dump/1,
    remote_dump/1,
    remote_or_local_dump/1,
    send_dump/6,
    apply_dump/2,
    table_name/1,
    other_nodes/1,
    other_pids/1,
    make_aliases_for/2,
    pause/1,
    unpause/2,
    is_paused/2,
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
    delete/2,
    delete_many/2,
    delete_object/2,
    delete_objects/2,
    dump/1,
    remote_dump/1,
    unpause/2,
    sync/1,
    ping/1,
    info/1,
    other_nodes/1,
    insert_request/2,
    insert_many_request/2,
    delete_request/2,
    delete_many_request/2,
    delete_object_request/2,
    delete_objects_request/2,
    wait_response/2
]).

-include_lib("kernel/include/logger.hrl").

-type server_ref() ::
    pid()
    | atom()
    | {local, atom()}
    | {global, term()}
    | {via, module(), term()}.
-type server_num() :: pos_integer().
-type server_mask() :: integer().
-type request_id() :: reference().
-type op() ::
    {insert, tuple()}
    | {delete, term()}
    | {delete_object, term()}
    | {insert_many, [tuple()]}
    | {delete_many, [term()]}
    | {delete_objects, [term()]}.
-type called_from() :: Alias :: reference() | {Pid :: pid, Mon :: reference()}.
-type backlog_entry() :: {called_from(), op()}.
-type table_name() :: atom().
-type pause_monitor() :: reference().
-type server_tuple() :: {pid(), Monitor :: reference(), Dest :: reference()}.
-type server_nums() :: map().
-type state() :: #{
    %% ETS table to write data into
    tab := table_name(),
    %% Process, that would receive acks from the remote nodes
    ack_pid := pid(),
    %% Each node in cluster has an unique integer id.
    %% Assigned in cets_join when joining the cluster.
    %% Could be reassigned during another join.
    server_num := server_num(),
    %% Mask to remove our server_num from a bitmask.
    %% Used in cets_bits when collecting acks.
    server_mask := server_mask(),
    %% Map to get server_num of other servers in the cluster.
    %% Assigned during the cluster join.
    server_nums := server_nums(),
    %% A list of remote servers.
    %% Each server has a pid, a monitor and an alias to send messages.
    %% Assigned during the cluster join.
    other_servers := [server_tuple()],
    %% Bitfield with ones set for the remote server_nums.
    remote_bits := non_neg_integer(),
    %% Pids from other_servers list.
    just_pids := [pid()],
    %% Aliases from other_servers list.
    just_dests := [reference()],
    opts := start_opts(),
    %% Pending operations collected when we are paused.
    backlog := [backlog_entry()],
    %% Who are blocking us from writing into the table.
    pause_monitors := [pause_monitor()],
    %% Optional
    %% We store dump between send_dump and apply_dump calls.
    pending_dump := send_dump_msg() | none(),
    pending_aliases := [{pid(), reference()}],
    %% Reference assigned when joining.
    %% All nodes in the cluster should have the same last_applied_dump_ref.
    %% Verified in handle_check_server function.
    last_applied_dump_ref := reference()
}.

-type send_dump_msg() ::
    {send_dump, DumpRef :: reference(), PauseRef :: reference(), Nums :: server_nums(),
        NewServers :: [server_tuple()], Dump :: [tuple()]}.

-type long_msg() ::
    pause
    | ping
    | remote_dump
    | sync
    | table_name
    | get_info
    | other_pids
    | {make_aliases_for, [pid()]}
    | {unpause, reference()}
    | {is_paused, reference()}
    | send_dump_msg()
    | {apply_dump, DumpRef :: reference()}.

-type info() :: #{
    table := table_name(),
    nodes := [node()],
    size := non_neg_integer(),
    memory := non_neg_integer(),
    ack_pid := pid(),
    opts := start_opts()
}.

-type handle_down_fun() :: fun((#{remote_pid := pid(), table := table_name()}) -> ok).
-type handle_conflict_fun() :: fun((tuple(), tuple()) -> tuple()).
-type start_opts() :: #{
    type => ordered_set | bag,
    keypos => non_neg_integer(),
    handle_down => handle_down_fun(),
    handle_conflict => handle_conflict_fun()
}.

-export_type([request_id/0, op/0, server_ref/0, long_msg/0, info/0, table_name/0, called_from/0]).

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
-spec start(table_name(), start_opts()) -> {ok, pid()}.
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

-spec dump(table_name()) -> {ok, Records :: [tuple()]}.
dump(Tab) when is_atom(Tab) ->
    {ok, ets:tab2list(Tab)}.

-spec remote_dump(server_ref()) -> {ok, Records :: [tuple()]}.
remote_dump(Server) ->
    cets_call:long_call(Server, remote_dump).

-spec remote_or_local_dump(pid()) -> {ok, Records :: [tuple()]}.
remote_or_local_dump(Pid) when node(Pid) =:= node() ->
    {ok, Tab} = cets:table_name(Pid),
    %% Reduce copying
    dump(Tab);
remote_or_local_dump(Pid) ->
    %% We actually need to ask the remote process
    remote_dump(Pid).

-spec table_name(server_ref()) -> {ok, table_name()}.
table_name(Tab) when is_atom(Tab) ->
    {ok, Tab};
table_name(Server) ->
    cets_call:long_call(Server, table_name).

-spec send_dump(server_ref(), reference(), reference(), server_nums(), [server_tuple()], [tuple()]) ->
    ok.
send_dump(Server, DumpRef, PauseRef, Nums, NewServers, OurDump) ->
    Info = #{msg => send_dump, count => length(OurDump)},
    cets_call:long_call(Server, {send_dump, DumpRef, PauseRef, Nums, NewServers, OurDump}, Info).

-spec apply_dump(server_ref(), reference()) -> ok.
apply_dump(Server, Ref) ->
    Info = #{msg => apply_dump},
    cets_call:long_call(Server, {apply_dump, Ref}, Info).

%% Only the node that owns the data could update/remove the data.
%% Ideally, Key should contain inserter node info so cleaning and merging is simplified.
-spec insert(server_ref(), tuple()) -> ok.
insert(Server, Rec) when is_tuple(Rec) ->
    cets_call:sync_operation(Server, {insert, Rec}).

-spec insert_many(server_ref(), list(tuple())) -> ok.
insert_many(Server, Records) when is_list(Records) ->
    cets_call:sync_operation(Server, {insert_many, Records}).

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

-spec wait_response(request_id(), non_neg_integer() | infinity) -> ok.
wait_response(Mon, Timeout) ->
    case gen_server:wait_response(Mon, Timeout) of
        {reply, ok} -> ok;
        Other -> error(Other)
    end.

-spec make_aliases_for(server_ref(), [server_ref()]) -> [{server_ref(), reference()}].
make_aliases_for(Server, RemotePids) ->
    cets_call:long_call(Server, {make_aliases_for, RemotePids}).

%% Get a list of other nodes in the cluster that are connected together.
-spec other_nodes(server_ref()) -> [node()].
other_nodes(Server) ->
    lists:usort(pids_to_nodes(other_pids(Server))).

%% Get a list of other CETS processes that are handling this table.
-spec other_pids(server_ref()) -> [pid()].
other_pids(Server) ->
    cets_call:long_call(Server, other_pids).

-spec pause(server_ref()) -> pause_monitor().
pause(Server) ->
    cets_call:long_call(Server, pause).

-spec unpause(server_ref(), pause_monitor()) -> ok | {error, unknown_pause_monitor}.
unpause(Server, PauseRef) ->
    cets_call:long_call(Server, {unpause, PauseRef}).

-spec is_paused(server_ref(), pause_monitor()) -> boolean().
is_paused(Server, PauseRef) ->
    cets_call:long_call(Server, {is_paused, PauseRef}).

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
    %% While this process could produce a lot of messages,
    %% blocking it would not help on the real system
    %% (probably any blocking of this process would make CPU/memory usage higher)
    %% Blocking this process could reduce a bit of pressure on the overloaded dist
    %% connection, but we would have to send the data there anyway.
    %% Instead of being blocked we could process remote_op messages and reduce our
    %% message queue.
    %% It is supported starting from OTP 25.3.
    catch process_flag(async_dist, true),
    process_flag(message_queue_data, off_heap),
    Type = maps:get(type, Opts, ordered_set),
    KeyPos = maps:get(keypos, Opts, 1),
    %% Match result to prevent the Dialyzer warning
    _ = ets:new(Tab, [Type, named_table, public, {keypos, KeyPos}, {read_concurrency, true}]),
    AckName = list_to_atom(atom_to_list(Tab) ++ "_ack"),
    {ok, AckPid} = cets_ack:start_link(AckName),
    {ok, #{
        tab => Tab,
        ack_pid => AckPid,
        server_num => 0,
        server_mask => cets_bits:unset_flag_mask(0),
        server_nums => #{self() => 0},
        remote_bits => 0,
        other_servers => [],
        just_pids => [],
        just_dests => [],
        opts => Opts,
        backlog => [],
        pause_monitors => [],
        pending_aliases => [],
        last_applied_dump_ref => make_ref()
    }}.

-spec handle_call(term(), called_from(), state()) ->
    {noreply, state()} | {reply, term(), state()}.
handle_call({op, Msg}, From, State = #{pause_monitors := []}) ->
    handle_op(cets_call:prepare_from(From), Msg, State),
    {noreply, State};
handle_call(
    {op, Msg}, From, State = #{pause_monitors := [_ | _], backlog := Backlog}
) ->
    %% Backlog is a list of pending operation, when our server is paused.
    %% The list would be applied, once our server is unpaused.
    {noreply, State#{backlog := [{cets_call:prepare_from(From), Msg} | Backlog]}};
handle_call(other_pids, _From, State = #{just_pids := Pids}) ->
    {reply, Pids, State};
handle_call({make_aliases_for, Pids}, _From, State = #{pending_aliases := PendingAliases}) ->
    %% Create channels used to deliver remote_op messages
    Aliases = [{Pid, erlang:monitor(process, Pid, [{alias, demonitor}])} || Pid <- Pids],
    {reply, Aliases, State#{pending_aliases := Aliases ++ PendingAliases}};
handle_call(sync, From, State = #{just_pids := Pids}) ->
    %% Do spawn to avoid any possible deadlocks
    proc_lib:spawn(fun() ->
        lists:foreach(fun ping/1, Pids),
        gen_server:reply(From, ok)
    end),
    {noreply, State};
handle_call(ping, _From, State) ->
    {reply, pong, State};
handle_call(table_name, _From, State = #{tab := Tab}) ->
    {reply, {ok, Tab}, State};
handle_call(remote_dump, From, State = #{tab := Tab}) ->
    %% Do not block the main process (also reduces GC of the main process)
    proc_lib:spawn_link(fun() -> gen_server:reply(From, dump(Tab)) end),
    {noreply, State};
handle_call({send_dump, _DumpRef, PauseRef, _Nums, _NewServers, _Dump} = Msg, _From, State) ->
    handle_send_dump(PauseRef, Msg, State);
handle_call({apply_dump, Ref}, _From, State) ->
    handle_apply_dump(Ref, State);
handle_call(pause, _From = {FromPid, _}, State = #{pause_monitors := Mons}) ->
    %% We monitor who pauses our server
    Mon = erlang:monitor(process, FromPid),
    {reply, Mon, State#{pause_monitors := [Mon | Mons]}};
handle_call({unpause, Ref}, _From, State) ->
    handle_unpause(Ref, State);
handle_call({is_paused, Ref}, _From, State = #{pause_monitors := Mons}) ->
    {reply, lists:member(Ref, Mons), State};
handle_call(get_info, _From, State) ->
    handle_get_info(State).

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({check_server, Source, Mon, Dest, DumpRef}, State) ->
    handle_check_server(Source, Mon, Dest, DumpRef, State),
    {noreply, State};
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({remote_op, _Dest, Alias, ReplyTo, Msg}, State) ->
    handle_remote_op(Alias, Msg, ReplyTo, State),
    {noreply, State};
handle_info({'DOWN', Mon, process, Pid, Reason}, State) ->
    handle_down(Mon, Pid, Reason, State);
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State = #{ack_pid := AckPid}) ->
    ok = gen_server:stop(AckPid).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal logic

handle_send_dump(_PauseRef, _Msg, State = #{pending_dump := _}) ->
    ?LOG_ERROR(#{what => already_have_pending_dump}),
    {reply, {error, already_have_pending_dump}, State};
handle_send_dump(PauseRef, Msg, State = #{pause_monitors := PauseRefs}) ->
    case lists:member(PauseRef, PauseRefs) of
        true ->
            ?LOG_INFO(#{what => received_pending_dump}),
            {reply, ok, State#{pending_dump => Msg}};
        false ->
            {reply, {error, unknown_pause_monitor}, State}
    end.

handle_apply_dump(
    DumpRef,
    State = #{
        tab := Tab,
        pending_dump := {send_dump, DumpRef, _PauseRef, Nums, NewServers, Dump},
        ack_pid := AckPid
    }
) ->
    ets:insert(Tab, Dump),
    Num = maps:get(self(), Nums),
    State2 = maps:remove(pending_dump, State#{
        server_num := Num,
        server_mask := cets_bits:unset_flag_mask(Num),
        server_nums := Nums,
        last_applied_dump_ref := DumpRef
    }),
    %% We need to clean cets_ack process to avoid possible infinite
    %% gen_server:wait_response/2 calls from the client.
    %% We don't expect a lot of records in cets_ack once we reached
    %% apply_dump step.
    %% We have to do it because we set new server_nums during the join procedure.
    cets_ack:erase(AckPid),
    {reply, ok, set_servers(NewServers, State2)};
handle_apply_dump(_Ref, State) ->
    {reply, {error, unknown_dump_ref}, State}.

remove_server(Mon, State = #{other_servers := Servers}) ->
    Servers2 = lists:keydelete(Mon, 2, Servers),
    set_servers(Servers2, State).

set_servers(Servers, State) ->
    Pids = servers_to_pids(Servers),
    Dests = servers_to_dests(Servers),
    Bits = make_remote_bits(Pids, State),
    State#{other_servers := Servers, just_pids := Pids, just_dests := Dests, remote_bits := Bits}.

%% Make a bitmask with bits set to 1 for still alive remote servers
make_remote_bits(Pids, #{server_nums := Nums}) ->
    RemoteNums = [Num || {Pid, Num} <- maps:to_list(Nums), lists:member(Pid, Pids)],
    cets_bits:set_flags(RemoteNums, 0).

handle_down(Mon, Pid, Reason, State = #{pause_monitors := Mons}) ->
    case lists:member(Mon, Mons) of
        true ->
            case Reason of
                normal ->
                    ok;
                _ ->
                    ?LOG_ERROR(#{
                        what => pause_owner_crashed,
                        reason => Reason,
                        state => State,
                        paused_by_pid => Pid
                    })
            end,
            {reply, ok, State2} = handle_unpause(Mon, State),
            {noreply, State2};
        false ->
            handle_down2(Mon, Pid, State)
    end.

handle_down2(Mon, RemotePid, State = #{ack_pid := AckPid, other_servers := Servers}) ->
    case lists:keymember(Mon, 2, Servers) of
        true ->
            Num = server_pid_to_server_num(RemotePid, State),
            cets_ack:send_remote_down(AckPid, Num),
            call_user_handle_down(RemotePid, State),
            {noreply, remove_server(Mon, State)};
        false ->
            %% This should not happen
            ?LOG_ERROR(#{
                what => handle_down_failed,
                remote_pid => RemotePid,
                state => State
            }),
            {noreply, State}
    end.

pids_to_nodes(Pids) ->
    lists:map(fun node/1, Pids).

ets_delete_keys(Tab, Keys) ->
    [ets:delete(Tab, Key) || Key <- Keys],
    ok.

ets_delete_objects(Tab, Objects) ->
    [ets:delete_object(Tab, Object) || Object <- Objects],
    ok.

reply_updated(Alias, ReplyTo, #{server_mask := Mask}) ->
    cets_ack:ack(ReplyTo, Alias, Mask).

send_to_remote(RemoteAlias, Msg) ->
    erlang:send(RemoteAlias, Msg, [noconnect]).

%% Handle operation from a remote node
handle_remote_op(Alias, Msg, ReplyTo, State) ->
    do_op(Msg, State),
    reply_updated(Alias, ReplyTo, State).

%% Apply operation for one local table only
do_op(Msg, #{tab := Tab}) ->
    do_table_op(Msg, Tab).

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
    ets_delete_objects(Tab, Objects).

%% Handle operation locally and replicate it across the cluster
handle_op(From, Msg, State) ->
    do_op(Msg, State),
    replicate(From, Msg, State).

replicate(From, _Msg, #{remote_bits := 0}) ->
    %% Skip replication
    cets_call:reply(From);
replicate(From, Msg, #{ack_pid := AckPid, just_dests := Dests, remote_bits := Bits}) ->
    cets_ack:add(AckPid, From, Bits),
    [send_to_remote(Dest, {remote_op, Dest, From, AckPid, Msg}) || Dest <- Dests],
    %% AckPid would call cets_call:reply(From) ones all the remote servers reply
    ok.

apply_backlog(State = #{backlog := Backlog}) ->
    [handle_op(From, Msg, State) || {From, Msg} <- lists:reverse(Backlog)],
    State#{backlog := []}.

%% We support multiple pauses
%% Only when all pause requests are unpaused we continue
handle_unpause(Mon, State = #{pause_monitors := Mons}) ->
    case lists:member(Mon, Mons) of
        true ->
            handle_unpause2(Mon, Mons, State);
        false ->
            {reply, {error, unknown_pause_monitor}, State}
    end.

handle_unpause2(Mon, Mons, State) ->
    erlang:demonitor(Mon, [flush]),
    Mons2 = lists:delete(Mon, Mons),
    State2 = State#{pause_monitors := Mons2},
    State4 =
        case Mons2 of
            [] ->
                State3 = check_servers(State2),
                apply_backlog(State3);
            _ ->
                State2
        end,
    {reply, ok, State4}.

-spec handle_get_info(state()) -> {reply, info(), state()}.
handle_get_info(
    State = #{
        tab := Tab,
        just_pids := Pids,
        just_dests := Dests,
        ack_pid := AckPid,
        pending_aliases := PendingAliases,
        opts := Opts
    }
) ->
    Info = #{
        table => Tab,
        nodes => lists:usort(pids_to_nodes([self() | Pids])),
        server_to_dest => maps:from_list(lists:zip(Pids, Dests)),
        size => ets:info(Tab, size),
        memory => ets:info(Tab, memory),
        ack_pid => AckPid,
        pending_aliases => PendingAliases,
        opts => Opts
    },
    {reply, Info, State}.

%% Cleanup
call_user_handle_down(RemotePid, _State = #{tab := Tab, opts := Opts}) ->
    case Opts of
        #{handle_down := F} ->
            FF = fun() -> F(#{remote_pid => RemotePid, table => Tab}) end,
            Info = #{
                task => call_user_handle_down,
                table => Tab,
                remote_pid => RemotePid,
                remote_node => node(RemotePid)
            },
            cets_long:run_safely(Info, FF);
        _ ->
            ok
    end.

-type start_error() :: bag_with_conflict_handler.
-spec check_opts(start_opts()) -> [start_error()].
check_opts(#{handle_conflict := _, type := bag}) ->
    [bag_with_conflict_handler];
check_opts(_) ->
    [].

-spec servers_to_pids(server_tuple()) -> [pid()].
servers_to_pids(Servers) ->
    [Pid || {Pid, _Mon, _Dest} <- Servers].

-spec servers_to_dests(server_tuple()) -> [reference()].
servers_to_dests(Servers) ->
    [Dest || {_Pid, _Mon, Dest} <- Servers].

server_pid_to_server_num(Pid, #{server_nums := Nums}) ->
    maps:get(Pid, Nums).

%% First thing to do after unpause is to send message check_server.
%% So the remote server has a chance to unalias (block) us.
%% Send to other servers an async request to verify that our Mon and Dest are valid
check_servers(State = #{other_servers := Servers, last_applied_dump_ref := DumpRef}) ->
    [
        gen_server:cast(Pid, {check_server, self(), Mon, Dest, DumpRef})
     || {Pid, Mon, Dest} <- Servers
    ],
    %% Reset dump if any before unpause
    State2 = maps:remove(pending_dump, State),
    check_pending_aliases(State2).

check_pending_aliases(State = #{pending_aliases := Aliases}) ->
    lists:foreach(
        fun({Pid, Alias}) ->
            case is_known_monitor(Alias, State) of
                true ->
                    ok;
                false ->
                    %% Alias was made by make_aliases_for but for some reason
                    %% it is not in the list of other_servers
                    ?LOG_ERROR(#{what => destroy_alias, remote_pid => Pid, alias => Alias}),
                    destroy_alias(Pid, Alias)
            end
        end,
        Aliases
    ),
    State#{pending_aliases := []}.

is_known_monitor(Mon, #{other_servers := Servers}) ->
    lists:keymember(Mon, 2, Servers).

%% Forces the remote server to disconnect if we don't know his alias
handle_check_server(Source, Mon, Dest, DumpRef, State = #{last_applied_dump_ref := OurDumpRef}) ->
    case {is_known_monitor(Dest, State), DumpRef} of
        {true, OurDumpRef} ->
            ok;
        _ ->
            destroy_alias(Source, Dest),
            %% Simulate disconnect for this alias.
            %% This would prevent us from the partially applied dumps.
            %% This could happen if send_dump failed for this or for the remote node.
            Source ! {'DOWN', Mon, process, self(), check_server_failed},
            ok
    end.

destroy_alias(Source, Dest) when is_pid(Source), is_reference(Dest) ->
    %% Prevent from receiving remote_ops.
    %% Though messages that are already in our message box would not get rejected.
    case erlang:unalias(Dest) of
        true ->
            flush_remote_ops(Dest);
        false ->
            ?LOG_ERROR(#{
                what => unknown_check_server_alias, alias => Dest, from_pid => Source
            })
    end,
    ok.

%% Reject messages to the alias which are already in our message box
flush_remote_ops(Dest) ->
    receive
        {remote_op, Dest, _Alias, _ReplyTo, _Msg} ->
            flush_remote_ops(Dest)
    after 0 ->
        ok
    end.
