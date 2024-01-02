%% @doc Main CETS module.
%%
%% CETS stores data in-memory. Writes are replicated on all nodes across the cluster.
%% Reads are done locally.
%%
%% This module contains functions to write data. To read data, use an `ets' module
%% from the Erlang/OTP.
%%
%% The preferred key format is a node specific key (i.e. a key should contain the inserter
%% node name or a pid). This should simplify the cleaning logic. This also avoids the key conflicts
%% during the cluster join.
%%
%% A random key is a good option too. But the cleaning logic in the case of a netsplit could
%% be tricky. Also, if you update a record with a random key, you have to provide
%% a `handle_conflict' function on startup (because two segments in the cluster could
%% contain a new and an old version of the record, so the records would be overwritten incorrectly).
%%
%% Be careful, CETS does not protect you from records being overwritten.
%% It is a good option to provide `handle_conflict' function as a start argument, so you could
%% choose, which version of the record to keep, if there are two versions present in the different
%% cluster segments.
%%
%% Often we need to insert some key, if it is not presented yet. Use `insert_new' for this, it would
%% use a single node to serialize inserts.
%%
%% Check MongooseIM code for examples of usage of this module.
-module(cets).
-behaviour(gen_server).

-export([
    start/2,
    stop/1,
    insert/2,
    insert_many/2,
    insert_new/2,
    insert_new_or_lookup/2,
    insert_serial/2,
    delete/2,
    delete_many/2,
    delete_object/2,
    delete_objects/2,
    dump/1,
    remote_dump/1,
    send_dump/5,
    table_name/1,
    other_nodes/1,
    get_nodes_request/1,
    other_pids/1,
    pause/1,
    unpause/2,
    get_leader/1,
    set_leader/2,
    ping_all/1,
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
    insert_new_or_lookup/2,
    insert_serial/2,
    delete/2,
    delete_many/2,
    delete_object/2,
    delete_objects/2,
    pause/1,
    unpause/2,
    get_leader/1,
    set_leader/2,
    ping_all/1,
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
%% CETS pid.

-type server_ref() ::
    server_pid()
    | atom()
    | {local, atom()}
    | {global, term()}
    | {via, module(), term()}.
%% CETS Process Reference.

-type request_id() :: gen_server:request_id().
%% Request Reference.

-type from() :: gen_server:from().
%% gen_server's caller.

-type join_ref() :: cets_join:join_ref().
%% An unique ID assigned during the table join attempt.

-type ack_pid() :: cets_ack:ack_pid().
%% Pid of the helper process that tracks unacked writes.

-type op() ::
    {insert, tuple()}
    | {delete, term()}
    | {delete_object, term()}
    | {insert_many, [tuple()]}
    | {delete_many, [term()]}
    | {delete_objects, [term()]}
    | {insert_new, tuple()}
    | {insert_new_or_lookup, tuple()}
    | {leader_op, op()}.
%% Write operation type.

-type remote_op() ::
    {remote_op, Op :: op(), From :: from(), AckPid :: ack_pid(), JoinRef :: join_ref()}.
%% Message broadcasted to other nodes.

-type backlog_entry() :: {op(), from()}.
%% Delayed operation.

-type table_name() :: atom().
%% ETS table name (and the process server name).

-type pause_monitor() :: reference().
%% Reference returned from `pause/1'.

-type servers() :: ordsets:ordset(server_pid()).
%% Ordered list of server pids.

-type node_down_event() :: #{node => node(), pid => pid(), reason => term()}.

-type state() :: #{
    tab := table_name(),
    keypos := pos_integer(),
    ack_pid := ack_pid(),
    join_ref := join_ref(),
    %% Updated by set_other_servers/2 function only
    other_servers := servers(),
    leader := server_pid(),
    is_leader := boolean(),
    opts := start_opts(),
    backlog := [backlog_entry()],
    pause_monitors := [pause_monitor()],
    node_down_history := [node_down_event()]
}.
%% gen_server's state.

-type long_msg() ::
    pause
    | ping
    | remote_dump
    | ping_all
    | table_name
    | get_info
    | other_servers
    | get_nodes
    | {unpause, reference()}
    | get_leader
    | {set_leader, boolean()}
    | {send_dump, servers(), join_ref(), pause_monitor(), [tuple()]}.
%% Types of gen_server calls.

-type info() :: #{
    table := table_name(),
    nodes := [node()],
    other_servers := [pid()],
    size := non_neg_integer(),
    memory := non_neg_integer(),
    ack_pid := ack_pid(),
    join_ref := join_ref(),
    opts := start_opts(),
    node_down_history := [node_down_event()],
    pause_monitors := [pause_monitor()]
}.
%% Status information returned `info/1'.

-type handle_down_fun() :: fun((#{remote_pid := server_pid(), table := table_name()}) -> ok).
%% Handler function which is called when the remote node goes down.

-type handle_conflict_fun() :: fun((tuple(), tuple()) -> tuple()).
%% Handler function which is called when we need to choose which record to keep during joining.

-type handle_wrong_leader() :: fun((#{from := from(), op := op(), server := server_pid()}) -> ok).
%% Handler function which is called when a leader operation is received by a non-leader (for debugging).

-type start_opts() :: #{
    type => ordered_set | bag,
    keypos => non_neg_integer(),
    handle_down => handle_down_fun(),
    handle_conflict => handle_conflict_fun(),
    handle_wrong_leader => handle_wrong_leader()
}.
%% Options for `start/2' function.

-type response_return() :: {reply, Reply :: term()} | {error, {_, _}} | timeout.
%% Response return from `gen_server''s API. `Reply' is usually ok.

-type response_timeout() :: timeout() | {abs, integer()}.
%% Timeout to wait for response.

-export_type([
    request_id/0,
    op/0,
    server_pid/0,
    server_ref/0,
    long_msg/0,
    info/0,
    table_name/0,
    pause_monitor/0,
    servers/0,
    response_return/0,
    response_timeout/0
]).

%% API functions

%% @doc Start a process serving an ETS table.
%%
%% The process would be registered under `table_name()' name.
%%
%% Options:
%%
%% - `handle_down = fun(#{remote_pid := Pid, table := Tab})'
%%
%%   Called when a remote node goes down. This function is called on all nodes
%%   in the remaining partition, so you should call the remote nodes
%%   from this function. Otherwise a circular locking could happen.
%%   i.e. any functions that replicate changes are not allowed (i.e. `cets:insert/2',
%%   `cets:remove/2' and so on).
%%   Use `ets' module to handle the cleaning (i.e. `ets:match_delete/2').
%%   Use spawn to make a new async process if you need to update the data on the
%%   remote nodes, but it could cause an improper cleaning due to the race conditions.
%%
%% - `handle_conflict = fun(Record1, Record2) -> NewRecord'
%%
%%   Called when two records have the same key when clustering.
%%   `NewRecord' would be the record CETS would keep in the table under the key.
%%   Does not work for bags.
%%
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

%% @doc Stops a CETS server.
-spec stop(server_ref()) -> ok.
stop(Server) ->
    gen_server:stop(Server).

%% @doc Gets all records from a local ETS table.
-spec dump(table_name()) -> Records :: [tuple()].
dump(Tab) ->
    ets:tab2list(Tab).

%% @doc Gets all records from a remote ETS table.
-spec remote_dump(server_ref()) -> {ok, Records :: [tuple()]}.
remote_dump(Server) ->
    cets_call:long_call(Server, remote_dump).

%% @doc Returns a table name, that the server is serving.
-spec table_name(server_ref()) -> {ok, table_name()}.
table_name(Tab) when is_atom(Tab) ->
    {ok, Tab};
table_name(Server) ->
    cets_call:long_call(Server, table_name).

%% Sends dump, used in `cets_join'.
%% @private
-spec send_dump(server_ref(), servers(), join_ref(), pause_monitor(), [tuple()]) ->
    ok | {error, ignored}.
send_dump(Server, NewPids, JoinRef, PauseRef, OurDump) ->
    Info = #{msg => send_dump, join_ref => JoinRef, count => length(OurDump)},
    cets_call:long_call(Server, {send_dump, NewPids, JoinRef, PauseRef, OurDump}, Info).

%% @doc Inserts (or overwrites) a tuple into a table.
%%
%% Only the node that owns the data could update/remove the data.
%% Ideally, Key should contain inserter node info so cleaning and merging is simplified.
-spec insert(server_ref(), tuple()) -> ok.
insert(Server, Rec) when is_tuple(Rec) ->
    cets_call:sync_operation(Server, {insert, Rec}).

%% @doc Inserts (or overwrites) several tuples into a table.
-spec insert_many(server_ref(), list(tuple())) -> ok.
insert_many(Server, Records) when is_list(Records) ->
    cets_call:sync_operation(Server, {insert_many, Records}).

%% @doc Tries to insert a new record.
%%
%% All inserts are sent to the leader node first.
%% It is a slightly slower comparing to just insert, because
%% extra messaging is required.
-spec insert_new(server_ref(), tuple()) -> WasInserted :: boolean().
insert_new(Server, Rec) when is_tuple(Rec) ->
    Res = cets_call:send_leader_op(Server, {insert_new, Rec}),
    handle_insert_new_result(Res).

handle_insert_new_result(ok) -> true;
handle_insert_new_result({error, rejected}) -> false.

%% @doc Inserts a new tuple or returns an existing one.
-spec insert_new_or_lookup(server_ref(), tuple()) -> {WasInserted, ReadRecords} when
    WasInserted :: boolean(),
    ReadRecords :: [tuple()].
insert_new_or_lookup(Server, Rec) when is_tuple(Rec) ->
    Res = cets_call:send_leader_op(Server, {insert_new_or_lookup, Rec}),
    handle_insert_new_or_lookup(Res, Rec).

handle_insert_new_or_lookup(ok, Rec) ->
    {true, [Rec]};
handle_insert_new_or_lookup({error, {rejected, Recs}}, _CandidateRec) ->
    {false, Recs}.

%% @doc Serialized version of `insert/2'.
%%
%% All `insert_serial' calls are sent to the leader node first.
%%
%% Similar to `insert_new/2', but overwrites the data silently on conflict.
%% It could be used to update entries, which use not node-specific keys.
-spec insert_serial(server_ref(), tuple()) -> ok.
insert_serial(Server, Rec) when is_tuple(Rec) ->
    ok = cets_call:send_leader_op(Server, {insert, Rec}).

%% @doc Removes an object with the key from all nodes in the cluster.
%%
%% Ideally, nodes should only remove data that they've inserted, not data from another node.
%% @see delete_many/2
%% @see delete_request/2
-spec delete(server_ref(), term()) -> ok.
delete(Server, Key) ->
    cets_call:sync_operation(Server, {delete, Key}).

%% @doc Removes a specific object. Useful to remove data from ETS tables of type `bag'.
-spec delete_object(server_ref(), tuple()) -> ok.
delete_object(Server, Object) ->
    cets_call:sync_operation(Server, {delete_object, Object}).

%% @doc Removes multiple objects using a list of keys.
%% @see delete/2
-spec delete_many(server_ref(), [term()]) -> ok.
delete_many(Server, Keys) ->
    cets_call:sync_operation(Server, {delete_many, Keys}).

%% @doc Removes multiple specific tuples.
-spec delete_objects(server_ref(), [tuple()]) -> ok.
delete_objects(Server, Objects) ->
    cets_call:sync_operation(Server, {delete_objects, Objects}).

%% @doc Async `insert/2' call.
-spec insert_request(server_ref(), tuple()) -> request_id().
insert_request(Server, Rec) ->
    cets_call:async_operation(Server, {insert, Rec}).

%% @doc Async `insert_many/2' call.
-spec insert_many_request(server_ref(), [tuple()]) -> request_id().
insert_many_request(Server, Records) ->
    cets_call:async_operation(Server, {insert_many, Records}).

%% @doc Async `delete/2' call.
-spec delete_request(server_ref(), term()) -> request_id().
delete_request(Server, Key) ->
    cets_call:async_operation(Server, {delete, Key}).

%% @doc Async `delete_object/2' call.
-spec delete_object_request(server_ref(), tuple()) -> request_id().
delete_object_request(Server, Object) ->
    cets_call:async_operation(Server, {delete_object, Object}).

%% @doc Async `delete_many/2' call.
-spec delete_many_request(server_ref(), [term()]) -> request_id().
delete_many_request(Server, Keys) ->
    cets_call:async_operation(Server, {delete_many, Keys}).

%% @doc Async `delete_objects/2' call.
-spec delete_objects_request(server_ref(), [tuple()]) -> request_id().
delete_objects_request(Server, Objects) ->
    cets_call:async_operation(Server, {delete_objects, Objects}).

%% @doc Waits for the result of the async operation.
-spec wait_response(request_id(), timeout()) -> response_return().
wait_response(ReqId, Timeout) ->
    gen_server:wait_response(ReqId, Timeout).

%% @doc Waits for multiple responses.
%%
%% Returns results in the same order as `ReqIds'.
%% Blocks for maximum `Timeout' milliseconds.
-spec wait_responses([request_id()], response_timeout()) ->
    [response_return()].
wait_responses(ReqIds, Timeout) ->
    cets_call:wait_responses(ReqIds, Timeout).

%% @doc Gets the pid of the process, which handles `insert_new' operations.
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

%% @doc Get a list of other nodes in the cluster that are connected together.
-spec other_nodes(server_ref()) -> ordsets:ordset(node()).
other_nodes(Server) ->
    lists:usort(pids_to_nodes(other_pids(Server))).

%% @doc Async `get_nodes/1' call.
-spec get_nodes_request(server_ref()) -> request_id().
get_nodes_request(Server) ->
    gen_server:send_request(Server, get_nodes).

%% @doc Gets a list of other CETS processes that are handling this table.
-spec other_pids(server_ref()) -> servers().
other_pids(Server) ->
    cets_call:long_call(Server, other_servers).

%% @doc Pauses update operations.
%%
%% `cets:insert/2' and other functions would block, till the server is unpaused.
-spec pause(server_ref()) -> pause_monitor().
pause(Server) ->
    cets_call:long_call(Server, pause).

%% @doc Unpauses update operations.
%%
%% Provide reference, returned from `cets:pause/1' as an argument.
-spec unpause(server_ref(), pause_monitor()) -> ok | {error, unknown_pause_monitor}.
unpause(Server, PauseRef) ->
    cets_call:long_call(Server, {unpause, PauseRef}).

%% @doc Sets `is_leader' field in the state.
%%
%% For debugging only.
%% Setting in in the real life would break leader election logic.
-spec set_leader(server_ref(), boolean()) -> ok.
set_leader(Server, IsLeader) ->
    cets_call:long_call(Server, {set_leader, IsLeader}).

%% @doc Waits till all pending operations are applied.
-spec ping_all(server_ref()) -> ok | {error, [{server_pid(), Reason :: term()}]}.
ping_all(Server) ->
    cets_call:long_call(Server, ping_all).

%% @doc Blocks until all pending Erlang messages are processed by the `Server'.
-spec ping(server_ref()) -> pong.
ping(Server) ->
    cets_call:long_call(Server, ping).

%% @doc Returns debug information from the server.
-spec info(server_ref()) -> info().
info(Server) ->
    cets_call:long_call(Server, get_info).

%% gen_server callbacks

%% @private
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
        keypos => KeyPos,
        ack_pid => AckPid,
        other_servers => [],
        %% Initial join_ref is random
        join_ref => make_ref(),
        leader => self(),
        is_leader => true,
        opts => Opts,
        backlog => [],
        pause_monitors => [],
        node_down_history => []
    }}.

%% @private
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
handle_call(ping_all, From, State = #{other_servers := Servers}) ->
    %% Do spawn to avoid any possible deadlocks
    proc_lib:spawn(fun() ->
        %% If ping crashes, the caller would not receive a reply.
        %% So, we have to use catch to still able to reply with ok.
        Results = lists:map(fun(Server) -> {Server, catch ping(Server)} end, Servers),
        BadResults = [Res || {_Server, Result} = Res <- Results, Result =/= pong],
        case BadResults of
            [] ->
                gen_server:reply(From, ok);
            _ ->
                gen_server:reply(From, {error, BadResults})
        end
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
handle_call({send_dump, NewPids, JoinRef, PauseRef, Dump}, _From, State) ->
    handle_send_dump(NewPids, JoinRef, PauseRef, Dump, State);
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

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

%% @private
-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({remote_op, Op, From, AckPid, JoinRef}, State) ->
    handle_remote_op(Op, From, AckPid, JoinRef, State),
    {noreply, State};
handle_info({'DOWN', Mon, process, Pid, Reason}, State) ->
    {noreply, handle_down(Mon, Pid, Reason, State)};
handle_info({check_server, FromPid, JoinRef}, State) ->
    {noreply, handle_check_server(FromPid, JoinRef, State)};
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

%% @private
terminate(_Reason, _State = #{ack_pid := AckPid}) ->
    ok = gen_server:stop(AckPid).

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal logic

-spec handle_send_dump(servers(), join_ref(), pause_monitor(), [tuple()], state()) ->
    {reply, ok, state()}.
handle_send_dump(NewPids, JoinRef, PauseRef, Dump, State) ->
    #{tab := Tab, other_servers := Servers, pause_monitors := PauseMons} = State,
    case lists:member(PauseRef, PauseMons) of
        true ->
            ets:insert(Tab, Dump),
            Servers2 = add_servers(NewPids, Servers),
            {reply, ok, set_other_servers(Servers2, State#{join_ref := JoinRef})};
        false ->
            ?LOG_ERROR(#{
                what => send_dump_received_when_unpaused,
                text => <<"Received send_dump message while in the unpaused state. Ignore it">>,
                join_ref => JoinRef,
                pause_ref => PauseRef,
                state => State
            }),
            {reply, {error, ignored}, State}
    end.

-spec handle_down(reference(), pid(), term(), state()) -> state().
handle_down(Mon, Pid, Reason, State = #{pause_monitors := Mons}) ->
    case lists:member(Mon, Mons) of
        true ->
            %% Ignore logging if the process exited normally
            case Reason of
                normal ->
                    ok;
                _ ->
                    ?LOG_ERROR(#{
                        what => pause_owner_crashed,
                        state => State,
                        paused_by_pid => Pid,
                        reason => Reason
                    })
            end,
            handle_unpause2(Mon, Mons, State);
        false ->
            handle_down2(Pid, Reason, State)
    end.

-spec handle_down2(pid(), term(), state()) -> state().
handle_down2(RemotePid, Reason, State = #{other_servers := Servers, ack_pid := AckPid}) ->
    case lists:member(RemotePid, Servers) of
        true ->
            cets_ack:send_remote_down(AckPid, RemotePid),
            call_user_handle_down(RemotePid, State),
            Servers2 = lists:delete(RemotePid, Servers),
            update_node_down_history(RemotePid, Reason, set_other_servers(Servers2, State));
        false ->
            %% This should not happen
            ?LOG_ERROR(#{
                what => handle_down_failed,
                remote_pid => RemotePid,
                state => State
            }),
            State
    end.

update_node_down_history(RemotePid, Reason, State = #{node_down_history := History}) ->
    Item = #{node => node(RemotePid), pid => RemotePid, reason => Reason},
    State#{node_down_history := [Item | History]}.

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

%% ETS returns booleans instead of ok, because ETS API is old and inspired by Prolog.
%% So, match the API logic here.
-spec ets_delete_keys(table_name(), [term()]) -> true.
ets_delete_keys(Tab, Keys) ->
    [ets:delete(Tab, Key) || Key <- Keys],
    true.

-spec ets_delete_objects(table_name(), [tuple()]) -> true.
ets_delete_objects(Tab, Objects) ->
    [ets:delete_object(Tab, Object) || Object <- Objects],
    true.

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
-spec do_op(op(), state()) -> boolean().
do_op(Op, #{tab := Tab}) ->
    do_table_op(Op, Tab).

-spec do_table_op(op(), table_name()) -> boolean().
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
    ets:insert_new(Tab, Rec);
do_table_op({insert_new_or_lookup, Rec}, Tab) ->
    ets:insert_new(Tab, Rec).

%% Handle operation locally and replicate it across the cluster
-spec handle_op(op(), from(), state()) -> ok.
handle_op({leader_op, Op}, From, State) ->
    handle_leader_op(Op, From, State);
handle_op(Op, From, State) ->
    do_op(Op, State),
    replicate(Op, From, State).

-spec rejected_result(op(), state()) -> term().
rejected_result({insert_new, _Rec}, _State) ->
    {error, rejected};
rejected_result({insert_new_or_lookup, Rec}, #{keypos := KeyPos, tab := Tab}) ->
    Key = element(KeyPos, Rec),
    %% Return a list of records, because the table could be a bag
    Recs = ets:lookup(Tab, Key),
    {error, {rejected, Recs}}.

-spec handle_leader_op(op(), from(), state()) -> ok.
handle_leader_op(Op, From, State = #{is_leader := true}) ->
    case do_op(Op, State) of
        %% Skip the replication - insert_new returns false.
        false ->
            gen_server:reply(From, rejected_result(Op, State)),
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

%% That could actually arrive before we get fully unpaused
%% (though cets_join:pause_on_remote_node/2 would ensure that CETS server
%%  would send check_server only after cets_join is down
%%  and does not send new send_dump messages)
handle_check_server(_FromPid, JoinRef, State = #{join_ref := JoinRef}) ->
    %% check_server passed - do nothing
    State;
handle_check_server(FromPid, RemoteJoinRef, State = #{join_ref := JoinRef}) ->
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
    State.

-spec handle_get_info(state()) -> info().
handle_get_info(
    #{
        tab := Tab,
        other_servers := Servers,
        ack_pid := AckPid,
        join_ref := JoinRef,
        node_down_history := DownHistory,
        pause_monitors := PauseMons,
        opts := Opts
    }
) ->
    #{
        table => Tab,
        nodes => lists:usort(pids_to_nodes([self() | Servers])),
        other_servers => Servers,
        size => ets:info(Tab, size),
        memory => ets:info(Tab, memory),
        ack_pid => AckPid,
        join_ref => JoinRef,
        node_down_history => DownHistory,
        pause_monitors => PauseMons,
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
