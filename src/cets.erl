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
    delete/2,
    delete_many/2,
    delete_object/2,
    delete_objects/2,
    dump/1,
    remote_dump/1,
    send_dump/4,
    table_name/1,
    other_nodes/1,
    other_pids/1,
    make_alias_for/2,
    pause/1,
    unpause/2,
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
    pause/1,
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
-type request_id() :: reference().
-type op() ::
    {insert, tuple()}
    | {delete, term()}
    | {delete_object, term()}
    | {insert_many, [tuple()]}
    | {delete_many, [term()]}
    | {delete_objects, [term()]}.
-type from() :: {pid(), reference()}.
-type backlog_entry() :: {op(), from()}.
-type table_name() :: atom().
-type pause_monitor() :: reference().
-type server_tuple() :: {pid(), Monitor :: reference(), Dest :: reference()}.
-type server_nums() :: map().
-type state() :: #{
    tab := table_name(),
    mon_tab := atom(),
    mon_pid := pid(),
    server_num := server_num(),
    server_nums := server_nums(),
    other_servers := [server_tuple()],
    just_pids := [pid()],
    just_dests := [reference()],
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
    | other_pids
    | {make_alias_for, [pid()]}
    | {unpause, reference()}
    | {send_dump, Nums :: server_nums(), NewServers :: [server_tuple()], Dump :: [tuple()]}.

-type info() :: #{
    table := table_name(),
    nodes := [node()],
    size := non_neg_integer(),
    memory := non_neg_integer(),
    mon_pid := pid(),
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

-export_type([request_id/0, op/0, server_ref/0, long_msg/0, info/0, table_name/0]).

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

-spec dump(table_name()) -> Records :: [tuple()].
dump(Tab) ->
    ets:tab2list(Tab).

-spec remote_dump(server_ref()) -> {ok, Records :: [tuple()]}.
remote_dump(Server) ->
    cets_call:long_call(Server, remote_dump).

-spec table_name(server_ref()) -> table_name().
table_name(Tab) when is_atom(Tab) ->
    Tab;
table_name(Server) ->
    cets_call:long_call(Server, table_name).

-spec send_dump(server_ref(), server_nums(), [server_tuple()], [tuple()]) -> ok.
send_dump(Server, Nums, NewServers, OurDump) ->
    Info = #{msg => send_dump, count => length(OurDump)},
    cets_call:long_call(Server, {send_dump, Nums, NewServers, OurDump}, Info).

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
    cets_call:wait_response(Mon, Timeout).

-spec make_alias_for(server_ref(), [server_ref()]) -> [{server_ref(), reference()}].
make_alias_for(Server, RemotePids) ->
    cets_call:long_call(Server, {make_alias_for, RemotePids}).

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
    MonTab = list_to_atom(atom_to_list(Tab) ++ "_mon"),
    Type = maps:get(type, Opts, ordered_set),
    KeyPos = maps:get(keypos, Opts, 1),
    %% Match result to prevent the Dialyzer warning
    _ = ets:new(Tab, [Type, named_table, public, {keypos, KeyPos}, {read_concurrency, true}]),
    _ = ets:new(MonTab, [public, named_table, {write_concurrency, true}]),
    {ok, MonPid} = cets_mon_cleaner:start_link(MonTab, MonTab),
    {ok, #{
        tab => Tab,
        mon_tab => MonTab,
        mon_pid => MonPid,
        server_num => 1,
        server_nums => #{self() => 1},
        other_servers => [],
        just_pids => [],
        just_dests => [],
        opts => Opts,
        backlog => [],
        pause_monitors => []
    }}.

-spec handle_call(term(), from(), state()) ->
    {noreply, state()} | {reply, term(), state()}.
handle_call(other_pids, _From, State = #{just_pids := Pids}) ->
    {reply, Pids, State};
handle_call({make_alias_for, Pids}, _From, State) ->
    %% Create channels used to deliver remote_op messages
    Aliases = [{Pid, erlang:monitor(process, Pid, [{alias, demonitor}])} || Pid <- Pids],
    {reply, Aliases, State};
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
    proc_lib:spawn_link(fun() -> gen_server:reply(From, {ok, dump(Tab)}) end),
    {noreply, State};
handle_call({send_dump, Nums, NewServers, Dump}, _From, State) ->
    handle_send_dump(Nums, NewServers, Dump, State);
handle_call(pause, _From = {FromPid, _}, State = #{pause_monitors := Mons}) ->
    %% We monitor who pauses our server
    Mon = erlang:monitor(process, FromPid),
    {reply, Mon, State#{pause_monitors := [Mon | Mons]}};
handle_call({unpause, Ref}, _From, State) ->
    handle_unpause(Ref, State);
handle_call(get_info, _From, State) ->
    handle_get_info(State).

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({op, From, Msg}, State = #{pause_monitors := []}) ->
    handle_op(From, Msg, State),
    {noreply, State};
handle_cast({op, From, Msg}, State = #{pause_monitors := [_ | _], backlog := Backlog}) ->
    %% Backlog is a list of pending operation, when our server is paused.
    %% The list would be applied, once our server is unpaused.
    {noreply, State#{backlog := [{Msg, From} | Backlog]}};
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({remote_op, Alias, Msg}, State) ->
    handle_remote_op(Alias, Msg, State),
    {noreply, State};
handle_info({'DOWN', Mon, process, Pid, _Reason}, State) ->
    handle_down(Mon, Pid, State);
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State = #{mon_pid := MonPid}) ->
    ok = gen_server:stop(MonPid).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal logic

handle_send_dump(Nums, NewServers, Dump, State = #{tab := Tab}) ->
    ets:insert(Tab, Dump),
    State2 = State#{server_nums := Nums, server_num := maps:get(self(), Nums)},
    {reply, ok, add_servers(NewServers, State2)}.

add_servers(NewServers, State = #{other_servers := Servers}) ->
    Servers2 = lists:sort(NewServers ++ Servers),
    set_servers(Servers2, State).

remove_server(Mon, State = #{other_servers := Servers}) ->
    Servers2 = lists:keydelete(Mon, 2, Servers),
    set_servers(Servers2, State).

set_servers(Servers, State) ->
    Pids = servers_to_pids(Servers),
    Dests = servers_to_dests(Servers),
    State#{other_servers := Servers, just_pids := Pids, just_dests := Dests}.

handle_down(Mon, Pid, State = #{pause_monitors := Mons}) ->
    case lists:member(Mon, Mons) of
        true ->
            ?LOG_ERROR(#{
                what => pause_owner_crashed,
                state => State,
                paused_by_pid => Pid
            }),
            {reply, ok, State2} = handle_unpause(Mon, State),
            {noreply, State2};
        false ->
            handle_down2(Mon, Pid, State)
    end.

handle_down2(Mon, RemotePid, State = #{mon_tab := MonTab, other_servers := Servers}) ->
    case lists:keymember(Mon, 2, Servers) of
        true ->
            Num = server_pid_to_server_num(RemotePid, State),
            notify_remote_down(Num, MonTab),
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

notify_remote_down(Num, MonTab) ->
    List = ets:tab2list(MonTab),
    [erlang:send(Mon, {cets_remote_down, Mon, Num}) || {Mon, _Pid} <- List],
    ok.

pids_to_nodes(Pids) ->
    lists:map(fun node/1, Pids).

ets_delete_keys(Tab, [Key | Keys]) ->
    ets:delete(Tab, Key),
    ets_delete_keys(Tab, Keys);
ets_delete_keys(_Tab, []) ->
    ok.

ets_delete_objects(Tab, [Object | Objects]) ->
    ets:delete_object(Tab, Object),
    ets_delete_objects(Tab, Objects);
ets_delete_objects(_Tab, []) ->
    ok.

reply_updated(Alias, #{server_num := Num}) ->
    %% nosuspend makes message sending unreliable
    erlang:send(Alias, {cets_updated, Alias, Num}, [noconnect]).

send_to_remote(RemoteAlias, Msg) ->
    erlang:send(RemoteAlias, Msg, [noconnect]).

%% Handle operation from a remote node
handle_remote_op(Alias, Msg, State) ->
    do_op(Msg, State),
    reply_updated(Alias, State).

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
handle_op(From = {Mon, Pid}, Msg, State) when is_pid(Pid) ->
    do_op(Msg, State),
    WaitInfo = replicate(From, Msg, State),
    Pid ! {cets_reply, Mon, WaitInfo},
    ok.

replicate({Alias, _} = From, Msg, #{mon_tab := MonTab, just_dests := Dests, server_num := Num, server_nums := Nums}) ->
    %% Reply would be routed directly to FromPid
    Msg2 = {remote_op, Alias, Msg},
    [send_to_remote(Dest, Msg2) || Dest <- Dests],
    ets:insert(MonTab, From),
    Nums2 = lists:delete(Num, maps:values(Nums)),
    {Nums2, MonTab}.

apply_backlog(State = #{backlog := Backlog}) ->
    [handle_op(From, Msg, State) || {Msg, From} <- lists:reverse(Backlog)],
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
    State3 =
        case Mons2 of
            [] ->
                apply_backlog(State2);
            _ ->
                State2
        end,
    {reply, ok, State3}.

-spec handle_get_info(state()) -> {reply, info(), state()}.
handle_get_info(
    State = #{
        tab := Tab,
        just_pids := Pids,
        mon_pid := MonPid,
        opts := Opts
    }
) ->
    Info = #{
        table => Tab,
        nodes => lists:usort(pids_to_nodes([self() | Pids])),
        size => ets:info(Tab, size),
        memory => ets:info(Tab, memory),
        mon_pid => MonPid,
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
