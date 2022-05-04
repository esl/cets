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

-export([start/2, stop/1, insert/2, insert_many/2, delete/2, delete_many/2]).
-export([dump/1, remote_dump/1, send_dump_to_remote_node/3, table_name/1]).
-export([other_nodes/1, other_pids/1]).
-export([pause/1, unpause/2, sync/1, ping/1]).
-export([info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([insert_request/2, insert_many_request/2,
         delete_request/2, delete_many_request/2, wait_response/2]).

-include_lib("kernel/include/logger.hrl").

-type server() :: atom() | pid().
-type request_id() :: term().
-type backlog_msg() :: {insert, term()} | {delete, term()}.
-type from() :: {pid(), reference()}.
-type backlog_entry() :: {backlog_msg(), from()}.
-type state() :: #{
        tab := atom(),
        mon_tab := atom(),
        other_servers := [pid()],
        opts := map(),
        backlog := [backlog_entry()],
        pause_monitors := [reference()]}.

%% API functions

%% Table and server has the same name
%% Opts:
%% - handle_down = fun(#{remote_pid => Pid, table => Tab})
%%   Called when a remote node goes down. Do not update other nodes data
%%   from this function (otherwise circular locking could happen - use spawn
%%   to make a new async process if you need to update).
%%   i.e. any functions that replicate changes are not allowed (i.e. insert/2,
%%   remove/2).
start(Tab, Opts) when is_atom(Tab) ->
    gen_server:start({local, Tab}, ?MODULE, {Tab, Opts}, []).

stop(Tab) ->
    gen_server:stop(Tab).

dump(Tab) ->
    ets:tab2list(Tab).

-spec remote_dump(server()) -> term().
remote_dump(Server) ->
    short_call(Server, remote_dump).

table_name(Tab) when is_atom(Tab) ->
    Tab;
table_name(Server) ->
    short_call(Server, table_name).

send_dump_to_remote_node(RemotePid, NewPids, OurDump) ->
    Msg = {send_dump_to_remote_node, NewPids, OurDump},
    F = fun() -> gen_server:call(RemotePid, Msg, infinity) end,
    Info = #{task => send_dump_to_remote_node,
             remote_pid => RemotePid, count => length(OurDump)},
    cets_long:run_safely(Info, F).

%% Only the node that owns the data could update/remove the data.
%% Ideally Key should contain inserter node info (for cleaning).
-spec insert(server(), tuple()) -> ok.
insert(Server, Rec) when is_tuple(Rec) ->
    sync_operation(Server, {insert, Rec}).

-spec insert_many(server(), list(tuple())) -> ok.
insert_many(Server, Records) when is_list(Records) ->
    sync_operation(Server, {insert, Records}).

-spec delete(server(), term()) -> ok.
delete(Server, Key) ->
    delete_many(Server, [Key]).

%% A separate function for multidelete (because key COULD be a list, so no confusion)
-spec delete_many(server(), [term()]) -> ok.
delete_many(Server, Keys) ->
    sync_operation(Server, {delete, Keys}).

-spec insert_request(server(), tuple()) -> request_id().
insert_request(Server, Rec) ->
    async_operation(Server, {insert, Rec}).

-spec insert_many_request(server(), [tuple()]) -> request_id().
insert_many_request(Server, Records) ->
    async_operation(Server, {insert, Records}).

-spec delete_request(server(), term()) -> request_id().
delete_request(Tab, Key) ->
    delete_many_request(Tab, [Key]).

-spec delete_many_request(server(), term()) -> request_id().
delete_many_request(Server, Keys) ->
    async_operation(Server, {delete, Keys}).

other_servers(Server) ->
    gen_server:call(Server, other_servers).

other_nodes(Server) ->
    lists:usort(pids_to_nodes(other_pids(Server))).

-spec other_pids(server()) -> [pid()].
other_pids(Server) ->
    other_servers(Server).

-spec pause(server()) -> reference().
pause(Server) ->
    short_call(Server, pause).

-spec unpause(server(), reference()) -> term().
unpause(Server, PauseRef) ->
    short_call(Server, {unpause, PauseRef}).

%% Waits till all pending operations are applied.
-spec sync(server()) -> term().
sync(Server) ->
    short_call(Server, sync).

-spec ping(server()) -> term().
ping(Server) ->
    short_call(Server, ping).

-spec info(server()) -> term().
info(Server) ->
    gen_server:call(Server, get_info).

%% gen_server callbacks

-spec init(term()) -> {ok, state()}.
init({Tab, Opts}) ->
    process_flag(message_queue_data, off_heap),
    MonTab = list_to_atom(atom_to_list(Tab) ++ "_mon"),
    ets:new(Tab, [ordered_set, named_table, public]),
    ets:new(MonTab, [public, named_table]),
    cets_mon_cleaner:start_link(MonTab, MonTab),
    {ok, #{tab => Tab, mon_tab => MonTab,
           other_servers => [], opts => Opts, backlog => [],
           pause_monitors => []}}.

-spec handle_call(term(), from(), state()) ->
        {noreply, state()} | {reply, term(), state()}.
handle_call(other_servers, _From, State = #{other_servers := Servers}) ->
    {reply, Servers, State};
handle_call(sync, From, State = #{other_servers := Servers}) ->
    %% Do spawn to avoid any possible deadlocks
    proc_lib:spawn(fun() ->
            [ping(Pid) || Pid <- Servers],
            gen_server:reply(From, ok)
        end),
    {noreply, State};
handle_call(ping, _From, State) ->
    {reply, ping, State};
handle_call(table_name, _From, State = #{tab := Tab}) ->
    {reply, {ok, Tab}, State};
handle_call(remote_dump, From, State = #{tab := Tab}) ->
    %% Do not block the main process (also reduces GC of the main process)
    proc_lib:spawn_link(fun() -> gen_server:reply(From, {ok, dump(Tab)}) end),
    {noreply, State};
handle_call({send_dump_to_remote_node, NewPids, Dump}, _From, State) ->
    handle_send_dump_to_remote_node(NewPids, Dump, State);
handle_call(pause, _From = {FromPid, _}, State = #{pause_monitors := Mons}) ->
    %% We monitor who pauses our server
    Mon = erlang:monitor(process, FromPid),
    {reply, Mon, State#{pause_monitors := [Mon|Mons]}};
handle_call({unpause, Ref}, _From, State) ->
    handle_unpause(Ref, State);
handle_call(get_info, _From, State) ->
    handle_get_info(State).

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({op, From, Msg}, State = #{pause_monitors := []}) ->
    handle_op(From, Msg, State),
    {noreply, State};
handle_cast({op, From, Msg}, State = #{pause_monitors := [_|_], backlog := Backlog}) ->
    %% Backlog is a list of pending operation, when our server is paused.
    %% The list would be applied, once our server is unpaused.
    {noreply, State#{backlog := [{Msg, From} | Backlog]}};
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({remote_op, From, Msg}, State) ->
    handle_remote_op(From, Msg, State),
    {noreply, State};
handle_info({'DOWN', Mon, process, Pid, _Reason}, State) ->
    handle_down(Mon, Pid, State);
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal logic

handle_send_dump_to_remote_node(NewPids, Dump,
                                State = #{tab := Tab, other_servers := Servers}) ->
    ets:insert(Tab, Dump),
    Servers2 = add_servers(NewPids, Servers),
    {reply, ok, State#{other_servers := Servers2}}.

handle_down(Mon, Pid, State = #{pause_monitors := Mons}) ->
    case lists:member(Mon, Mons) of
        true ->
            ?LOG_ERROR(#{what => pause_owner_crashed,
                         state => State, paused_by_pid => Pid}),
            {reply, ok, State2} = handle_unpause(Mon, State),
            {noreply, State2};
        false ->
            handle_down2(Mon, Pid, State)
    end.

handle_down2(_Mon, RemotePid, State = #{other_servers := Servers, mon_tab := MonTab}) ->
    case lists:member(RemotePid, Servers) of
        true ->
            Servers2 = lists:delete(RemotePid, Servers),
            notify_remote_down(RemotePid, MonTab),
            %% Down from a proxy
            call_user_handle_down(RemotePid, State),
            {noreply, State#{other_servers := Servers2}};
        false ->
            %% This should not happen
            ?LOG_ERROR(#{what => handle_down_failed,
                         remote_pid => RemotePid, state => State}),
            {noreply, State}
    end.

notify_remote_down(RemotePid, MonTab) ->
    List = ets:tab2list(MonTab),
    notify_remote_down_loop(RemotePid, List).

notify_remote_down_loop(RemotePid, [{Mon, Pid} | List]) ->
    Pid ! {remote_down, Mon, RemotePid},
    notify_remote_down_loop(RemotePid, List);
notify_remote_down_loop(_RemotePid, []) ->
    ok.

%% Merge two lists of pids, create the missing monitors.
add_servers(Pids, Servers) ->
    lists:sort(add_servers2(Pids, Servers) ++ Servers).

add_servers2([RemotePid | OtherPids], Servers)
  when is_pid(RemotePid), RemotePid =/= self() ->
    case has_remote_pid(RemotePid, Servers) of
        false ->
            erlang:monitor(process, RemotePid),
            [RemotePid | add_servers2(OtherPids, Servers)];
        true ->
            ?LOG_INFO(#{what => already_added,
                        remote_pid => RemotePid, remote_node => node(RemotePid)}),
            add_servers2(OtherPids, Servers)
    end;
add_servers2([], _Servers) ->
    [].

pids_to_nodes(Pids) ->
    lists:map(fun node/1, Pids).

ets_delete_keys(Tab, [Key | Keys]) ->
    ets:delete(Tab, Key),
    ets_delete_keys(Tab, Keys);
ets_delete_keys(_Tab, []) ->
    ok.

has_remote_pid(RemotePid, Servers) ->
    lists:member(RemotePid, Servers).

reply_updated({Mon, Pid}) ->
    %% We really don't wanna block this process
    erlang:send(Pid, {updated, Mon, self()}, [noconnect, nosuspend]).

send_to_remote(RemotePid, Msg) ->
    erlang:send(RemotePid, Msg, [noconnect, nosuspend]).

%% Handle operation from a remote node
handle_remote_op(From, Msg, State) ->
    do_op(Msg, State),
    reply_updated(From).

%% Apply operation for one local table only
do_op(Msg, #{tab := Tab}) ->
    do_table_op(Msg, Tab).

do_table_op({insert, Rec}, Tab) ->
    ets:insert(Tab, Rec);
do_table_op({delete, Keys}, Tab) ->
    ets_delete_keys(Tab, Keys).

%% Handle operation locally and replicate it across the cluster
handle_op(From = {Mon, Pid}, Msg, State) when is_pid(Pid) ->
    do_op(Msg, State),
    WaitInfo = replicate(From, Msg, State),
    Pid ! {cets_reply, Mon, WaitInfo}.

replicate(From, Msg, #{mon_tab := MonTab, other_servers := Servers}) ->
    %% Reply would be routed directly to FromPid
    Msg2 = {remote_op, From, Msg},
    replicate2(Servers, Msg2),
    ets:insert(MonTab, From),
    {Servers, MonTab}.

replicate2([RemotePid | Servers], Msg) ->
    send_to_remote(RemotePid, Msg),
    replicate2(Servers, Msg);
replicate2([], _Msg) ->
    ok.

apply_backlog([{Msg, From} | Backlog], State) ->
    handle_op(From, Msg, State),
    apply_backlog(Backlog, State);
apply_backlog([], State) ->
    State.

-spec short_call(server(), term()) -> term().
short_call(RemotePid, Msg) when is_pid(RemotePid) ->
    F = fun() -> gen_server:call(RemotePid, Msg, infinity) end,
    Info = #{task => Msg,
             remote_pid => RemotePid, remote_node => node(RemotePid)},
    cets_long:run_safely(Info, F);
short_call(Name, Msg) when is_atom(Name) ->
    short_call(whereis(Name), Msg).

%% We support multiple pauses
%% Only when all pause requests are unpaused we continue
handle_unpause(_Ref, State = #{pause_monitors := []}) ->
    {reply, {error, already_unpaused}, State};
handle_unpause(Mon, State = #{backlog := Backlog, pause_monitors := Mons}) ->
    erlang:demonitor(Mon, [flush]),
    Mons2 = lists:delete(Mon, Mons),
    State2 = State#{pause_monitors := Mons2},
    State3 =
        case Mons2 of
            [] ->
                apply_backlog(lists:reverse(Backlog), State2),
                State2#{backlog := []};
            _ ->
                State2
        end,
    {reply, ok, State3}.

handle_get_info(State = #{tab := Tab, other_servers := Servers}) ->
    Info = #{table => Tab,
             nodes => lists:usort(pids_to_nodes([self() | Servers])),
             size => ets:info(Tab, size),
             memory => ets:info(Tab, memory)},
    {reply, Info, State}.

%% Cleanup
call_user_handle_down(RemotePid, _State = #{tab := Tab, opts := Opts}) ->
    case Opts of
        #{handle_down := F} ->
            FF = fun() -> F(#{remote_pid => RemotePid, table => Tab}) end,
            Info = #{task => call_user_handle_down, table => Tab,
                     remote_pid => RemotePid, remote_node => node(RemotePid)},
            cets_long:run_safely(Info, FF);
        _ ->
            ok
    end.

async_operation(Server, Msg) ->
    Mon = erlang:monitor(process, Server),
    gen_server:cast(Server, {op, {Mon, self()}, Msg}),
    Mon.

sync_operation(Server, Msg) ->
    Mon = async_operation(Server, Msg),
    %% We monitor the local server until the response from all servers is collected.
    wait_response(Mon, infinity).

-spec wait_response(request_id(), non_neg_integer() | infinity) -> term().
wait_response(Mon, Timeout) ->
    receive
        {'DOWN', Mon, process, _Pid, Reason} ->
            error({cets_down, Reason});
        {cets_reply, Mon, WaitInfo} ->
            wait_for_updated(Mon, WaitInfo)
    after Timeout ->
            erlang:demonitor(Mon, [flush]),
            error(timeout)
    end.

%% Wait for response from the remote nodes that the operation is completed.
%% remote_down is sent by the local server, if the remote server is down.
wait_for_updated(Mon, {Servers, MonTab}) ->
    try
        wait_for_updated2(Mon, Servers)
    after
        erlang:demonitor(Mon, [flush]),
        ets:delete(MonTab, Mon)
    end.

wait_for_updated2(_Mon, []) ->
    ok;
wait_for_updated2(Mon, Servers) ->
    receive
        {updated, Mon, Pid} ->
            %% A replication confirmation from the remote server is received
            Servers2 = lists:delete(Pid, Servers),
            wait_for_updated2(Mon, Servers2);
        {remote_down, Mon, Pid} ->
            %% This message is sent by our local server when
            %% the remote server is down condition is detected
            Servers2 = lists:delete(Pid, Servers),
            wait_for_updated2(Mon, Servers2);
        {'DOWN', Mon, process, _Pid, Reason} ->
            %% Local server is down, this is a critical error
            error({cets_down, Reason})
    end.
