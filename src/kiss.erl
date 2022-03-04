%% Very simple multinode ETS writer
%% One file, everything is simple, but we don't silently hide race conditions
%% No transactions
%% We don't use rpc module, because it is one gen_server
%% We monitor a proxy module (so, no remote monitors on each insert)


%% If we write in format {Key, WriterName}, we should resolve conflicts automatically.
%%
%% While Tab is an atom, we can join tables with different atoms for the local testing.

%% We don't use monitors to avoid round-trips (that's why we don't use calls neither)
-module(kiss).
-behaviour(gen_server).

-export([start/2, stop/1, dump/1, insert/2, delete/2, delete_many/2, join/3, other_nodes/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

%% Table and server has the same name
%% Opts:
%% - handle_down = fun(#{remote_pid => Pid, table => Tab})
%%   Called when a remote node goes down. Do not update other nodes data
%%   from this function (otherwise circular locking could happen - use spawn
%%   to make a new async process if you need to update).
%%   i.e. any functions that replicate changes are not allowed (i.e. insert/2,
%%   remove/2).
start(Tab, Opts) when is_atom(Tab) ->
    gen_server:start({local, Tab}, ?MODULE, [Tab, Opts], []).

stop(Tab) ->
    gen_server:stop(Tab).

dump(Tab) ->
    ets:tab2list(Tab).

%% Adds a node to a cluster.
%% Writes from other nodes would wait for join completion.
%% LockKey should be the same on all nodes.
join(LockKey, Tab, RemotePid) when is_pid(RemotePid) ->
    Servers = other_servers(Tab),
    case lists:keymember(RemotePid, 1, Servers) of
        true ->
            {error, already_joined};
        false ->
                Start = os:timestamp(),
                F = fun() -> join_loop(LockKey, Tab, RemotePid, Start) end,
                kiss_long:run(#{task => join, table => Tab,
                                remote_pid => RemotePid,
                                remote_node => node(RemotePid)}, F)
    end.

join_loop(LockKey, Tab, RemotePid, Start) ->
    %% Only one join at a time:
    %% - for performance reasons, we don't want to cause too much load for active nodes
    %% - to avoid deadlocks, because joining does gen_server calls
    F = fun() ->
        Diff = timer:now_diff(os:timestamp(), Start) div 1000,
        %% Getting the lock could take really long time in case nodes are
        %% overloaded or joining is already in progress on another node
        ?LOG_INFO(#{what => join_got_lock, table => Tab, after_time_ms => Diff}),
        gen_server:call(Tab, {join, RemotePid}, infinity)
        end,
    LockRequest = {LockKey, self()},
    %% Just lock all nodes, no magic here :)
    Nodes = [node() | nodes()],
    Retries = 1,
    case global:trans(LockRequest, F, Nodes, Retries) of
        aborted ->
            ?LOG_ERROR(#{what => join_retry, reason => lock_aborted}),
            join_loop(LockKey, Tab, RemotePid, Start);
        Result ->
            Result
    end.

remote_add_node_to_schema(RemotePid, ServerPid, OtherPids, ReturnDump) ->
    Msg = {remote_add_node_to_schema, ServerPid, OtherPids, ReturnDump},
    F = fun() -> gen_server:call(RemotePid, Msg, infinity) end,
    Info = #{task => remote_add_node_to_schema, return_dump => ReturnDump,
             remote_pid => RemotePid, remote_node => node(RemotePid),
             other_pids => OtherPids, other_nodes => pids_to_nodes(OtherPids)},
    kiss_long:run_safely(Info, F).

get_other_pids(RemotePid) ->
    F = fun() -> gen_server:call(RemotePid, get_other_pids, infinity) end,
    Info = #{task => get_other_pids,
             remote_pid => RemotePid, remote_node => node(RemotePid)},
    kiss_long:run_safely(Info, F).

send_dump_to_remote_node(_RemotePid, _FromPid, []) ->
    skipped;
send_dump_to_remote_node(RemotePid, FromPid, OurDump) ->
    Msg = {send_dump_to_remote_node, FromPid, OurDump},
    F = fun() -> gen_server:call(RemotePid, Msg, infinity) end,
    Info = #{task => send_dump_to_remote_node,
             remote_pid => RemotePid, count => length(OurDump)},
    kiss_long:run_safely(Info, F).

%% Only the node that owns the data could update/remove the data.
%% Ideally Key should contain inserter node info (for cleaning).
insert(Server, Rec) ->
    {ok, Monitors} = gen_server:call(Server, {insert, Rec}),
    wait_for_updated(Monitors).

delete(Tab, Key) ->
    delete_many(Tab, [Key]).

%% A separate function for multidelete (because key COULD be a list, so no confusion)
delete_many(Server, Keys) ->
    {ok, Monitors} = gen_server:call(Server, {delete, Keys}),
    wait_for_updated(Monitors).

wait_for_updated([Mon | Monitors]) ->
    receive
        {updated, Mon} ->
            wait_for_updated(Monitors);
        {'DOWN', Mon, process, _Pid, _Reason} ->
            wait_for_updated(Monitors)
    end;
wait_for_updated([]) ->
    ok.

other_servers(Server) ->
    gen_server:call(Server, other_servers).

other_nodes(Tab) ->
    lists:usort(pids_to_nodes(servers_to_pids(other_servers(Tab)))).

pause(RemotePid) ->
    F = fun() -> gen_server:call(RemotePid, pause, infinity) end,
    Info = #{task => pause,
             remote_pid => RemotePid, remote_node => node(RemotePid)},
    kiss_long:run_safely(Info, F).

unpause(RemotePid) ->
    F = fun() -> gen_server:call(RemotePid, unpause, infinity) end,
    Info = #{task => unpause,
             remote_pid => RemotePid, remote_node => node(RemotePid)},
    kiss_long:run_safely(Info, F).

init([Tab, Opts]) ->
    ets:new(Tab, [ordered_set, named_table,
                  public, {read_concurrency, true}]),
    {ok, #{tab => Tab, other_servers => [], opts => Opts, backlog => [],
           paused => false, pause_monitor => undefined}}.

handle_call(other_servers, _From, State = #{other_servers := Servers}) ->
    {reply, Servers, State};
handle_call(get_other_pids, _From, State = #{other_servers := Servers}) ->
    {reply, {ok, servers_to_pids(Servers)}, State};
handle_call({join, RemotePid}, _From, State) ->
    handle_join(RemotePid, State);
handle_call({remote_add_node_to_schema, ServerPid, OtherPids, ReturnDump}, _From, State) ->
    handle_remote_add_node_to_schema(ServerPid, OtherPids, ReturnDump, State);
handle_call({send_dump_to_remote_node, FromPid, Dump}, _From, State) ->
    handle_send_dump_to_remote_node(FromPid, Dump, State);
handle_call(pause, _From = {FromPid, _}, State) ->
    Mon = erlang:monitor(process, FromPid),
    {reply, ok, State#{pause => true, pause_monitor => Mon}};
handle_call(unpause, _From, State) ->
    handle_unpause(State);
handle_call(Msg, From, State = #{paused := true, backlog := Backlog}) ->
    {noreply, State#{backlog => [{Msg, From} | Backlog]}};
handle_call({insert, Rec}, From, State) ->
    handle_insert(Rec, From, State);
handle_call({delete, Keys}, From, State) ->
    handle_delete(Keys, From, State).

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Mon, process, Pid, _Reason}, State) ->
    handle_down(Mon, Pid, State);
handle_info({insert_from_remote_node, Mon, Pid, Rec}, State = #{tab := Tab}) ->
    ets:insert(Tab, Rec),
    reply_updated(Pid, Mon),
    {noreply, State};
handle_info({delete_from_remote_node, Mon, Pid, Keys}, State = #{tab := Tab}) ->
    ets_delete_keys(Tab, Keys),
    reply_updated(Pid, Mon),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_join(RemotePid, State = #{other_servers := Servers}) when is_pid(RemotePid) ->
    case has_remote_pid(RemotePid, Servers) of
        true ->
            %% Already added
            {reply, ok, State};
        false ->
            handle_join2(RemotePid, State)
    end.

handle_join2(RemotePid, State = #{other_servers := Servers}) ->
    KnownPids = servers_to_pids(Servers),
    case get_other_pids(RemotePid) of
        {ok, OtherPids} ->
            AllPids = KnownPids ++ OtherPids,
            [pause(Pid) || Pid <- AllPids],
            try
                handle_join3(RemotePid, OtherPids, KnownPids, State)
            after
                [unpause(Pid) || Pid <- AllPids]
            end;
       Other ->
            ?LOG_ERROR(#{what => get_other_pids_failed, reason => Other}),
            {reply, {error, get_other_pids_failed}, State}
    end.

handle_join3(RemotePid, OtherPids, KnownPids, State) ->
    %% Remote gen_server calls here are "safe"
    case remote_add_node_to_schema(RemotePid, self(), KnownPids, true) of
        {ok, Dump, OtherPids} ->
            handle_join4(RemotePid, Dump, OtherPids, KnownPids, State);
       Other ->
            ?LOG_ERROR(#{what => remote_add_node_to_schema, reason => Other}),
            {reply, {error, remote_add_node_to_schema_failed}, State}
    end.

handle_join4(RemotePid, Dump, OtherPids, KnownPids,
             State = #{tab := Tab, other_servers := Servers}) ->
    Self = self(),
    NewPids = [RemotePid | OtherPids],
    %% Let all nodes to know each other
    [remote_add_node_to_schema(Pid, Self, KnownPids, false) || Pid <- OtherPids],
    [remote_add_node_to_schema(Pid, Self, NewPids, false) || Pid <- KnownPids],
    Servers2 = add_servers(NewPids, Servers),
    %% Ask our node to replicate data there before applying the dump
    OurDump = dump(Tab),
    %% A race condition is possible: when the remote node inserts a deleted record
    %% Send to all nodes from that partition
    [send_dump_to_remote_node(Pid, Self, OurDump) || Pid <- NewPids],
    %% Apply to our nodes
    [send_dump_to_remote_node(Pid, Self, Dump) || Pid <- KnownPids],
    ets:insert(Tab, Dump),
    %% Add ourself into remote schema
    %% Add remote nodes into our schema
    %% Copy from our node / Copy into our node
    {reply, ok, State#{other_servers => Servers2}}.

handle_remote_add_node_to_schema(RemotePid, OtherPids, ReturnDump,
                                 State = #{tab := Tab, other_servers := Servers}) ->
    Servers2 = add_servers([RemotePid | OtherPids], Servers),
    KnownPids = servers_to_pids(Servers),
    Dump = case ReturnDump of true -> dump(Tab); false -> not_requested end,
    {reply, {ok, Dump, KnownPids}, State#{other_servers => Servers2}}.

handle_send_dump_to_remote_node(_FromPid, Dump, State = #{tab := Tab}) ->
    ets:insert(Tab, Dump),
    {reply, ok, State}.

handle_down(Mon, PausedByPid, State = #{pause_monitor := Mon}) ->
    ?LOG_ERROR(#{what => pause_owner_crashed, state => State, paused_by_pid => PausedByPid}),
    handle_unpause(State);
handle_down(_Mon, ProxyPid, State = #{other_servers := Servers}) ->
    case lists:keytake(ProxyPid, 2, Servers) of
        {value, {RemotePid, _}, Servers2} ->
            %% Down from a proxy
            call_user_handle_down(RemotePid, State),
            {noreply, State#{other_servers => Servers2}};
        false ->
            %% This should not happen
            ?LOG_ERROR(#{what => handle_down_failed, proxy_pid => ProxyPid, state => State}),
            {noreply, State}
    end.

add_servers(Pids, Servers) ->
    lists:sort(start_proxies_for(Pids, Servers) ++ Servers).

start_proxies_for([RemotePid | OtherPids], Servers)
  when is_pid(RemotePid), RemotePid =/= self() ->
    case has_remote_pid(RemotePid, Servers) of
        false ->
            {ok, ProxyPid} = kiss_proxy:start(RemotePid),
            erlang:monitor(process, ProxyPid),
            [{RemotePid, ProxyPid} | start_proxies_for(OtherPids, Servers)];
        true ->
            ?LOG_INFO(#{what => already_added,
                        remote_pid => RemotePid, remote_node => node(RemotePid)}),
            start_proxies_for(OtherPids, Servers)
    end;
start_proxies_for([], _Servers) ->
    [].

pids_to_nodes(Pids) ->
    lists:map(fun node/1, Pids).

ets_delete_keys(Tab, [Key | Keys]) ->
    ets:delete(Tab, Key),
    ets_delete_keys(Tab, Keys);
ets_delete_keys(_Tab, []) ->
    ok.

servers_to_pids(Servers) ->
    [Pid || {Pid, _} <- Servers].

has_remote_pid(RemotePid, Servers) ->
    lists:keymember(RemotePid, 1, Servers).

%% Cleanup
call_user_handle_down(RemotePid, _State = #{tab := Tab, opts := Opts}) ->
    case Opts of
        #{handle_down := F} ->
            FF = fun() -> F(#{remote_pid => RemotePid, table => Tab}) end,
            Info = #{task => call_user_handle_down, table => Tab,
                     remote_pid => RemotePid, remote_node => node(RemotePid)},
            kiss_long:run_safely(Info, FF);
        _ ->
            ok
    end.

reply_updated(Pid, Mon) ->
    %% We really don't wanna block this process
    erlang:send(Pid, {updated, Mon}, [noconnect, nosuspend]).

send_to_remote(RemotePid, Msg) ->
    erlang:send(RemotePid, Msg, [noconnect, nosuspend]).

handle_insert(Rec, _From = {FromPid, _}, State = #{tab := Tab, other_servers := Servers}) ->
    ets:insert(Tab, Rec),
    %% Insert to other nodes and block till written
    Monitors = insert_to_remote_nodes(Servers, Rec, FromPid),
    {reply, {ok, Monitors}, State}.

insert_to_remote_nodes([{RemotePid, ProxyPid} | Servers], Rec, FromPid) ->
    Mon = erlang:monitor(process, ProxyPid),
    %% Reply would be routed directly to FromPid
    Msg = {insert_from_remote_node, Mon, FromPid, Rec},
    send_to_remote(RemotePid, Msg),
    [Mon|insert_to_remote_nodes(Servers, Rec, FromPid)];
insert_to_remote_nodes([], _Rec, _FromPid) ->
    [].

handle_delete(Keys, _From = {FromPid, _}, State = #{tab := Tab, other_servers := Servers}) ->
    ets_delete_keys(Tab, Keys),
    %% Insert to other nodes and block till written
    Monitors = delete_from_remote_nodes(Servers, Keys, FromPid),
    {reply, {ok, Monitors}, State}.

delete_from_remote_nodes([{RemotePid, ProxyPid} | Servers], Keys, FromPid) ->
    Mon = erlang:monitor(process, ProxyPid),
    %% Reply would be routed directly to FromPid
    Msg = {delete_from_remote_node, Mon, FromPid, Keys},
    send_to_remote(RemotePid, Msg),
    [Mon|delete_from_remote_nodes(Servers, Keys, FromPid)];
delete_from_remote_nodes([], _Keys, _FromPid) ->
    [].

apply_backlog([{Msg, From}|Backlog], State) ->
    {reply, Reply, State2} = handle_call(Msg, From, State),
    gen_server:reply(From, Reply),
    apply_backlog(Backlog, State2);
apply_backlog([], State) ->
    State.

%% Theoretically we can support mupltiple pauses (but no need for now because
%% we pause in the global locked function)
handle_unpause(State = #{pause := false}) ->
    {reply, {error, already_unpaused}, State};
handle_unpause(State = #{backlog := Backlog, pause_monitor := Mon}) ->
    erlang:demonitor(Mon, [flush]),
    State2 = State#{pause => false, backlog := [], pause_monitor => undefined},
    {reply, ok, apply_backlog(Backlog, State2)}.
