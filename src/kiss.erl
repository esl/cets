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
-export([start/2, stop/1, dump/1, insert/2, delete/2, delete_many/2, join/3, other_nodes/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-behaviour(gen_server).

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
                kiss_long:run("task=join table=~p remote_pid=~p remote_node=~p ",
                              [Tab, RemotePid, node(RemotePid)], F)
    end.

join_loop(LockKey, Tab, RemotePid, Start) ->
    %% Only one join at a time:
    %% - for performance reasons, we don't want to cause too much load for active nodes
    %% - to avoid deadlocks, because joining does gen_server calls
    F = fun() ->
        Diff = timer:now_diff(os:timestamp(), Start) div 1000,
        %% Getting the lock could take really long time in case nodes are
        %% overloaded or joining is already in progress on another node
        error_logger:info_msg("what=join_got_lock table=~p after_time=~p ms", [Tab, Diff]),
        gen_server:call(Tab, {join, RemotePid}, infinity)
        end,
    LockRequest = {LockKey, self()},
    %% Just lock all nodes, no magic here :)
    Nodes = [node() | nodes()],
    Retries = 1,
    case global:trans(LockRequest, F, Nodes, Retries) of
        aborted ->
            error_logger:error_msg("what=join_retry reason=lock_aborted", []),
            join_loop(LockKey, Tab, RemotePid, Start);
        Result ->
            Result
    end.

remote_add_node_to_schema(RemotePid, ServerPid, OtherPids) ->
    F = fun() -> gen_server:call(RemotePid, {remote_add_node_to_schema, ServerPid, OtherPids}, infinity) end,
    kiss_long:run_safely("task=remote_add_node_to_schema remote_pid=~p remote_node=~p other_pids=~0p other_nodes=~0p ",
                         [RemotePid, node(RemotePid), OtherPids, pids_to_nodes(OtherPids)], F).

remote_just_add_node_to_schema(RemotePid, ServerPid, OtherPids) ->
    F = fun() -> gen_server:call(RemotePid, {remote_just_add_node_to_schema, ServerPid, OtherPids}, infinity) end,
    kiss_long:run_safely("task=remote_just_add_node_to_schema remote_pid=~p remote_node=~p other_pids=~0p other_nodes=~0p ",
                         [RemotePid, node(RemotePid), OtherPids, pids_to_nodes(OtherPids)], F).

send_dump_to_remote_node(_RemotePid, _FromPid, []) ->
    skipped;
send_dump_to_remote_node(RemotePid, FromPid, OurDump) ->
    F = fun() -> gen_server:call(RemotePid, {send_dump_to_remote_node, FromPid, OurDump}, infinity) end,
    kiss_long:run_safely("task=send_dump_to_remote_node remote_pid=~p count=~p ",
                         [RemotePid, length(OurDump)], F).

%% Only the node that owns the data could update/remove the data.
%% Ideally Key should contain inserter node info (for cleaning).
insert(Tab, Rec) ->
    Servers = other_servers(Tab),
    ets:insert(Tab, Rec),
    %% Insert to other nodes and block till written
    Monitors = insert_to_remote_nodes(Servers, Rec),
    wait_for_updated(Monitors).

insert_to_remote_nodes([{RemotePid, ProxyPid} | Servers], Rec) ->
    Mon = erlang:monitor(process, ProxyPid),
    erlang:send(RemotePid, {insert_from_remote_node, Mon, self(), Rec}, [noconnect, nosuspend]),
    [Mon | insert_to_remote_nodes(Servers, Rec)];
insert_to_remote_nodes([], _Rec) ->
    [].

delete(Tab, Key) ->
    delete_many(Tab, [Key]).

%% A separate function for multidelete (because key COULD be a list, so no confusion)
delete_many(Tab, Keys) ->
    Servers = other_servers(Tab),
    ets_delete_keys(Tab, Keys),
    Monitors = delete_from_remote_nodes(Servers, Keys),
    wait_for_updated(Monitors).

delete_from_remote_nodes([{RemotePid, ProxyPid} | Servers], Keys) ->
    Mon = erlang:monitor(process, ProxyPid),
    erlang:send(RemotePid, {delete_from_remote_node, Mon, self(), Keys}, [noconnect, nosuspend]),
    [Mon | delete_from_remote_nodes(Servers, Keys)];
delete_from_remote_nodes([], _Keys) ->
    [].

wait_for_updated([Mon | Monitors]) ->
    receive
        {updated, Mon} ->
            wait_for_updated(Monitors);
        {'DOWN', Mon, process, _Pid, _Reason} ->
            wait_for_updated(Monitors)
    end;
wait_for_updated([]) ->
    ok.

other_servers(Tab) ->
    kiss_pt:get(Tab).

other_nodes(Tab) ->
    lists:sort([node(Pid) || {Pid, _} <- other_servers(Tab)]).

init([Tab, Opts]) ->
    ets:new(Tab, [ordered_set, named_table,
                  public, {read_concurrency, true}]),
    update_pt(Tab, []),
    {ok, #{tab => Tab, other_servers => [], opts => Opts}}.

handle_call({join, RemotePid}, _From, State) ->
    handle_join(RemotePid, State);
handle_call({remote_add_node_to_schema, ServerPid, OtherPids}, _From, State) ->
    handle_remote_add_node_to_schema(ServerPid, OtherPids, State);
handle_call({remote_just_add_node_to_schema, ServerPid, OtherPids}, _From, State) ->
    handle_remote_just_add_node_to_schema(ServerPid, OtherPids, State);
handle_call({send_dump_to_remote_node, FromPid, Dump}, _From, State) ->
    handle_send_dump_to_remote_node(FromPid, Dump, State);
handle_call({insert, Rec}, _From, State = #{tab := Tab}) ->
    ets:insert(Tab, Rec),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Mon, process, Pid, _Reason}, State) ->
    handle_down(Pid, State);
handle_info({insert_from_remote_node, Mon, Pid, Rec}, State = #{tab := Tab}) ->
    ets:insert(Tab, Rec),
    reply_updated(Pid, Mon),
    {noreply, State};
handle_info({delete_from_remote_node, Mon, Pid, Keys}, State = #{tab := Tab}) ->
    ets_delete_keys(Tab, Keys),
    reply_updated(Pid, Mon),
    {noreply, State}.

terminate(_Reason, _State = #{tab := Tab}) ->
    kiss_pt:put(Tab, []),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_join(RemotePid, State = #{tab := Tab, other_servers := Servers}) when is_pid(RemotePid) ->
    case lists:keymember(RemotePid, 1, Servers) of
        true ->
            %% Already added
            {reply, ok, State};
        false ->
            KnownPids = [Pid || {Pid, _} <- Servers],
            Self = self(),
            %% Remote gen_server calls here are "safe"
            case remote_add_node_to_schema(RemotePid, Self, KnownPids) of
                {ok, Dump, OtherPids} ->
                    NewPids = [RemotePid | OtherPids],
                    %% Let all nodes to know each other
                    [remote_just_add_node_to_schema(Pid, Self, KnownPids) || Pid <- OtherPids],
                    [remote_just_add_node_to_schema(Pid, Self, NewPids) || Pid <- KnownPids],
                    Servers2 = add_servers(NewPids, Servers),
                    %% Ask our node to replicate data there before applying the dump
                    update_pt(Tab, Servers2),
                    OurDump = dump(Tab),
                    %% Send to all nodes from that partition
                    [send_dump_to_remote_node(Pid, Self, OurDump) || Pid <- NewPids],
                    %% Apply to our nodes
                    [send_dump_to_remote_node(Pid, Self, Dump) || Pid <- KnownPids],
                    insert_many(Tab, Dump),
                    %% Add ourself into remote schema
                    %% Add remote nodes into our schema
                    %% Copy from our node / Copy into our node
                    {reply, ok, State#{other_servers => Servers2}};
               Other ->
                    error_logger:error_msg("remote_add_node_to_schema failed ~p", [Other]),
                    {reply, {error, remote_add_node_to_schema_failed}, State}
            end
    end.

handle_remote_add_node_to_schema(ServerPid, OtherPids, State = #{tab := Tab}) ->
    case handle_remote_just_add_node_to_schema(ServerPid, OtherPids, State) of
        {reply, {ok, KnownPids}, State2} ->
            {reply, {ok, dump(Tab), KnownPids}, State2};
        Other ->
            Other
    end.

handle_remote_just_add_node_to_schema(RemotePid, OtherPids, State = #{tab := Tab, other_servers := Servers}) ->
    Servers2 = add_servers([RemotePid | OtherPids], Servers),
    update_pt(Tab, Servers2),
    KnownPids = [Pid || {Pid, _} <- Servers],
    {reply, {ok, KnownPids}, State#{other_servers => Servers2}}.

add_servers(Pids, Servers) ->
    lists:sort(start_proxies_for(Pids, Servers) ++ Servers).

start_proxies_for([RemotePid | OtherPids], AlreadyAddedNodes)
  when is_pid(RemotePid), RemotePid =/= self() ->
    case lists:keymember(RemotePid, 1, AlreadyAddedNodes) of
        false ->
            {ok, ProxyPid} = kiss_proxy:start(RemotePid),
            erlang:monitor(process, ProxyPid),
            [{RemotePid, ProxyPid} | start_proxies_for(OtherPids, AlreadyAddedNodes)];
        true ->
            error_logger:info_msg("what=already_added remote_pid=~p node=~p", [RemotePid, node(RemotePid)]),
            start_proxies_for(OtherPids, AlreadyAddedNodes)
    end;
start_proxies_for([], _AlreadyAddedNodes) ->
    [].

handle_send_dump_to_remote_node(_FromPid, Dump, State = #{tab := Tab}) ->
    insert_many(Tab, Dump),
    {reply, ok, State}.

insert_many(Tab, Recs) ->
    ets:insert(Tab, Recs).

handle_down(ProxyPid, State = #{tab := Tab, other_servers := Servers}) ->
    case lists:keytake(ProxyPid, 2, Servers) of
        {value, {RemotePid, _}, Servers2} ->
            %% Down from a proxy
            update_pt(Tab, Servers2),
            call_user_handle_down(RemotePid, State),
            {noreply, State#{other_servers => Servers2}};
        false ->
            %% This should not happen
            error_logger:error_msg("handle_down failed proxy_pid=~p state=~0p", [ProxyPid, State]),
            {noreply, State}
    end.

%% Called each time other_servers changes
update_pt(Tab, Servers2) ->
    kiss_pt:put(Tab, Servers2).

pids_to_nodes(Pids) ->
    lists:map(fun node/1, Pids).

ets_delete_keys(Tab, [Key|Keys]) ->
    ets:delete(Tab, Key),
    ets_delete_keys(Tab, Keys);
ets_delete_keys(_Tab, []) ->
    ok.

%% Cleanup
call_user_handle_down(RemotePid, _State = #{tab := Tab, opts := Opts}) ->
    case Opts of
        #{handle_down := F} ->
            FF = fun() -> F(#{remote_pid => RemotePid, table => Tab}) end,
            kiss_long:run_safely("task=call_user_handle_down table=~p remote_pid=~p remote_node=~p ",
                                 [Tab, RemotePid, node(RemotePid)], FF);
        _ ->
            ok
    end.

reply_updated(Pid, Mon) ->
    %% We really don't wanna block this process
    erlang:send(Pid, {updated, Mon}, [noconnect, nosuspend]).
