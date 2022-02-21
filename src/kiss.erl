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
-export([start/2, stop/1, dump/1, insert/2, join/2, other_nodes/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-behaviour(gen_server).

%% Table and server has the same name
start(Tab, _Opts) when is_atom(Tab) ->
    gen_server:start({local, Tab}, ?MODULE, [Tab], []).

stop(Tab) ->
    gen_server:stop(Tab).

dump(Tab) ->
    ets:tab2list(Tab).

%% Adds a node to a cluster.
%% Writes from other nodes would wait for join completion.
join(Tab, RemotePid) when is_pid(RemotePid) ->
    F = fun() -> gen_server:call(Tab, {join, RemotePid}, infinity) end,
    kiss_long:run("task=join table=~p remote_pid=~p remote_node=~p ",
                  [Tab, RemotePid, node(RemotePid)], F).

remote_add_node_to_schema(RemotePid, ServerPid, OtherPids) ->
    F = fun() -> gen_server:call(RemotePid, {remote_add_node_to_schema, ServerPid, OtherPids}, infinity) end,
    kiss_long:run("task=remote_add_node_to_schema remote_pid=~p remote_node=~p other_pids=~0p other_nodes=~0p ",
                  [RemotePid, node(RemotePid), OtherPids, pids_to_nodes(OtherPids)], F).

remote_just_add_node_to_schema(RemotePid, ServerPid, OtherPids) ->
    F = fun() -> gen_server:call(RemotePid, {remote_just_add_node_to_schema, ServerPid, OtherPids}, infinity) end,
    kiss_long:run("task=remote_just_add_node_to_schema remote_pid=~p remote_node=~p other_pids=~0p other_nodes=~0p ",
                  [RemotePid, node(RemotePid), OtherPids, pids_to_nodes(OtherPids)], F).

send_dump_to_remote_node(_RemotePid, _FromPid, []) ->
    skipped;
send_dump_to_remote_node(RemotePid, FromPid, OurDump) ->
    F = fun() -> gen_server:call(RemotePid, {send_dump_to_remote_node, FromPid, OurDump}, infinity) end,
    kiss_long:run("task=send_dump_to_remote_node remote_pid=~p count=~p ",
                  [RemotePid, length(OurDump)], F).

%% Inserts do not override data (i.e. immunable)
%% But we can remove data
%% Key is {USR, Sid, UpdateNumber}
%% Where first UpdateNumber is 0
insert(Tab, Rec) ->
    Servers = other_servers(Tab),
    ets:insert(Tab, Rec),
    %% Insert to other nodes and block till written
    Monitors = insert_to_remote_nodes(Servers, Rec),
    wait_for_inserted(Monitors).

insert_to_remote_nodes([{RemotePid, ProxyPid} | Servers], Rec) ->
    Mon = erlang:monitor(process, ProxyPid),
    erlang:send(RemotePid, {insert_from_remote_node, Mon, self(), Rec}, [noconnect]),
    [Mon | insert_to_remote_nodes(Servers, Rec)];
insert_to_remote_nodes([], _Rec) ->
    [].

wait_for_inserted([Mon | Monitors]) ->
    receive
        {inserted, Mon2} when Mon2 =:= Mon ->
            wait_for_inserted(Monitors);
        {'DOWN', Mon2, process, _Pid, _Reason} when Mon2 =:= Mon ->
            wait_for_inserted(Monitors)
    end;
wait_for_inserted([]) ->
    ok.

other_servers(Tab) ->
%   gen_server:call(Tab, get_other_servers).
    kiss_pt:get(Tab).

other_nodes(Tab) ->
    lists:sort([node(Pid) || {Pid, _} <- other_servers(Tab)]).

init([Tab]) ->
    ets:new(Tab, [ordered_set, named_table,
                  public, {read_concurrency, true}]),
    update_pt(Tab, []),
    {ok, #{tab => Tab, other_servers => []}}.

handle_call({join, RemotePid}, _From, State) ->
    handle_join(RemotePid, State);
handle_call({remote_add_node_to_schema, ServerPid, OtherPids}, _From, State) ->
    handle_remote_add_node_to_schema(ServerPid, OtherPids, State);
handle_call({remote_just_add_node_to_schema, ServerPid, OtherPids}, _From, State) ->
    handle_remote_just_add_node_to_schema(ServerPid, OtherPids, State);
handle_call({send_dump_to_remote_node, FromPid, Dump}, _From, State) ->
    handle_send_dump_to_remote_node(FromPid, Dump, State);
handle_call(get_other_servers, _From, State = #{other_servers := Servers}) ->
    {reply, Servers, State};
handle_call({insert, Rec}, _From, State = #{tab := Tab}) ->
    ets:insert(Tab, Rec),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Mon, Pid, _Reason}, State) ->
    handle_down(Pid, State);
handle_info({insert_from_remote_node, Mon, Pid, Rec}, State = #{tab := Tab}) ->
    ets:insert(Tab, Rec),
    Pid ! {inserted, Mon},
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
            %% TODO can crash
            case remote_add_node_to_schema(RemotePid, self(), KnownPids) of
                {ok, Dump, OtherPids} ->
                    %% Let all nodes to know each other
                    [remote_just_add_node_to_schema(Pid, self(), KnownPids) || Pid <- OtherPids],
                    [remote_just_add_node_to_schema(Pid, self(), [RemotePid | OtherPids]) || Pid <- KnownPids],
                    Servers2 = lists:usort(start_proxies_for([RemotePid | OtherPids], Servers) ++ Servers),
                    %% Ask our node to replicate data there before applying the dump
                    update_pt(Tab, Servers2),
                    OurDump = dump(Tab),
                    %% Send to all nodes from that partition
                    [send_dump_to_remote_node(Pid, self(), OurDump) || Pid <- [RemotePid | OtherPids]],
                    %% Apply to our nodes
                    [send_dump_to_remote_node(Pid, self(), Dump) || Pid <- KnownPids],
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
    Servers2 = lists:usort(start_proxies_for([RemotePid | OtherPids], Servers) ++ Servers),
    update_pt(Tab, Servers2),
    KnownPids = [Pid || {Pid, _} <- Servers],
    {reply, {ok, KnownPids}, State#{other_servers => Servers2}}.

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

handle_down(Pid, State = #{tab := Tab, other_servers := Servers}) ->
    %% Down from a proxy
    Servers2 = lists:keydelete(Pid, 2, Servers),
    update_pt(Tab, Servers2),
    {noreply, State#{other_servers => Servers2}}.

%% Called each time other_servers changes
update_pt(Tab, Servers2) ->
    kiss_pt:put(Tab, Servers2).

pids_to_nodes(Pids) ->
    lists:map(fun node/1, Pids).
