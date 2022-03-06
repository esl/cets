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
-module(kiss).
-behaviour(gen_server).

-export([start/2, stop/1, insert/2, delete/2, delete_many/2]).
-export([dump/1, remote_dump/1, send_dump_to_remote_node/3]).
-export([other_nodes/1, other_pids/1]).
-export([pause/1, unpause/1, sync/1, ping/1]).
-export([info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([insert_request/2, delete_request/2, delete_many_request/2, wait_response/2]).

-include_lib("kernel/include/logger.hrl").

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
    gen_server:start({local, Tab}, ?MODULE, [Tab, Opts], []).

stop(Tab) ->
    gen_server:stop(Tab).

dump(Tab) ->
    ets:tab2list(Tab).

remote_dump(Pid) ->
    short_call(Pid, remote_dump).

send_dump_to_remote_node(RemotePid, NewPids, OurDump) ->
    Msg = {send_dump_to_remote_node, NewPids, OurDump},
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
    {ok, WaitInfo} = gen_server:call(Server, {delete, Keys}),
    wait_for_updated(WaitInfo).

insert_request(Server, Rec) ->
    gen_server:send_request(Server, {insert, Rec}).

delete_request(Tab, Key) ->
    delete_many_request(Tab, [Key]).

delete_many_request(Server, Keys) ->
    gen_server:send_request(Server, {delete, Keys}).

wait_response(RequestId, Timeout) ->
    case gen_server:wait_response(RequestId, Timeout) of
        {reply, {ok, WaitInfo}} ->
            wait_for_updated(WaitInfo);
        Other ->
            Other
    end.

other_servers(Server) ->
    gen_server:call(Server, other_servers).

other_nodes(Server) ->
    lists:usort(pids_to_nodes(other_pids(Server))).

other_pids(Server) ->
    servers_to_pids(other_servers(Server)).

pause(RemotePid) ->
    short_call(RemotePid, pause).

unpause(RemotePid) ->
    short_call(RemotePid, unpause).

sync(RemotePid) ->
    short_call(RemotePid, sync).

ping(RemotePid) ->
    short_call(RemotePid, ping).

info(Server) ->
    gen_server:call(Server, get_info).

%% gen_server callbacks

init([Tab, Opts]) ->
    MonTab = list_to_atom(atom_to_list(Tab) ++ "_mon"),
    ets:new(Tab, [ordered_set, named_table, public]),
    ets:new(MonTab, [public, named_table]),
    kiss_mon_cleaner:start_link(MonTab, MonTab),
    {ok, #{tab => Tab, mon_tab => MonTab,
           other_servers => [], opts => Opts, backlog => [],
           paused => false, pause_monitor => undefined}}.

handle_call({insert, Rec}, From, State = #{paused := false}) ->
    handle_insert(Rec, From, State);
handle_call({delete, Keys}, From, State = #{paused := false}) ->
    handle_delete(Keys, From, State);
handle_call(other_servers, _From, State = #{other_servers := Servers}) ->
    {reply, Servers, State};
handle_call(sync, _From, State = #{other_servers := Servers}) ->
    [ping(Pid) || Pid <- servers_to_pids(Servers)],
    {reply, ok, State};
handle_call(ping, _From, State) ->
    {reply, ping, State};
handle_call(remote_dump, _From, State = #{tab := Tab}) ->
    {reply, {ok, dump(Tab)}, State};
handle_call({send_dump_to_remote_node, NewPids, Dump}, _From, State) ->
    handle_send_dump_to_remote_node(NewPids, Dump, State);
handle_call(pause, _From = {FromPid, _}, State) ->
    Mon = erlang:monitor(process, FromPid),
    {reply, ok, State#{paused => true, pause_monitor => Mon}};
handle_call(unpause, _From, State) ->
    handle_unpause(State);
handle_call(get_info, _From, State) ->
    handle_get_info(State);
handle_call(Msg, From, State = #{paused := true, backlog := Backlog}) ->
    {noreply, State#{backlog => [{Msg, From} | Backlog]}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({remote_insert, Mon, Pid, Rec}, State = #{tab := Tab}) ->
    ets:insert(Tab, Rec),
    reply_updated(Pid, Mon),
    {noreply, State};
handle_info({remote_delete, Mon, Pid, Keys}, State = #{tab := Tab}) ->
    ets_delete_keys(Tab, Keys),
    reply_updated(Pid, Mon),
    {noreply, State};
handle_info({'DOWN', Mon, process, Pid, _Reason}, State) ->
    handle_down(Mon, Pid, State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal logic

handle_send_dump_to_remote_node(NewPids, Dump,
                                State = #{tab := Tab, other_servers := Servers}) ->
    ets:insert(Tab, Dump),
    Servers2 = add_servers(NewPids, Servers),
    {reply, ok, State#{other_servers => Servers2}}.

handle_down(Mon, PausedByPid, State = #{pause_monitor := Mon}) ->
    ?LOG_ERROR(#{what => pause_owner_crashed,
                 state => State, paused_by_pid => PausedByPid}),
    {reply, ok, State2} = handle_unpause(State),
    {noreply, State2};
handle_down(_Mon, RemotePid, State = #{other_servers := Servers, mon_tab := MonTab}) ->
    case lists:member(RemotePid, Servers) of
        true ->
            Servers2 = lists:delete(RemotePid, Servers),
            notify_remote_down(RemotePid, MonTab),
            %% Down from a proxy
            call_user_handle_down(RemotePid, State),
            {noreply, State#{other_servers => Servers2}};
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

servers_to_pids(Servers) ->
    [Pid || Pid <- Servers].

has_remote_pid(RemotePid, Servers) ->
    lists:member(RemotePid, Servers).

reply_updated(Pid, Mon) ->
    %% We really don't wanna block this process
    erlang:send(Pid, {updated, Mon, self()}, [noconnect, nosuspend]).

send_to_remote(RemotePid, Msg) ->
    erlang:send(RemotePid, Msg, [noconnect, nosuspend]).

handle_insert(Rec, _From = {FromPid, Mon},
              State = #{tab := Tab, mon_tab := MonTab, other_servers := Servers}) ->
    ets:insert(Tab, Rec),
    %% Insert to other nodes and block till written
    WaitInfo = replicate(Mon, Servers, remote_insert, Rec, FromPid, MonTab),
    {reply, {ok, WaitInfo}, State}.

handle_delete(Keys, _From = {FromPid, Mon},
              State = #{tab := Tab, mon_tab := MonTab, other_servers := Servers}) ->
    ets_delete_keys(Tab, Keys),
    %% Insert to other nodes and block till written
    WaitInfo = replicate(Mon, Servers, remote_delete, Keys, FromPid, MonTab),
    {reply, {ok, WaitInfo}, State}.

replicate(Mon, Servers, Cmd, Payload, FromPid, MonTab) ->
    %% Reply would be routed directly to FromPid
    Msg = {Cmd, Mon, FromPid, Payload},
    replicate2(Servers, Msg),
    ets:insert(MonTab, {Mon, FromPid}),
    {Mon, Servers, MonTab}.

replicate2([RemotePid | Servers], Msg) ->
    send_to_remote(RemotePid, Msg),
    replicate2(Servers, Msg);
replicate2([], _Msg) ->
    ok.

wait_for_updated({Mon, Servers, MonTab}) ->
    try
        wait_for_updated2(Mon, Servers)
    after
        ets:delete(MonTab, Mon)
    end.

wait_for_updated2(_Mon, []) ->
    ok;
wait_for_updated2(Mon, Servers) ->
    receive
        {updated, Mon, Pid} ->
            Servers2 = lists:delete(Pid, Servers),
            wait_for_updated2(Mon, Servers2);
        {remote_down, Mon, Pid} ->
            Servers2 = lists:delete(Pid, Servers),
            wait_for_updated2(Mon, Servers2)
    end.

apply_backlog([{Msg, From} | Backlog], State) ->
    {reply, Reply, State2} = handle_call(Msg, From, State),
    gen_server:reply(From, Reply),
    apply_backlog(Backlog, State2);
apply_backlog([], State) ->
    State.

short_call(RemotePid, Msg) ->
    F = fun() -> gen_server:call(RemotePid, Msg, infinity) end,
    Info = #{task => Msg,
             remote_pid => RemotePid, remote_node => node(RemotePid)},
    kiss_long:run_safely(Info, F).

%% Theoretically we can support mupltiple pauses (but no need for now because
%% we pause in the global locked function)
handle_unpause(State = #{paused := false}) ->
    {reply, {error, already_unpaused}, State};
handle_unpause(State = #{backlog := Backlog, pause_monitor := Mon}) ->
    erlang:demonitor(Mon, [flush]),
    State2 = State#{paused => false, backlog := [], pause_monitor => undefined},
    {reply, ok, apply_backlog(lists:reverse(Backlog), State2)}.

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
            kiss_long:run_safely(Info, FF);
        _ ->
            ok
    end.
