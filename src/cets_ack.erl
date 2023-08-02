%% CETS Ack Helper Process
%%
%% Contains a list of processes, that are waiting for writes to finish.
%% Collects acks from nodes in the cluster.
%% When one of the remote nodes go down, the server stops waiting for acks from it.
-module(cets_ack).
-behaviour(gen_server).

%% API functions
-export([
    start_link/1,
    set_servers/2,
    add/2,
    ack/3,
    send_remote_down/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("kernel/include/logger.hrl").

-type ack_pid() :: pid().
-type server_pid() :: cets:server_pid().
-type from() :: gen_server:from().
-type state() :: #{
    servers := [server_pid()],
    %% We store tasks directly in the state map
    from() => [server_pid(), ...]
}.

-export_type([ack_pid/0]).

%% API functions

-spec start_link(cets:table_name()) -> {ok, ack_pid()}.
start_link(Tab) ->
    Name = list_to_atom(atom_to_list(Tab) ++ "_ack"),
    gen_server:start_link({local, Name}, ?MODULE, [], []).

%% Sets a list of servers to be used for newly added tasks
-spec set_servers(ack_pid(), [server_pid()]) -> ok.
set_servers(AckPid, Servers) ->
    gen_server:cast(AckPid, {set_servers, Servers}),
    ok.

%% Adds a new task to wait replies
-spec add(ack_pid(), from()) -> ok.
add(AckPid, From) ->
    AckPid ! {add, From},
    ok.

%% Called by a remote server after an operation is applied.
-spec ack(ack_pid(), from(), server_pid()) -> ok.
ack(AckPid, From, RemotePid) ->
    %% nosuspend makes message sending unreliable
    erlang:send(AckPid, {ack, From, RemotePid}, [noconnect]),
    ok.

%% Calls ack(AckPid, From, RemotePid) for all locally tracked From entries
-spec send_remote_down(ack_pid(), server_pid()) -> ok.
send_remote_down(AckPid, RemotePid) ->
    AckPid ! {cets_remote_down, RemotePid},
    ok.

%% gen_server callbacks

-spec init(atom()) -> {ok, state()}.
init(_) ->
    {ok, #{servers => []}}.

-spec handle_call(term(), _From, state()) -> {reply, state()}.
handle_call(Msg, From, State) ->
    ?LOG_ERROR(#{what => unexpected_call, msg => Msg, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast({set_servers, Servers}, State) ->
    {noreply, State#{servers := Servers}};
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({ack, From, RemotePid}, State) ->
    {noreply, handle_updated(From, RemotePid, State)};
handle_info({add, From}, State) ->
    {noreply, handle_add(From, State)};
handle_info({cets_remote_down, RemotePid}, State) ->
    {noreply, handle_remote_down(RemotePid, State)};
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

%% Calling add when the server list is empty is not allowed
%% (and CETS process checks for it when sending us a new task)
handle_add(From, State = #{servers := [_ | _] = Servers}) ->
    maps:put(From, Servers, State);
handle_add(_, _) ->
    error(unexpected_add_msg).

-spec handle_remote_down(server_pid(), state()) -> state().
handle_remote_down(RemotePid, State) ->
    %% Call handle_updated for all pending tasks
    F = fun
        (Key, _Value, State2) when is_atom(Key) ->
            %% Ignore keys that are not used for tasks (i.e. servers key)
            State2;
        (From, Servers, State2) ->
            handle_updated(From, RemotePid, Servers, State2)
    end,
    maps:fold(F, State, State).

-spec handle_updated(from(), server_pid(), state()) -> state().
handle_updated(From, RemotePid, State) ->
    case State of
        #{From := Servers} ->
            handle_updated(From, RemotePid, Servers, State);
        _ ->
            %% Ignore unknown From
            State
    end.

-spec handle_updated(from(), server_pid(), [server_pid(), ...], state()) -> state().
handle_updated(From, RemotePid, Servers, State) ->
    %% Removes the remote server from a waiting list
    case lists:delete(RemotePid, Servers) of
        [] ->
            %% Send a reply to our client
            %% confirming that his operation has been finished
            gen_server:reply(From, ok),
            %% Remove the task
            maps:remove(From, State);
        Servers2 ->
            %% Set an updated waiting list
            State#{From := Servers2}
    end.
