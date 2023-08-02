%% Monitor Table contains processes, that are waiting for writes to finish.
%% It is usually cleaned automatically.
%% Unless the caller process crashes.
%% This server removes such entries from the MonTab.
%% We don't expect the MonTab to be extremely big, so this check should be quick.
-module(cets_ack).
-behaviour(gen_server).

-export([
    start_link/1,
    add/3,
    ack/3,
    send_remote_down/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("kernel/include/logger.hrl").

-type state() :: #{
    gen_server:from() => [pid()]
}.

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

-spec add(pid(), gen_server:from(), [pid()]) -> ok.
add(AckPid, From, Servers) ->
    AckPid ! {add, From, Servers},
    ok.

%% Called by a remote server after an operation is applied.
-spec ack(pid(), gen_server:from(), pid()) -> ok.
ack(AckPid, From, RemotePid) ->
    %% nosuspend makes message sending unreliable
    erlang:send(AckPid, {ack, From, RemotePid}, [noconnect]),
    ok.

%% Calls ack(AckPid, From, RemotePid) for all locally tracked From entries
-spec send_remote_down(pid(), pid()) -> ok.
send_remote_down(AckPid, RemotePid) ->
    AckPid ! {cets_remote_down, RemotePid},
    ok.

-spec init(atom()) -> {ok, state()}.
init(_) ->
    State = #{},
    {ok, State}.

-spec handle_call(term(), _From, state()) -> {reply, state()}.
handle_call(Msg, From, State) ->
    ?LOG_ERROR(#{what => unexpected_call, msg => Msg, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({ack, From, RemotePid}, State) ->
    {noreply, handle_updated(From, RemotePid, State)};
handle_info({add, From, Servers}, State) ->
    {noreply, maps:put(From, Servers, State)};
handle_info({cets_remote_down, RemotePid}, State) ->
    {noreply, handle_remote_down(RemotePid, State)};
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec handle_remote_down(pid(), state()) -> state().
handle_remote_down(RemotePid, State) ->
    %% Call handle_updated for all pending operations
    F = fun(From, Servers, Acc) -> handle_updated(From, RemotePid, Acc, Servers) end,
    maps:fold(F, State, State).

-spec handle_updated(gen_server:from(), pid(), state()) -> state().
handle_updated(From, RemotePid, State) ->
    Servers = maps:get(From, State, []),
    handle_updated(From, RemotePid, State, Servers).

handle_updated(From, RemotePid, State, Servers) ->
    case lists:delete(RemotePid, Servers) of
        [] ->
            gen_server:reply(From, ok),
            maps:remove(From, State);
        Servers2 ->
            State#{From := Servers2}
    end.
