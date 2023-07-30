%% Contains processes, that are waiting for writes to finish.
%% Collects acks from nodes in the cluster.
-module(cets_ack).
-behaviour(gen_server).

%% API, called from cets module
-export([
    start_link/1,
    add/3,
    ack/3,
    send_remote_down/2,
    erase/1
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
    cets:called_from() => Mask :: integer()
}.

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, false, []).

-spec add(pid(), cets:called_from(), integer()) -> ok.
add(AckPid, From, Bits) ->
    AckPid ! {add, From, Bits},
    ok.

%% Called by a remote server after an operation is applied.
%% Alias is an alias from the original gen_server:call made by a client.
%% Mask identifies the remote server.
-spec ack(pid(), cets:called_from(), integer()) -> ok.
ack(AckPid, From, Mask) ->
    %% nosuspend makes message sending unreliable
    erlang:send(AckPid, {ack, From, Mask}, [noconnect]),
    ok.

-spec send_remote_down(pid(), non_neg_integer()) -> ok.
send_remote_down(AckPid, Num) ->
    AckPid ! {cets_remote_down, Num},
    ok.

-spec erase(pid()) -> ok.
erase(AckPid) ->
    AckPid ! erase,
    ok.

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
handle_info({ack, From, Mask}, State) ->
    {noreply, handle_updated(From, Mask, State)};
handle_info({add, From, Bits}, State) ->
    {noreply, maps:put(From, Bits, State)};
handle_info({cets_remote_down, Num}, State) ->
    {noreply, handle_remote_down(Num, State)};
handle_info(erase, State) ->
    {noreply, handle_erase(State)};
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec handle_erase(state()) -> state().
handle_erase(State) ->
    maps:foreach(fun send_down_all/2, State),
    #{}.

send_down_all(From, _Val) ->
    cets_call:reply(From).

-spec handle_remote_down(integer(), state()) -> state().
handle_remote_down(Num, State) ->
    Mask = cets_bits:unset_flag_mask(Num),
    F = fun(From, Bits, Acc) -> handle_updated(From, Mask, Acc, Bits) end,
    maps:fold(F, State, State).

-spec handle_updated(cets:called_from(), integer(), state()) -> state().
handle_updated(From, Mask, State) ->
    handle_updated(From, Mask, State, maps:get(From, State, false)).

handle_updated(From, Mask, State, Bits) when is_integer(Bits) ->
    case cets_bits:apply_mask(Mask, Bits) of
        0 ->
            cets_call:reply(From),
            maps:remove(From, State);
        Bits2 ->
            State#{From := Bits2}
    end;
handle_updated(_From, _Mask, State, false) ->
    State.
