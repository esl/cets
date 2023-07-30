%% Monitor Table contains processes, that are waiting for writes to finish.
%% It is usually cleaned automatically.
%% Unless the caller process crashes.
%% This server removes such entries from the MonTab.
%% We don't expect the MonTab to be extremely big, so this check should be quick.
-module(cets_mon_cleaner).
-behaviour(gen_server).

-export([start_link/1]).
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
    interval := non_neg_integer(),
    timer_ref := reference(),
    reference() => pid()
}.

state_keys() -> [].

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, false, []).

init(_) ->
    State = #{},
    {ok, State}.

handle_call(dump, _From, State) ->
    {reply, maps:without(state_keys(), State), State};
handle_call(Msg, From, State) ->
    ?LOG_ERROR(#{what => unexpected_call, msg => Msg, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

handle_info({cets_updated, Mon, Num}, State) when is_reference(Mon) ->
    {noreply, handle_updated(Mon, Num, State)};
handle_info({Mon, Bits}, State) when is_reference(Mon) ->
    {noreply, maps:put(Mon, Bits, State)};
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

handle_erase(State) ->
    maps:foreach(fun send_down_all/2, State),
    maps:with(state_keys(), State).

send_down_all(Mon, _Val) when is_reference(Mon) ->
    Mon ! {cets_ok, Mon};
send_down_all(_Key, _Val) ->
    true.

handle_remote_down(Num, State) ->
    maps:fold(fun(K, V, Acc) when is_reference(K) -> handle_updated(K, Num, Acc, V); (_, _, Acc) -> Acc end, State, State).

handle_updated(Mon, Num, State) ->
    handle_updated(Mon, Num, State, maps:get(Mon, State, false)).

handle_updated(Mon, Num, State, Bits) when is_integer(Bits) ->
    case unset_flag(Num, Bits) of
        0 ->
            Mon ! {cets_ok, Mon},
            maps:remove(Mon, State);
        Bits2 ->
            State#{Mon := Bits2}
    end;
handle_updated(Mon, Num, State, false) ->
    {noreply, State}.

unset_flag(Pos, Bits) ->
    Bits band (bnot (1 bsl Pos)).
