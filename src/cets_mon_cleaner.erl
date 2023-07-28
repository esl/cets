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

state_keys() -> [interval, timer_ref].

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, false, []).

init(_) ->
    Interval = 30000,
    State = #{
        interval => Interval,
        timer_ref => start_timer(Interval)
    },
    {ok, State}.

handle_call(dump, _From, State) ->
    {reply, maps:without(state_keys(), State), State};
handle_call(Msg, From, State) ->
    ?LOG_ERROR(#{what => unexpected_call, msg => Msg, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

handle_info({Mon, Pid}, State) when is_reference(Mon) ->
    {noreply, maps:put(Mon, Pid, State)};
handle_info(Mon, State) when is_reference(Mon) ->
    {noreply, maps:remove(Mon, State)};
handle_info({cets_remote_down, Num}, State) ->
    {noreply, handle_remote_down(Num, State)};
handle_info(erase, State) ->
    {noreply, handle_erase(State)};
handle_info(check, State) ->
    {noreply, handle_check(State)};
handle_info(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec schedule_check(state()) -> state().
schedule_check(State = #{interval := Interval, timer_ref := OldRef}) ->
    %% Match result to prevent the Dialyzer warning
    _ = erlang:cancel_timer(OldRef),
    flush_all_checks(),
    State#{timer_ref := start_timer(Interval)}.

flush_all_checks() ->
    receive
        check -> flush_all_checks()
    after 0 -> ok
    end.

start_timer(Interval) ->
    erlang:send_after(Interval, self(), check).

-spec handle_check(state()) -> state().
handle_check(State) ->
    State2 = maps:filter(fun check_filter/2, State),
    schedule_check(State2).

check_filter(Mon, Pid) when is_reference(Mon) ->
    is_process_alive(Pid);
check_filter(_Key, _Val) ->
    true.

handle_erase(State) ->
    maps:foreach(fun send_down_all/2, State),
    maps:with(state_keys(), State).

send_down_all(Mon, Pid) when is_reference(Mon) ->
    Pid ! {cets_remote_down, Mon, all};
send_down_all(_Key, _Val) ->
    true.

handle_remote_down(Num, State) ->
    maps:foreach(fun(K, V) -> send_remote_down(K, V, Num) end, State),
    State.

send_remote_down(Mon, Pid, Num) when is_reference(Mon) ->
    Pid ! {cets_remote_down, Mon, Num};
send_remote_down(_Key, _Val, _Num) ->
    true.
