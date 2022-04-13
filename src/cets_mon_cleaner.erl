%% Monitor Table contains processes, that are waiting for writes to finish.
%% It is usually cleaned automatically.
%% Unless the caller process crashes.
%% This server removes such entries from the MonTab.
%% We don't expect the MonTab to be extremely big, so this check should be quick.
-module(cets_mon_cleaner).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

-type timer_ref() :: reference() | undefined.
-type state() :: #{
        mon_tab := atom(),
        interval := non_neg_integer(),
        timer_ref := timer_ref()
       }.

start_link(Name, MonTab) ->
    gen_server:start_link({local, Name}, ?MODULE, MonTab, []).

-spec init(atom()) -> {ok, state()}.
init(MonTab) ->
    State = #{mon_tab => MonTab, interval => 30000, timer_ref => undefined},
    {ok, schedule_check(State)}.

handle_call(Msg, From, State) ->
    ?LOG_ERROR(#{what => unexpected_call, msg => Msg, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

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
schedule_check(State = #{interval := Interval}) ->
    cancel_old_timer(State),
    TimerRef = erlang:send_after(Interval, self(), check),
    State#{timer_ref := TimerRef}.

cancel_old_timer(#{timer_ref := OldRef}) when is_reference(OldRef) ->
    erlang:cancel_timer(OldRef);
cancel_old_timer(_State) ->
    ok.

-spec handle_check(state()) -> state().
handle_check(State = #{mon_tab := MonTab}) ->
    check_loop(ets:tab2list(MonTab), MonTab),
    schedule_check(State).

check_loop([{Mon, Pid}|List], MonTab) ->
    case is_process_alive(Pid) of
        false ->
            ets:delete(MonTab, Mon);
        true ->
            ok
    end,
    check_loop(List, MonTab);
check_loop([], _MonTab) ->
    ok.
