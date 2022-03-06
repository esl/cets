%% Monitor Table contains processes, that are waiting for writes to finish.
%% It is usually cleaned automatically.
%% Unless the caller process crashes.
%% This server removes such entries from the MonTab.
%% We don't expect the MonTab to be extremely big, so this check should be quick.
-module(kiss_mon_cleaner).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

start_link(Name, MonTab) ->
    gen_server:start_link({local, Name}, ?MODULE, [MonTab], []).

init([MonTab]) ->
    State = #{mon_tab => MonTab, interval => 30000},
    schedule_check(State),
    {ok, State}.

handle_call(_Reply, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, State) ->
    handle_check(State),
    {stop, normal, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

schedule_check(State = #{interval := Interval}) ->
    cancel_old_timer(State),
    TimerRef = erlang:send_after(Interval, self(), check),
    State#{timer_ref => TimerRef}.

cancel_old_timer(#{timer_ref := OldRef}) ->
    erlang:cancel_timer(OldRef);
cancel_old_timer(_State) ->
    ok.

handle_check(State = #{mon_tab := MonTab}) ->
    check_loop(ets:tab2list(MonTab), MonTab),
    schedule_check(State).

check_loop([{Mon, Pid}|List], MonTab) ->
    case is_process_alive(Pid) of
        true ->
            ets:delete(MonTab, Mon);
        false ->
            ok
    end,
    check_loop(List, MonTab);
check_loop([], _MonTab) ->
    ok.
