%% Monitor Table contains processes, that are waiting for writes to finish.
%% It is usually cleaned automatically.
%% Unless the caller process crashes.
%% This server removes such entries from the MonTab.
%% We don't expect the MonTab to be extremely big, so this check should be quick.
-module(cets_ack).
-behaviour(gen_server).

-export([start_link/2]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("kernel/include/logger.hrl").

-type timer_ref() :: reference().
-type state() :: #{
    mon_tab := atom(),
    interval := non_neg_integer(),
    timer_ref := timer_ref()
}.

start_link(Name, MonTab) ->
    gen_server:start_link({local, Name}, ?MODULE, MonTab, []).

-spec init(atom()) -> {ok, state()}.
init(MonTab) ->
    Interval = 30000,
    State = #{
        mon_tab => MonTab,
        interval => Interval,
        timer_ref => start_timer(Interval)
    },
    {ok, State}.

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
handle_check(State = #{mon_tab := MonTab}) ->
    check_loop(ets:tab2list(MonTab), MonTab),
    schedule_check(State).

check_loop([{Mon, Pid} | List], MonTab) ->
    case is_process_alive(Pid) of
        false ->
            ets:delete(MonTab, Mon);
        true ->
            ok
    end,
    check_loop(List, MonTab);
check_loop([], _MonTab) ->
    ok.
