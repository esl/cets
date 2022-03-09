-module(kiss_long).
-export([run/2]).
-export([run_safely/2]).

-include_lib("kernel/include/logger.hrl").

run_safely(Info, Fun) ->
    run(Info, Fun, true).

run(Info, Fun) ->
    run(Info, Fun, false).

run(Info, Fun, Catch) ->
    Parent = self(),
    Start = os:timestamp(),
    ?LOG_INFO(Info#{what => long_task_started}),
    Pid = spawn_mon(Info, Parent, Start),
    try
            case Catch of
                true -> kiss_safety:run(Info#{what => long_task_failed}, Fun);
                false -> Fun()
            end
        after
            Diff = diff(Start),
            ?LOG_INFO(Info#{what => long_task_finished, time_ms => Diff}),
            Pid ! stop
    end.

spawn_mon(Info, Parent, Start) ->
    spawn_link(fun() -> run_monitor(Info, Parent, Start) end).

run_monitor(Info, Parent, Start) ->
    Mon = erlang:monitor(process, Parent),
    monitor_loop(Mon, Info, Start).

monitor_loop(Mon, Info, Start) ->
    receive
        {'DOWN', MonRef, process, _Pid, Reason} when Mon =:= MonRef ->
            ?LOG_ERROR(Info#{what => long_task_failed, reason => Reason}),
            ok;
        stop -> ok
        after 5000 ->
            Diff = diff(Start),
            ?LOG_INFO(Info#{what => long_task_progress, time_ms => Diff}),
            monitor_loop(Mon, Info, Start)
    end.

diff(Start) ->
    timer:now_diff(os:timestamp(), Start) div 1000.
