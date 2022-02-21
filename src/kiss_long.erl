-module(kiss_long).
-export([run/3]).

run(InfoText, InfoArgs, Fun) ->
    Parent = self(),
    Start = os:timestamp(),
    error_logger:info_msg("what=long_task_started " ++ InfoText, InfoArgs),
    Pid = spawn_mon(InfoText, InfoArgs, Parent, Start),
    try
            Fun()
        after
            Diff = diff(Start),
            error_logger:info_msg("what=long_task_finished time=~p ms " ++ InfoText,
                                  [Diff] ++ InfoArgs),
            Pid ! stop
    end.

spawn_mon(InfoText, InfoArgs, Parent, Start) ->
    spawn_link(fun() -> run_monitor(InfoText, InfoArgs, Parent, Start) end).

run_monitor(InfoText, InfoArgs, Parent, Start) ->
    Mon = erlang:monitor(process, Parent),
    monitor_loop(Mon, InfoText, InfoArgs, Start).

monitor_loop(Mon, InfoText, InfoArgs, Start) ->
    receive
        {'DOWN', MonRef, process, _Pid, Reason} when Mon =:= MonRef ->
            error_logger:error_msg("what=long_task_failed reason=~p " ++ InfoText,
                                   [Reason] ++ InfoArgs),
            ok;
        stop -> ok
        after 5000 ->
            Diff = diff(Start),
            error_logger:info_msg("what=long_task_progress time=~p ms " ++ InfoText,
                                  [Diff] ++ InfoArgs),
            monitor_loop(Mon, InfoText, InfoArgs, Start)
    end.

diff(Start) ->
    timer:now_diff(os:timestamp(), Start) div 1000.
