%% Helper to log long running operations.
-module(cets_long).
-export([run_spawn/2, run/2, run_safely/2]).

-include_lib("kernel/include/logger.hrl").

%% Extra logging information
-type log_info() :: map().
-type task_result() :: term().
-type task_fun() :: fun(() -> task_result()).
-type reason() :: {Class :: atom(), Reason :: term(), Stacktrace :: list()}.
-export_type([log_info/0]).

%% Spawn a new process to do some memory-intensive task
%% This allows to reduce GC on the parent process
%% Wait for function to finish
%% Handles errors
%% Returns result from the function or crashes
-spec run_spawn(log_info(), task_fun()) -> task_result().
run_spawn(Info, F) ->
    Pid = self(),
    Ref = make_ref(),
    proc_lib:spawn_link(fun() ->
        Res = cets_long:run_safely(Info, F),
        Pid ! {result, Ref, Res}
    end),
    receive
        {result, Ref, Res} ->
            Res
    end.

-spec run_safely(log_info(), task_fun()) -> task_result() | {error, reason()}.
run_safely(Info, Fun) ->
    run(Info, Fun, true).

-spec run(log_info(), task_fun()) -> task_result().
run(Info, Fun) ->
    run(Info, Fun, false).

-spec run(log_info(), task_fun(), boolean()) -> task_result() | {error, reason()}.
run(Info, Fun, Catch) ->
    Parent = self(),
    Start = erlang:system_time(millisecond),
    ?LOG_INFO(Info#{what => long_task_started}),
    Pid = spawn_mon(Info, Parent, Start),
    try
        case Catch of
            true -> just_run_safely(Info#{what => long_task_failed}, Fun);
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
        stop ->
            ok
    after 5000 ->
        Diff = diff(Start),
        ?LOG_INFO(Info#{what => long_task_progress, time_ms => Diff}),
        monitor_loop(Mon, Info, Start)
    end.

diff(Start) ->
    erlang:system_time(millisecond) - Start.

just_run_safely(Info, Fun) ->
    try
        Fun()
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(Info#{class => Class, reason => Reason, stacktrace => Stacktrace}),
            {error, {Class, Reason, Stacktrace}}
    end.
