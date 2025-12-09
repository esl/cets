%% @doc Helper to log long running operations.
-module(cets_long).
-export([run_spawn/2, run_tracked/2, run_tracked/3]).
-ignore_xref([run_tracked/3]).

-ifdef(TEST).
-export([pinfo/2]).
-endif.

-export_type([log_info/0]).

-include_lib("kernel/include/logger.hrl").

-type log_info() :: map().
%% Extra logging information.

-type task_result() :: term().
%% The generic result of execution.

-type task_fun() :: fun(() -> task_result()).
%% User provided function to execute.

-type task_timeout() :: timeout().
%% Timeout for task execution in milliseconds.

%% @doc Spawns a new process to do some memory-intensive task.
%%
%% This allows to reduce GC on the parent process.
%% Waits for function to finish.
%% Handles errors.
%% Returns result from the function or crashes (i.e. forwards an error).
-spec run_spawn(log_info(), task_fun()) -> task_result().
run_spawn(Info, F) ->
    Pid = self(),
    Ref = make_ref(),
    proc_lib:spawn_link(fun() ->
        try run_tracked(Info, F) of
            Res ->
                Pid ! {result, Ref, Res}
        catch
            Class:Reason:Stacktrace ->
                Pid ! {forward_error, Ref, {Class, Reason, Stacktrace}}
        end
    end),
    receive
        {result, Ref, Res} ->
            Res;
        {forward_error, Ref, {Class, Reason, Stacktrace}} ->
            erlang:raise(Class, Reason, Stacktrace)
    end.

%% @doc Runs function `Fun' without timeout.
%%
%% Logs errors.
%% Logs if function execution takes too long.
%% Catches errors and re-raises them after logging.
-spec run_tracked(log_info(), task_fun()) -> task_result().
run_tracked(Info, Fun) ->
    run_tracked(Info, Fun, infinity).

%% @doc Runs function `Fun' with a specified timeout.
%%
%% Logs errors.
%% Logs if function execution takes too long.
%% Catches errors and re-raises them after logging.
-spec run_tracked(log_info(), task_fun(), task_timeout()) -> task_result().
run_tracked(Info, Fun, Timeout) ->
    Parent = self(),
    Start = erlang:system_time(millisecond),
    ?LOG_INFO(Info#{what => task_started}),
    Pid = spawn_mon(Info, Parent, Start, Timeout),
    try
        Fun()
    catch
        %% Skip nested task_failed errors
        Class:{task_failed, Reason, Info2}:Stacktrace ->
            erlang:raise(Class, {task_failed, Reason, Info2}, Stacktrace);
        Class:Reason:Stacktrace ->
            Log = Info#{
                what => task_failed,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace,
                caller_pid => Parent,
                long_ref => make_ref()
            },
            ?LOG_ERROR(Log),
            erlang:raise(Class, {task_failed, Reason, Info}, Stacktrace)
    after
        Diff = diff(Start),
        ?LOG_INFO(Info#{what => task_finished, time_ms => Diff}),
        Pid ! stop
    end.

spawn_mon(Info, Parent, Start, Timeout) ->
    Ref = make_ref(),
    %% We do not link, because we want to log if the Parent dies
    Pid = spawn(fun() -> run_monitor(Info, Ref, Parent, Start, Timeout) end),
    %% Ensure there is no race conditions by waiting till the monitor is added
    receive
        {monitor_added, Ref} -> ok
    end,
    Pid.

run_monitor(Info, Ref, Parent, Start, Timeout) ->
    Mon = erlang:monitor(process, Parent),
    Parent ! {monitor_added, Ref},
    Interval = maps:get(report_interval, Info, 5000),
    monitor_loop(Mon, Info, Parent, Start, Interval, Timeout).

monitor_loop(Mon, Info, Parent, Start, Interval, Timeout) ->
    Diff = diff(Start),
    case Timeout =/= infinity andalso Diff >= Timeout of
        true ->
            ?LOG_ERROR(Info#{
                what => task_timeout,
                caller_pid => Parent,
                time_ms => Diff,
                timeout_ms => Timeout,
                current_stacktrace => pinfo(Parent, current_stacktrace)
            }),
            exit(Parent, {task_timeout, Info});
        false ->
            WaitTime =
                case Timeout of
                    infinity -> Interval;
                    _ -> min(Interval, Timeout - Diff)
                end,
            receive
                {'DOWN', _MonRef, process, _Pid, shutdown} ->
                    %% Special case, the long task is stopped using exit(Pid, shutdown)
                    ok;
                {'DOWN', MonRef, process, _Pid, Reason} when Mon =:= MonRef ->
                    ?LOG_ERROR(Info#{
                        what => task_failed,
                        reason => Reason,
                        caller_pid => Parent,
                        reported_by => monitor_process
                    }),
                    ok;
                stop ->
                    ok
            after WaitTime ->
                Diff2 = diff(Start),
                %% Check if timeout is reached before logging progress
                case Timeout =/= infinity andalso Diff2 >= Timeout of
                    true ->
                        %% Don't log progress, let the next iteration handle timeout
                        ok;
                    false ->
                        ?LOG_WARNING(Info#{
                            what => long_task_progress,
                            caller_pid => Parent,
                            time_ms => Diff2,
                            current_stacktrace => pinfo(Parent, current_stacktrace)
                        })
                end,
                monitor_loop(Mon, Info, Parent, Start, Interval, Timeout)
            end
    end.

diff(Start) ->
    erlang:system_time(millisecond) - Start.

pinfo(Pid, Key) ->
    case erlang:process_info(Pid, Key) of
        {Key, Val} ->
            Val;
        undefined ->
            undefined
    end.
