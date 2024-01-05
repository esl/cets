%% Logger helper module
-module(cets_test_log).
-export([
    receive_all_logs_with_log_ref/2,
    receive_all_logs_from_pid/2
]).

-include_lib("kernel/include/logger.hrl").

receive_all_logs_with_log_ref(LogHandlerId, LogRef) ->
    ensure_logger_is_working(LogHandlerId, LogRef),
    %% Do a new logging call to check that it is the only log message
    ?LOG_ERROR(#{what => ensure_nothing_logged_after, log_ref => LogRef}),
    receive_all_logs_with_log_ref_loop(LogHandlerId, LogRef).

receive_all_logs_with_log_ref_loop(LogHandlerId, LogRef) ->
    %% We only match messages with the matching log_ref here
    %% to ignore messages from the other parallel tests
    receive
        {log, LogHandlerId, Log = #{msg := {report, Report = #{log_ref := LogRef}}}} ->
            case Report of
                #{what := ensure_nothing_logged_after} ->
                    [];
                _ ->
                    [Log | receive_all_logs_with_log_ref_loop(LogHandlerId, LogRef)]
            end
    after 5000 ->
        ct:fail({timeout, receive_all_logs_with_log_ref})
    end.

%% Return logged messages so far. Filters by pid.
receive_all_logs_from_pid(LogHandlerId, Pid) ->
    LogRef = make_ref(),
    ensure_logger_is_working(LogHandlerId, LogRef),
    %% Do a new logging call to check that it is the only log message
    ?LOG_ERROR(#{what => ensure_nothing_logged_after, log_ref => LogRef}),
    receive_all_logs_from_pid_loop(LogHandlerId, Pid, LogRef).

receive_all_logs_from_pid_loop(LogHandlerId, Pid, LogRef) ->
    %% We only match messages with the matching log_ref here
    %% to ignore messages from the other parallel tests
    receive
        {log, LogHandlerId, #{msg := {report, #{log_ref := LogRef}}}} ->
            %% Finish waiting
            [];
        {log, LogHandlerId, Log = #{meta := #{pid := Pid}}} ->
            [Log | receive_all_logs_from_pid_loop(LogHandlerId, Pid, LogRef)];
        {log, LogHandlerId, _Log} ->
            receive_all_logs_from_pid_loop(LogHandlerId, Pid, LogRef)
    after 5000 ->
        ct:fail({timeout, receive_all_logs_from_pid})
    end.

ensure_logger_is_working(LogHandlerId, LogRef) ->
    ?LOG_ERROR(#{what => ensure_nothing_logged_before, log_ref => LogRef}),
    receive
        {log, LogHandlerId, #{
            msg := {report, #{log_ref := LogRef, what := ensure_nothing_logged_before}}
        }} ->
            ok
    after 5000 ->
        ct:fail({timeout, logger_is_broken})
    end.
