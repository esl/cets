-module(cets_test_receive).
-export([
    receive_message/1,
    receive_message_with_arg/1,
    flush_message/1,
    receive_all_logs/1,
    assert_nothing_is_logged/2
]).

receive_message(M) ->
    receive
        M -> ok
    after 5000 -> error({receive_message_timeout, M})
    end.

receive_message_with_arg(Tag) ->
    receive
        {Tag, Arg} -> Arg
    after 5000 -> error({receive_message_with_arg_timeout, Tag})
    end.

flush_message(M) ->
    receive
        M ->
            flush_message(M)
    after 0 ->
        ok
    end.

receive_all_logs(Id) ->
    receive
        {log, Id, Log} ->
            [Log | receive_all_logs(Id)]
    after 100 ->
        []
    end.

assert_nothing_is_logged(LogHandlerId, LogRef) ->
    receive
        {log, LogHandlerId, #{
            level := Level,
            msg := {report, #{log_ref := LogRef}}
        }} when Level =:= warning; Level =:= error ->
            ct:fail(got_logging_but_should_not)
    after 0 ->
        ok
    end.
