-module(cets_test_receive).
-export([
    receive_message/1,
    receive_message_with_arg/1,
    flush_message/1
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
