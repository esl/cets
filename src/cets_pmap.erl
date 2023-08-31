-module(cets_pmap).
-export([map/2]).

-include_lib("kernel/include/logger.hrl").

%% Applies F for all elements in parallel
%% Waits forever for the results
%% Forwards crashes (i.e. raises an error in the context of the caller process)
-spec map(fun((term()) -> term()), list()) -> list().
map(F, Elems) when is_function(F, 1) ->
    Pids = [apply_async(F, Elem) || Elem <- Elems],
    Results = [wait_for_result(Pid) || Pid <- Pids],
    [unpack(Result) || Result <- Results].

apply_async(F, Elem) ->
    Me = self(),
    spawn_link(fun() -> Me ! {result, self(), safe_apply(F, Elem)} end).

wait_for_result(Pid) ->
    receive
        {result, Pid, Res} ->
            Res
    end.

unpack({ok, X}) ->
    X;
unpack({error, {Class, Reason, Stacktrace}}) ->
    erlang:raise(Class, Reason, Stacktrace).

safe_apply(F, Argument) when is_function(F, 1) ->
    try
        {ok, F(Argument)}
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                what => pmap_apply,
                argument => Argument,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, {Class, Reason, Stacktrace}}
    end.
