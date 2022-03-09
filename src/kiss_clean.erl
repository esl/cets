-module(kiss_clean).
-export([blocking/1]).

-include_lib("kernel/include/logger.hrl").

%% Spawn a new process to do some memory-intensive task
%% This allows to reduce GC on the parent process
%% Wait for function to finish
%% Handles errors
blocking(F) ->
    Pid = self(),
    Ref = make_ref(),
    proc_lib:spawn_link(fun() ->
            Res = try
                      F()
                  catch Class:Reason:Stacktrace ->
                      ?LOG_ERROR(#{what => blocking_call_failed,
                                   class => Class, reason => Reason,
                                   stacktrace => Stacktrace}),
                      {error, {Class, Reason, Stacktrace}}
                  end,
            Pid ! {result, Ref, Res}
        end),
    receive
        {result, Ref, Res} ->
            Res
    end.
